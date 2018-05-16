package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

var emptyHash [32]byte

// Verifies that hashes loaded from the hashfile match with the root
func (t *Trie) rebuildFromHashes(dbr DatabaseReader) (root node, roothash hashNode, err error) {
	startTime := time.Now()
	var vertical [6]*fullNode
	var fillCount [6]int // couting number of children for determining whether we need fullNode, shortNode, or nothing
	var lastFill [6]node
	var lastFillIdx [6]byte
	var lastFull [6]bool
	var shorts [6]*shortNode
	hasher := newHasher(t.encodeToBytes)
	defer returnHasherToPool(hasher)
	for i := 0; i < 1024*1024; i++ {
		hashBytes := dbr.GetHash(uint32(i))
		var hash node
		hash = hashNode(hashBytes)
		var short *shortNode
		fullNodeHash := false
		for level := 4; level >= 0; level-- {
			var v int
			switch level {
			case 4:
				v = i&0xf
			case 3:
				v = (i>>4)&0xf
			case 2:
				v = (i>>8)&0xf
			case 1:
				v = (i>>12)&0xf
			case 0:
				v = (i>>16)&0xf
			}
			if vertical[level] == nil {
				vertical[level] = &fullNode{}
			}
			if h, ok := hash.(hashNode); ok && bytes.Equal(h, emptyHash[:]) {
				vertical[level].Children[v] = nil
			} else {
				vertical[level].Children[v] = hash
				lastFill[level], hash = hash, nil
				lastFillIdx[level] = byte(v)
				lastFull[level], fullNodeHash = fullNodeHash, false
				shorts[level], short = short, nil
				fillCount[level]++
			}
			if v != 15 {
				break
			}
			// We filled up 16 cells, check how many are not empty
			if fillCount[level] == 0 {
				hash = hashNode(emptyHash[:])
				short = nil
				fullNodeHash = false
			} else if fillCount[level] == 1 {
				if lastFull[level] {
					// lastFill was a fullNode
					short = &shortNode{Key: hexToCompact([]byte{lastFillIdx[level]}), Val: lastFill[level]}
					hash = short
				} else if shorts[level] != nil {
					// lastFill was a short node which needs to be extended
					short = &shortNode{Key: hexToCompact(append([]byte{lastFillIdx[level]}, compactToHex(shorts[level].Key)...)), Val: shorts[level].Val}
					hash = short
				} else {
					hash = lastFill[level]
				}
				fullNodeHash = false
			} else {
				short = nil
				shorts[level] = nil
				hash = vertical[level]
				fullNodeHash = true
			}
			lastFill[level] = nil
			lastFull[level] = false
			fillCount[level] = 0
			vertical[level] = nil
			if level == 0 {
				root = hash
			}
		}
	}
	var rootHash common.Hash
	if root != nil {
		_, err := hasher.hash(root, true, rootHash[:])
		if err != nil {
			return root, nil, err
		}
	}
	log.Debug("rebuildFromHashes took %v\n", time.Since(startTime))
	return root, hashNode(rootHash[:]), nil
}

func (t *Trie) Rebuild(db ethdb.Database, blockNr uint64) hashNode {
	if t.root == nil {
		return nil
	}
	n, ok := t.root.(hashNode)
	if !ok {
		panic("Expected hashNode")
	}
	root, roothash, err := t.rebuildFromHashes(db)
	if err != nil {
		panic(err)
	}
	if bytes.Equal(roothash, n) {
		t.relistNodes(root)
		t.root = root
		log.Info("Successfuly loaded from hashfile", "nodes", t.nodeList.Len(), "root hash", roothash)
	} else {
		_, hn, err := t.rebuildHashes(db, nil, 0, blockNr, true, n)
		if err != nil {
			panic(err)
		}
		root, roothash, err = t.rebuildFromHashes(db)
		if err != nil {
			panic(err)
		}
		if bytes.Equal(roothash, hn) {
			t.relistNodes(root)
			t.root = root
			log.Info("Rebuilt hashfile and verified", "nodes", t.nodeList.Len(), "root hash", roothash)
		} else {
			log.Error(fmt.Sprintf("Could not rebuild %s vs %s\n", roothash, hn))
		}
	}
	return roothash
}

const Levels = 64

type ResolveHexes [][]byte

// ResolveHexes implements sort.Interface
func (rh ResolveHexes) Len() int {
	return len(rh)
}

func (rh ResolveHexes) Less(i, j int) bool {
	return bytes.Compare(rh[i], rh[j]) < 0
}

func (rh ResolveHexes) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
}

/* One resolver per trie (prefix) */
type TrieResolver struct {
	t *Trie
	dbw ethdb.Putter // For updating hashes
	hashes bool
	continuations []*TrieContinuation
	resolveHexes ResolveHexes
	rhIndexLte int // index in resolveHexes with resolve key less or equal to the current key
	               // if the current key is less than the first resolve key, this index is -1
	rhIndexGt int // index in resolveHexes with resolve key greater than the current key
	              // if the current key is greater than the last resolve key, this index is len(resolveHexes)
	contIndices []int // Indices pointing back to continuation array from arrays retured by PrepareResolveParams
	key [32]byte
	value []byte
	key_set bool
	nodeStack [Levels+1]shortNode
	vertical [Levels+1]fullNode
	fillCount [Levels+1]int
	startLevel int
	keyIdx int
	h *hasher
}

func (t *Trie) NewResolver(dbw ethdb.Putter, hashes bool) *TrieResolver {
	tr := TrieResolver{
		t: t,
		dbw: dbw,
		hashes: hashes,
		continuations: []*TrieContinuation{},
		resolveHexes: [][]byte{},
		rhIndexLte: -1,
		rhIndexGt: 0,
		contIndices: []int{},
		h: newHasher(t.encodeToBytes),
	}
	return &tr
}

// TrieResolver implements sort.Interface
func (tr *TrieResolver) Len() int {
	return len(tr.continuations)
}

func min(a, b int) int {
	if a < b {
		return a
    }
    return b
}

func (tr *TrieResolver) Less(i, j int) bool {
	m := min(tr.continuations[i].resolvePos, tr.continuations[j].resolvePos)
	c := bytes.Compare(tr.continuations[i].resolveKey[:m], tr.continuations[j].resolveKey[:m])
	if c != 0 {
		return c < 0
	}
	return tr.continuations[i].resolvePos < tr.continuations[j].resolvePos
}

func (tr *TrieResolver) Swap(i, j int) {
	tr.continuations[i], tr.continuations[j] = tr.continuations[j], tr.continuations[i]
}

func (tr *TrieResolver) AddContinuation(c *TrieContinuation) {
	tr.continuations = append(tr.continuations, c)
	tr.resolveHexes = append(tr.resolveHexes, c.resolveKey)
}

func (tr *TrieResolver) Print() {
	for _, c := range tr.continuations {
		c.Print()
	}
}

// Prepares information for the MultiWalk
func (tr *TrieResolver) PrepareResolveParams() ([][]byte, []uint) {
	// Remove continuations strictly contained in the preceeding ones
	startkeys := [][]byte{}
	fixedbits := []uint{}
	if len(tr.continuations) == 0 {
		return startkeys, fixedbits
	}
	sort.Stable(tr)
	sort.Sort(tr.resolveHexes)
	newHexes := [][]byte{}
	for i, h := range tr.resolveHexes {
		if i == len(tr.resolveHexes) - 1 || !bytes.HasPrefix(tr.resolveHexes[i+1], h) {
			newHexes = append(newHexes, h)
		}
	}
	tr.resolveHexes = newHexes
	var prevC *TrieContinuation
	for i, c := range tr.continuations {
		if prevC == nil || c.resolvePos < prevC.resolvePos || !bytes.HasPrefix(c.resolveKey[:c.resolvePos], prevC.resolveKey[:prevC.resolvePos]) {
			tr.contIndices = append(tr.contIndices, i)
			key := make([]byte, 32)
			decodeNibbles(c.resolveKey[:c.resolvePos], key)
			startkeys = append(startkeys, key)
			fixedbits = append(fixedbits, uint(4*c.resolvePos))
			prevC = c
			//c.Print()
		}
	}
	tr.startLevel = tr.continuations[0].resolvePos
	return startkeys, fixedbits
}

func (tr *TrieResolver) finishPreviousKey(k []byte) error {
	pLen := prefixLen(k, tr.key[:])
	stopLevel := 2*pLen
	if k != nil && (k[pLen]^tr.key[pLen])&0xf0 == 0 {
		stopLevel++
	}
	tc := tr.continuations[tr.contIndices[tr.keyIdx]]
	startLevel := tr.startLevel
	if startLevel < tc.resolvePos {
		startLevel = tc.resolvePos
	}
	if startLevel < stopLevel {
		startLevel = stopLevel
	}
	hex := keybytesToHex(tr.key[:])
	tr.nodeStack[startLevel+1].Key = hexToCompact(hex[startLevel+1:])
	tr.nodeStack[startLevel+1].setVal(valueNode(tr.value))
	tr.nodeStack[startLevel+1].hashTrue = false
	tr.fillCount[startLevel+1] = 1
	// Adjust rhIndices if needed
	if tr.rhIndexGt < tr.resolveHexes.Len() {
		resComp := bytes.Compare(hex, tr.resolveHexes[tr.rhIndexGt])
		for tr.rhIndexGt < tr.resolveHexes.Len() && resComp != -1 {
			tr.rhIndexGt++
			tr.rhIndexLte++
			if tr.rhIndexGt < tr.resolveHexes.Len() {
				resComp = bytes.Compare(hex, tr.resolveHexes[tr.rhIndexGt])
			}
		}
	}
	var rhPrefixLen int
	if tr.rhIndexLte >= 0 {
		rhPrefixLen = prefixLen(hex, tr.resolveHexes[tr.rhIndexLte])
	}
	if tr.rhIndexGt < tr.resolveHexes.Len() {
		rhPrefixLenGt := prefixLen(hex, tr.resolveHexes[tr.rhIndexGt])
		if rhPrefixLenGt > rhPrefixLen {
			rhPrefixLen = rhPrefixLenGt
		}
	}
	for level := startLevel; level >= stopLevel; level-- {
		keynibble := hex[level]
		onResolvingPath := level <= rhPrefixLen // <= instead of < to be able to resolve deletes in one go
		//fmt.Printf("Level %d, keynibble %d, onResolvingPath %t, fillCount %d\n",
		//	level, keynibble, onResolvingPath, tr.fillCount[level+1])
		var hashIdx uint32
		if tr.hashes && level <= 4 {
			hashIdx = binary.BigEndian.Uint32(tr.key[:4]) >> 12
		}
		if tr.fillCount[level+1] == 1 {
			// Short node, needs to be promoted to the level above
			short := &tr.nodeStack[level+1]
			if tr.vertical[level].childHashes[keynibble] == nil {
				tr.vertical[level].childHashes[keynibble] = make([]byte, common.HashLength)
			}
			hn, err := tr.h.hash(short, false, tr.vertical[level].childHashes[keynibble])
			if err != nil {
				return err
			}
			if _, ok := hn.(hashNode); ok {
				tr.vertical[level].hashTrueMask |= (uint32(1)<<keynibble)
			} else {
				tr.vertical[level].hashTrueMask &^= (uint32(1)<<keynibble)
			}
			if onResolvingPath {
				tr.vertical[level].Children[keynibble] = short.copy()
				//fmt.Printf("Promoting copy of short\n")
			} else {
				tr.vertical[level].Children[keynibble] = hn
				//if hash, ok := hn.(hashNode); ok {
				//	fmt.Printf("Promoting hash %s\n", hash)
				//} else {
				//	fmt.Printf("Promoting embedded node %s\n", hn.fstring(""))
				//}
			}
			tr.nodeStack[level].Key = hexToCompact(append([]byte{keynibble}, compactToHex(short.Key)...))
			tr.nodeStack[level].setVal(short.Val)
			_, tr.nodeStack[level].hashTrue = short.Val.(hashNode)
			tr.fillCount[level]++
			if tr.hashes && level <= 4 && compactLen(short.Key) + level >= 4 {
				tr.dbw.PutHash(hashIdx, hn.(hashNode))
			}
			if level >= tc.resolvePos {
				tr.nodeStack[level+1].Key = nil
				tr.nodeStack[level+1].Val = nil
				tr.nodeStack[level+1].hashTrue = false
				tr.fillCount[level+1] = 0
				for i := 0; i < 17; i++ {
					tr.vertical[level+1].Children[i] = nil
				}
				tr.vertical[level+1].hashTrueMask = 0
			}
			continue
		}
		full := &tr.vertical[level+1]
		if tr.vertical[level].childHashes[keynibble] == nil {
			tr.vertical[level].childHashes[keynibble] = make([]byte, common.HashLength)
		}
		hn, err := tr.h.hash(full, false, tr.vertical[level].childHashes[keynibble])
		if err != nil {
			return err
		}
		tr.vertical[level].hashTrueMask |= (uint32(1)<<keynibble)
		tr.nodeStack[level].hashTrue = true
		if tr.nodeStack[level].valHash == nil {
			tr.nodeStack[level].valHash = make([]byte, common.HashLength)
		}
		copy(tr.nodeStack[level].valHash, tr.vertical[level].childHashes[keynibble])
		//hn1 := tr.nodeStack[level].valHash
		if tr.hashes && level == 4 {
			tr.dbw.PutHash(hashIdx, hn.(hashNode))
		}
		tr.nodeStack[level].Key = hexToCompact([]byte{keynibble})
		if onResolvingPath {
			var c node
			if tr.fillCount[level+1] == 2 {
				c = full.duoCopy()
			} else {
				c = full.copy()
			}
			tr.vertical[level].setChild(keynibble, c)
			tr.nodeStack[level].setVal(c)
			//fmt.Printf("Promoting copy of full\n")
		} else {
			tr.vertical[level].setChild(keynibble, hn)
			tr.nodeStack[level].setVal(hn)
			//if hash, ok := hn.(hashNode); ok {
			//	fmt.Printf("Promoting hash %s\n", hash)
			//} else {
			//	fmt.Printf("Promoting embedded node %s\n", hn.fstring(""))
			//}
		}
		tr.fillCount[level]++
		if level >= tc.resolvePos {
			tr.nodeStack[level+1].Key = nil
			tr.nodeStack[level+1].Val = nil
			tr.nodeStack[level+1].hashTrue = false
			tr.fillCount[level+1] = 0
			for i := 0; i < 17; i++ {
				tr.vertical[level+1].Children[i] = nil
			}
			tr.vertical[level+1].hashTrueMask = 0
		}
	}
	tr.startLevel = stopLevel
	if k == nil {
		var root node
		//fmt.Printf("root fillCount %d\n", tr.fillCount[tc.resolvePos])
		if tr.fillCount[tc.resolvePos] == 1 {
			root = tr.nodeStack[tc.resolvePos].copy()
		} else if tr.fillCount[tc.resolvePos] == 2 {
			root = tr.vertical[tc.resolvePos].duoCopy()
		} else if tr.fillCount[tc.resolvePos] > 2 {
			root = tr.vertical[tc.resolvePos].copy()
		}
		if root == nil {
			return fmt.Errorf("Resolve returned nil root")
		}
		var gotHash common.Hash
		hash, err := tr.h.hash(root, tc.resolvePos == 0, gotHash[:])
		if err != nil {
			return err
		}
		if _, ok := hash.(hashNode); ok {
			if !bytes.Equal(tc.resolveHash, gotHash[:]) {
				return fmt.Errorf("Resolving wrong hash for key %x, pos %d, trie prefix %x\nexpected %s, got %s\n",
					tc.resolveKey,
					tc.resolvePos,
					tr.t.prefix,
					tc.resolveHash,
					hashNode(gotHash[:]),
				)
			}
		} else {
			if tc.resolveHash != nil {
				return fmt.Errorf("Resolving wrong hash for key %x, pos %d, trie prefix %x\nexpected %s, got embedded node\n",
					tc.resolveKey,
					tc.resolvePos,
					tr.t.prefix,
					tc.resolveHash)
			}
		}
		tc.resolved = root
		for i := 0; i <= Levels; i++ {
			tr.nodeStack[i].Key = nil
			tr.nodeStack[i].Val = nil
			tr.nodeStack[i].hashTrue = false
			for j := 0; j<17; j++ {
				tr.vertical[i].Children[j] = nil
			}
			tr.vertical[i].hashTrueMask = 0
			tr.fillCount[i] = 0
		}
	}
	return nil
}

func (tr *TrieResolver) Walker(keyIdx int, k []byte, v []byte) (bool, error) {
	//fmt.Printf("%d %x %x\n", keyIdx, k, v)
	if keyIdx != tr.keyIdx {
		if tr.key_set {
			if err := tr.finishPreviousKey(nil); err != nil {
				return false, err
			}
			tr.key_set = false
		}
		tr.keyIdx = keyIdx
	}
	if len(v) > 0 {
		// First, finish off the previous key
		if tr.key_set {
			if err := tr.finishPreviousKey(k); err != nil {
				return false, err
			}
		}
		// Remember the current key and value
		copy(tr.key[:], k[:32])
		tr.value = common.CopyBytes(v)
		tr.key_set = true
	}
	return true, nil
}

func (tr *TrieResolver) ResolveWithDb(db ethdb.Database, blockNr uint64) error {
	defer returnHasherToPool(tr.h)
	startkeys, fixedbits := tr.PrepareResolveParams()
	//fmt.Printf("ResolveWithDb with %d startkeys\n", len(startkeys))
	//for i, startkey := range startkeys {
	//	fmt.Printf("%x %d\n", startkey, fixedbits[i])
	//}
	if err := db.MultiWalkAsOf(tr.t.prefix, startkeys, fixedbits, blockNr, tr.Walker); err != nil {
		return err
	}
	return nil
}

func (t *Trie) rebuildHashes(db ethdb.Database, key []byte, pos int, blockNr uint64, hashes bool, expected hashNode) (node, hashNode, error) {
	tc := t.NewContinuation(key, pos, expected)
	r := t.NewResolver(db, true)
	r.AddContinuation(tc)
	if err := r.ResolveWithDb(db, blockNr); err != nil {
		return nil, nil, err
	}
	return tc.resolved, expected, nil
}

