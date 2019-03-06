// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package trie implements Merkle Patricia Tries.
package trie

import (
	"bytes"
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyState is the known hash of an empty state trie entry.
	emptyState = crypto.Keccak256Hash(nil)
)

// LeafCallback is a callback type invoked when a trie operation reaches a leaf
// node. It's used by state sync and commit to allow handling external references
// between account and storage tries.
type LeafCallback func(leaf []byte, parent common.Hash) error

// Trie is a Merkle Patricia Trie.
// The zero value is an empty trie with no database.
// Use New to create a trie that sits on top of a database.
//
// Trie is not safe for concurrent use.
type Trie struct {
	root         	node
	originalRoot 	common.Hash

	// Bucket for the database access
	bucket 			[]byte
	// Prefix to form database key (for storage)
	prefix          []byte
	encodeToBytes 	bool
	accounts        bool

	historical      bool
	resolveReads    bool
	joinGeneration  func(gen uint64)
	leftGeneration  func(gen uint64)
	addReadProof    func(prefix, key []byte, pos int, mask uint32, hashes []common.Hash)
	addWriteProof   func(prefix, key []byte, pos int, mask uint32, hashes []common.Hash)
	addValue        func(prefix, key []byte, pos int, value []byte)
	addShort        func(prefix, key []byte, pos int, short []byte)
}

func (t *Trie) PrintTrie() {
	if t.root == nil {
		fmt.Printf("nil Trie\n")
	} else {
		fmt.Printf("%s\n", t.root.fstring(""))
	}
}

// New creates a trie with an existing root node from db.
//
// If root is the zero hash or the sha3 hash of an empty string, the
// trie is initially empty and does not require a database. Otherwise,
// New will panic if db is nil and returns a MissingNodeError if root does
// not exist in the database. Accessing the trie loads nodes from db on demand.
func New(root common.Hash, bucket []byte, prefix []byte, encodeToBytes bool) *Trie {
	trie := &Trie{
		originalRoot: root,
		bucket: bucket,
		prefix: prefix,
		encodeToBytes: encodeToBytes,
		accounts: bytes.Equal(bucket, []byte("AT")),
		joinGeneration: func(uint64) {},
		leftGeneration: func(uint64) {},
		addReadProof: func(prefix, key []byte, pos int, mask uint32, hashes []common.Hash) {},
		addWriteProof: func(prefix, key []byte, pos int, mask uint32, hashes []common.Hash) {},
		addValue: func(prefix, key []byte, pos int, value []byte) {},
		addShort: func(prefix, key []byte, pos int, short []byte) {},
	}
	if (root != common.Hash{}) && root != emptyRoot {
		rootcopy := make([]byte, len(root[:]))
		copy(rootcopy, root[:])
		trie.root = hashNode(rootcopy)
	}
	return trie
}

func construct(pos int, masks []uint32, shortKeys [][]byte, values [][]byte, hashes []common.Hash, maskIdx, shortIdx, valueIdx, hashIdx *int) node {
	fullMask := masks[*maskIdx]
	(*maskIdx)++
	mask := fullMask & 0xffff
	downmask := fullMask >> 16
	fmt.Printf("mask: %16b, downmask: %16b", mask, downmask)
	if mask == 0 {
		// short node (leaf or extension)
		nKey := shortKeys[*shortIdx]
		(*shortIdx)++
		s := &shortNode{Key: hexToCompact(nKey)}
		s.flags.dirty = true
		fmt.Printf("\n")
		if pos + len(nKey) == 65 {
			s.Val = valueNode(values[*valueIdx])
			(*valueIdx)++
		} else {
			s.Val = construct(pos+len(nKey), masks, shortKeys, values, hashes, maskIdx, shortIdx, valueIdx, hashIdx)
		}
		return s
	} else {
		fmt.Printf(", hashes:")
		// Make a full node
		f := &fullNode{}
		f.flags.dirty = true
		for nibble := byte(0); nibble < 16; nibble++ {
			if (mask & (uint32(1)<<nibble)) != 0 {
				fmt.Printf(" %x", hashes[*hashIdx][:4])
				f.Children[nibble] = hashNode(common.CopyBytes(hashes[*hashIdx][:]))
				(*hashIdx)++
			} else {
				f.Children[nibble] = nil
				fmt.Printf(" .")
			}
		}
		fmt.Printf("\n")
		for nibble := byte(0); nibble < 16; nibble++ {
			if (downmask & (uint32(1)<<nibble)) != 0 {
				f.Children[nibble] = construct(pos+1, masks, shortKeys, values, hashes, maskIdx, shortIdx, valueIdx, hashIdx)
			}
		}
		return f
	}
}

func NewFromProofs(bucket []byte, prefix []byte, encodeToBytes bool, masks []uint32, shortKeys [][]byte, values [][]byte, hashes []common.Hash) *Trie {
	t := &Trie{
		bucket: bucket,
		prefix: prefix,
		encodeToBytes: encodeToBytes,
		accounts: bytes.Equal(bucket, []byte("AT")),
		joinGeneration: func(uint64) {},
		leftGeneration: func(uint64) {},
		addReadProof: func(prefix, key []byte, pos int, mask uint32, hashes []common.Hash) {},
		addWriteProof: func(prefix, key []byte, pos int, mask uint32, hashes []common.Hash) {},
		addValue: func(prefix, key []byte, pos int, value []byte) {},
		addShort: func(prefix, key []byte, pos int, short []byte) {},
	}
	var maskIdx int
	var hashIdx int // index in the hashes
	var shortIdx int // index in the shortKeys
	var valueIdx int // inde in the values
	t.root = construct(0, masks, shortKeys, values, hashes, &maskIdx, &shortIdx, &valueIdx, &hashIdx)
	return t
}

func (t *Trie) SetHistorical(h bool) {
	t.historical = h
	if h && !bytes.HasPrefix(t.bucket, []byte("h")) {
		t.bucket = append([]byte("h"), t.bucket...)
	}
}

func (t *Trie) SetResolveReads(rr bool) {
	t.resolveReads = rr
}

func (t *Trie) MakeListed(joinGeneration, leftGeneration func (gen uint64),
	addReadProof func(prefix, key []byte, pos int, mask uint32, hashes []common.Hash),
	addWriteProof func(prefix, key []byte, pos int, mask uint32, hashes []common.Hash),
	addValue func(prefix, key []byte, pos int, value []byte),
	addShort func(prefix, key []byte, pos int, short []byte),
) {
	t.joinGeneration = joinGeneration
	t.leftGeneration = leftGeneration
	t.addReadProof = addReadProof
	t.addWriteProof = addWriteProof
	t.addValue = addValue
	t.addShort = addShort
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration starts at
// the key after the given start key.
func (t *Trie) NodeIterator(db ethdb.Database, start []byte, blockNr uint64) NodeIterator {
	return newNodeIterator(db, t, start, blockNr)
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
func (t *Trie) Get(db ethdb.Database, key []byte, blockNr uint64) []byte {
	res, err := t.TryGet(db, key, blockNr)
	if err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
	return res
}

// TryGet returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryGet(db ethdb.Database, key []byte, blockNr uint64) (value []byte, err error) {
	k := keybytesToHex(key)
	value, gotValue := t.tryGet1(db, t.root, k, 0, blockNr)
	if !gotValue {
		value, err = t.tryGet(db, t.root, key, 0, blockNr)
	}
	return value, err
}

func (t *Trie) emptyShortHash(db ethdb.Database, n *shortNode, level int, index uint32) {
	if compactLen(n.Key) + level < 6 {
		return
	}
	hexKey := compactToHex(n.Key)
	hashIdx := index
	for i := 0; i < 6-level; i++ {
		hashIdx = (hashIdx<<4)+uint32(hexKey[i])
	}
	//fmt.Printf("emptyShort %d %x\n", level, hashIdx)
	db.PutHash(hashIdx, emptyHash[:])
}

func (t *Trie) emptyFullHash(db ethdb.Database, level int, index uint32) {
	if level != 6 {
		return
	}
	//fmt.Printf("emptyFull %d %x\n", level, index)
	db.PutHash(index, emptyHash[:])
}

func calcIndex(key []byte, pos int) uint32 {
	var index uint32
	for i := 0; i < pos; i++ {
		index = (index << 4) + uint32(key[i])
	}
	return index
}

// Touching the node removes it from the nodeList
func (t *Trie) touch(db ethdb.Database, n node, key []byte, pos int) {
	if n == nil {
		return
	}
	// Zeroing out the places in the hashes
	if db == nil {
		return
	}
	if key == nil {
		return
	}
	if pos > 6 {
		return
	}
	if !t.accounts {
		return
	}
	switch n := (n).(type) {
	case *shortNode:
		t.emptyShortHash(db, n, pos, calcIndex(key, pos))
	case *duoNode:
		t.emptyFullHash(db, pos, calcIndex(key, pos))
	case *fullNode:
		t.emptyFullHash(db, pos, calcIndex(key, pos))
	}
}

func (t *Trie) saveShortHash(db ethdb.Database, n *shortNode, level int, index uint32, h *hasher) bool {
	if !t.accounts {
		return false
	}
	if level > 6 {
		return false
	}
	//fmt.Printf("saveShort(pre) %d\n", level)
	if compactLen(n.Key) + level < 6 {
		return true
	}
	hexKey := compactToHex(n.Key)
	hashIdx := index
	for i := 0; i < 6-level; i++ {
		hashIdx = (hashIdx<<4)+uint32(hexKey[i])
	}
	//fmt.Printf("saveShort %d %x %s\n", level, hashIdx, hash)
	db.PutHash(hashIdx, n.hash())
	return false
}

func (t *Trie) saveFullHash(db ethdb.Database, n node, level int, hashIdx uint32, h *hasher) bool {
	if !t.accounts {
		return false
	}
	if level > 6 {
		return false
	}
	if level < 6 {
		return true
	}
	//fmt.Printf("saveFull %d %x %s\n", level, hashIdx, hash)
	db.PutHash(hashIdx, n.hash())
	return false
}

func (t *Trie) saveHashes(db ethdb.Database, n node, level int, index uint32, h *hasher, blockNr uint64) {
	switch n := (n).(type) {
	case *shortNode:
		if n.flags.t < blockNr {
			return
		}
		// First re-add the child, then self
		if !t.saveShortHash(db, n, level, index, h) {
			return
		}
		index1 := index
		level1 := level
		for _, i := range compactToHex(n.Key) {
			if i < 16 {
				index1 = (index1<<4)+uint32(i)
				level1++
			}
		}
		t.saveHashes(db, n.Val, level1, index1, h, blockNr)
	case *duoNode:
		if n.flags.t < blockNr {
			return
		}
		if !t.saveFullHash(db, n, level, index, h) {
			return
		}
		i1, i2 := n.childrenIdx()
		t.saveHashes(db, n.child1, level+1, (index<<4)+uint32(i1), h, blockNr)
		t.saveHashes(db, n.child2, level+1, (index<<4)+uint32(i2), h, blockNr)
	case *fullNode:
		if n.flags.t < blockNr {
			return
		}
		// First re-add children, then self
		if !t.saveFullHash(db, n, level, index, h) {
			return
		}
		for i := 0; i<=16; i++ {
			if n.Children[i] != nil {
				t.saveHashes(db, n.Children[i], level+1, (index<<4)+uint32(i), h, blockNr)
			}
		}
	case hashNode:
		if level == 6 {
			//fmt.Printf("saveHash %x %s\n", index, n)
			db.PutHash(index, n)
		}
	}
}

func (t *Trie) tryGet(dbr DatabaseReader, origNode node, key []byte, pos int, blockNr uint64) (value []byte, err error) {
	if t.historical {
		value, err = dbr.GetAsOf(t.bucket[1:], t.bucket, append(t.prefix, key...), blockNr)
	} else {
		value, err = dbr.Get(t.bucket, append(t.prefix, key...))
	}
	if err != nil || value == nil {
		return nil, nil
	}
	return
}

func (t *Trie) tryGet1(db ethdb.Database, origNode node, key []byte, pos int, blockNr uint64) (value []byte, gotValue bool) {
	switch n := (origNode).(type) {
	case nil:
		return nil, true
	case valueNode:
		if t.resolveReads {
			t.addValue(t.prefix, key, pos, []byte(n))
		}
		return n, true
	case *shortNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var adjust bool
		nKey := compactToHex(n.Key)
		if t.resolveReads {
			t.addShort(t.prefix, key, pos, nKey)
		}
		if len(key)-pos < len(nKey) || !bytes.Equal(nKey, key[pos:pos+len(nKey)]) {
			adjust = false
			value, gotValue = nil, true
		} else {
			adjust = true
			value, gotValue = t.tryGet1(db, n.Val, key, pos+len(nKey), blockNr)
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	case *duoNode:
		if t.resolveReads {
			t.addReadProof(t.prefix, key, pos, n.mask &^ (uint32(1) << key[pos]), n.hashesExcept(key[pos]))
		}
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var adjust bool
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			adjust = n.tod(blockNr) == n.child1.tod(blockNr)
			value, gotValue = t.tryGet1(db, n.child1, key, pos+1, blockNr)
		case i2:
			adjust = n.tod(blockNr) == n.child2.tod(blockNr)
			value, gotValue = t.tryGet1(db, n.child2, key, pos+1, blockNr)
		default:
			adjust = false
			value, gotValue = nil, true
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	case *fullNode:
		if t.resolveReads {
			t.addReadProof(t.prefix, key, pos, n.mask() &^ (uint32(1) << key[pos]), n.hashesExcept(key[pos]))
		}
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		child := n.Children[key[pos]]
		adjust := child != nil && n.tod(blockNr) == child.tod(blockNr)
		value, gotValue = t.tryGet1(db, child, key, pos+1, blockNr)
		if adjust {
			n.adjustTod(blockNr)
		}
		return
	case hashNode:
		if t.resolveReads {
			nd, err := t.resolveHash(db, n, key, pos, blockNr)
			if err != nil {
				panic(err)
			}
			return t.tryGet1(db, nd, key, pos, blockNr)
		} else {
			return nil, false
		}
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
func (t *Trie) Update(db ethdb.Database, key, value []byte, blockNr uint64) {
	if err := t.TryUpdate(db, key, value, blockNr); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryUpdate associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
//
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryUpdate(db ethdb.Database, key, value []byte, blockNr uint64) error {
	tc := t.UpdateAction(key, value)
	for !tc.RunWithDb(db, blockNr) {
		r := NewResolver(db, false, t.accounts)
		r.AddContinuation(tc)
		if err := r.ResolveWithDb(db, blockNr); err != nil {
			return err
		}
	}
	t.Hash()
	t.SaveHashes(db, blockNr)
	return nil
}

func (t *Trie) UpdateAction(key, value []byte) *TrieContinuation {
	var tc TrieContinuation
	tc.t = t
	tc.key = keybytesToHex(key)
	if len(value) != 0 {
		tc.action = TrieActionInsert
		tc.value = valueNode(value)
	} else {
		tc.action = TrieActionDelete
	}
	return &tc
}

func (t *Trie) SaveHashes(db ethdb.Database, blockNr uint64) {
	if t.accounts {
		h := newHasher(t.encodeToBytes)
		defer returnHasherToPool(h)
		t.saveHashes(db, t.root, 0, 0, h, blockNr)
	}
}

func (t *Trie) Print(w io.Writer) {
	if t.prefix != nil {
		fmt.Fprintf(w, "%x:", t.prefix)
	}
	if t.root != nil {
		t.root.print(w)
	}
	fmt.Fprintf(w, "\n")
}

func loadNode(br *bufio.Reader) (node, error) {
	nodeType, err := br.ReadString('(')
	if err != nil {
		return nil, err
	}
	switch nodeType[len(nodeType)-2:] {
	case "f(":
		return loadFull(br)
	case "d(":
		return loadDuo(br)
	case "s(":
		return loadShort(br)
	case "h(":
		return loadHash(br)
	case "v(":
		return loadValue(br)
	}
	return nil, fmt.Errorf("unknown node type: %s", nodeType)
}

func loadFull(br *bufio.Reader) (*fullNode, error) {
	n := fullNode{}
	n.flags.dirty = true
	for {
		next, err := br.Peek(1)
		if err != nil {
			return nil, err
		}
		if next[0] == ')' {
			break
		}
		idxStr, err := br.ReadBytes(':')
		if err != nil {
			return nil, err
		}
		idxStr = idxStr[:len(idxStr)-1] // chop off ":"
		idx, err := strconv.ParseInt(string(idxStr), 10, 64)
		if err != nil {
			return nil, err
		}
		n.Children[idx], err = loadNode(br)
		if err != nil {
			return nil, err
		}
	}
	if _, err := br.Discard(1); err != nil { // Discard ")"
		return nil, err
	}
	return &n, nil
}

func loadDuo(br *bufio.Reader) (*duoNode, error) {
	n := duoNode{}
	n.flags.dirty = true
	idxStr1, err := br.ReadBytes(':')
	if err != nil {
		return nil, err
	}
	idxStr1 = idxStr1[:len(idxStr1)-1] // chop off ":"
	idx1, err := strconv.ParseInt(string(idxStr1), 10, 64)
	if err != nil {
		return nil, err
	}
	n.child1, err = loadNode(br)
	if err != nil {
		return nil, err
	}
	idxStr2, err := br.ReadBytes(':')
	if err != nil {
		return nil, err
	}
	idxStr2 = idxStr2[:len(idxStr2)-1] // chop off ":"
	idx2, err := strconv.ParseInt(string(idxStr2), 10, 64)
	if err != nil {
		return nil, err
	}
	n.child2, err = loadNode(br)
	if err != nil {
		return nil, err
	}
	n.mask = (uint32(1)<<uint(idx1)) | (uint32(1)<<uint(idx2))
	if _, err := br.Discard(1); err != nil { // Discard ")"
		return nil, err
	}
	return &n, nil
}

func loadShort(br *bufio.Reader) (*shortNode, error) {
	n := shortNode{}
	n.flags.dirty = true
	keyHexHex, err := br.ReadBytes(':')
	if err != nil {
		return nil, err
	}
	keyHexHex = keyHexHex[:len(keyHexHex)-1]
	keyHex, err := hex.DecodeString(string(keyHexHex))
	if err != nil {
		return nil, err
	}
	n.Key = hexToCompact(keyHex)
	n.Val, err = loadNode(br)
	if err != nil {
		return nil, err
	}
	if _, err := br.Discard(1); err != nil { // Discard ")"
		return nil, err
	}
	return &n, nil
}

func loadHash(br *bufio.Reader) (hashNode, error) {
	hashHex, err := br.ReadBytes(')')
	if err != nil {
		return nil, err
	}
	hashHex = hashHex[:len(hashHex)-1]
	hash, err := hex.DecodeString(string(hashHex))
	if err != nil {
		return nil, err
	}
	return hashNode(hash), nil
}

func loadValue(br *bufio.Reader) (valueNode, error) {
	valHex, err := br.ReadBytes(')')
	if err != nil {
		return nil, err
	}
	valHex = valHex[:len(valHex)-1]
	val, err := hex.DecodeString(string(valHex))
	if err != nil {
		return nil, err
	}
	return valueNode(val), nil
}

func Load(r io.Reader, encodeToBytes bool) (*Trie, error) {
	br := bufio.NewReader(r)
	t := new(Trie)
	t.encodeToBytes = encodeToBytes
	var err error
	t.root, err = loadNode(br)
	return t, err
}

func (t *Trie) PrintDiff(t2 *Trie, w io.Writer) {
	printDiff(t.root, t2.root, w, "", "0x")
}

func (tc *TrieContinuation) RunWithDb(db ethdb.Database, blockNr uint64) bool {
	var done bool
	tc.updated = false
	switch tc.action {
	case TrieActionInsert:
		if tc.t.root == nil {
			newnode := &shortNode{Key: hexToCompact(tc.key[:]), Val: tc.value}
			newnode.flags.dirty = true
			newnode.flags.t = blockNr
			newnode.adjustTod(blockNr)
			tc.t.joinGeneration(blockNr)
			tc.n = newnode
			tc.updated = true
			done = true
		} else {
			done = tc.t.insert(tc.t.root, tc.key, 0, tc.value, tc, blockNr)
		}
	case TrieActionDelete:
		done = tc.t.delete(tc.t.root, tc.key, 0, tc, blockNr)
	}
	if tc.updated {
		for _, touch := range tc.touched {
			tc.t.touch(db, touch.n, touch.key, touch.pos)
		}
		tc.touched = []Touch{}
		tc.t.root = tc.n
	}
	return done
}

type TrieAction int

const (
	TrieActionInsert = iota
	TrieActionDelete
)

type Touch struct {
	n node
	key []byte
	pos int
}

type TrieContinuation struct {
	t *Trie              // trie to act upon
	action TrieAction    // insert of delete
	key []byte           // original key being inserted or deleted
	value node           // original value being inserted or deleted
	resolveKey []byte    // Key for which the resolution is requested
	resolvePos int       // Position in the key for which resolution is requested
	extResolvePos int
	resolveHash hashNode // Expected hash of the resolved node (for correctness checking)
	resolved node        // Node that has been resolved via Database access
	n node               // Returned node after the operation is complete
	updated bool         // Whether the trie was updated
	touched []Touch      // Nodes touched during the operation, by level
}

func (t *Trie) NewContinuation(key []byte, pos int, resolveHash []byte) *TrieContinuation {
	return &TrieContinuation{t: t, key: key, resolveKey: key, resolvePos: pos, resolveHash: hashNode(resolveHash)}
}

func (tc *TrieContinuation) Print() {
	fmt.Printf("tc{t:%x/%x,action:%d,key:%x,resolveKey:%x,resolvePos:%d}\n", tc.t.bucket, tc.t.prefix, tc.action, tc.key, tc.resolveKey, tc.resolvePos)
	if tc.resolved != nil {
		fmt.Printf("%s\n", tc.resolved)
	}
}

func (t *Trie) insert(origNode node, key []byte, pos int, value node, c *TrieContinuation, blockNr uint64) bool {
	c.touched = append(c.touched, Touch{n: origNode, key: key, pos: pos})
	if len(key) == pos {
		if v, ok := origNode.(valueNode); ok {
			if t.resolveReads {
				t.addValue(t.prefix, key, pos, []byte(v))
			}
			c.updated = !bytes.Equal(v, value.(valueNode))
			if c.updated {
				c.n = value
			} else {
				c.n = v
			}
			return true
		} else if t.resolveReads {
			t.addWriteProof(t.prefix, key, pos, uint32(0), []common.Hash{common.BytesToHash(origNode.hash())})
		}
		c.touched = append(c.touched, Touch{n: value, key: key, pos: pos})
		c.updated = true
		c.n = value
		return true
	}
	switch n := origNode.(type) {
	case *shortNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		nKey := compactToHex(n.Key)
		if t.resolveReads {
			t.addShort(t.prefix, key, pos, nKey)
		}
		matchlen := prefixLen(key[pos:], nKey)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		var done bool
		if matchlen == len(nKey) {
			if t.resolveReads {
				fmt.Printf("Short value replace %x, type: %T\n", nKey, n.Val)
				if v, ok := n.Val.(valueNode); ok {
					t.addValue(t.prefix, key, pos+matchlen, v)
				}
			}
			done = t.insert(n.Val, key, pos+matchlen, value, c, blockNr)
			if c.updated {
				n.Val = c.n
				n.flags.dirty = true
			}
			c.n = n
			n.adjustTod(blockNr)
		} else {
			// Otherwise branch out at the index where they differ.
			if t.resolveReads {
				if v, ok := n.Val.(valueNode); ok {
					t.addValue(t.prefix, key, pos+matchlen, v)
				} else {
					t.addWriteProof(t.prefix, key, pos+matchlen, uint32(0), []common.Hash{common.BytesToHash(n.Val.hash())})
				}
			}
			var c1 node
			if len(nKey) == matchlen+1 {
				c1 = n.Val
			} else {
				s1 := &shortNode{Key: hexToCompact(nKey[matchlen+1:]), Val: n.Val}
				s1.flags.dirty = true
				s1.flags.t = blockNr
				s1.adjustTod(blockNr)
				c1 = s1
				t.joinGeneration(blockNr)
			}
			var c2 node
			if len(key) == pos+matchlen+1 {
				c2 = value
			} else {
				s2 := &shortNode{Key: hexToCompact(key[pos+matchlen+1:]), Val: value}
				s2.flags.dirty = true
				s2.flags.t = blockNr
				s2.adjustTod(blockNr)
				c2 = s2
				t.joinGeneration(blockNr)
			}
			branch := &duoNode{}
			if nKey[matchlen] < key[pos+matchlen] {
				branch.child1 = c1
				branch.child2 = c2
			} else {
				branch.child1 = c2
				branch.child2 = c1
			}
			branch.mask = (1 << (nKey[matchlen])) | (1 << (key[pos+matchlen]))
			branch.flags.dirty = true
			branch.flags.t = blockNr
			branch.adjustTod(blockNr)

			// Replace this shortNode with the branch if it occurs at index 0.
			if matchlen == 0 {
				c.n = branch // current node leaves the generation, but new node branch joins it
			} else {
				// Otherwise, replace it with a short node leading up to the branch.
				n.Key = hexToCompact(key[pos:pos+matchlen])
				n.Val = branch
				t.joinGeneration(blockNr) // new branch node joins the generation
				n.flags.dirty = true
				c.n = n
				n.adjustTod(blockNr)
			}
			c.updated = true
			done = true
		}
		return done

	case *duoNode:
		if t.resolveReads {
			t.addWriteProof(t.prefix, key, pos, n.mask &^ (uint32(1) << key[pos]), n.hashesExcept(key[pos]))
		}
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var done bool
		var adjust bool
		i1, i2 := n.childrenIdx()
		switch key[pos] {
		case i1:
			adjust = n.child1 != nil && n.tod(blockNr) == n.child1.tod(blockNr)
			if n.child1 == nil {
				if len(key) == pos+1 {
					n.child1 = value
				} else {
					short := &shortNode{Key: hexToCompact(key[pos+1:]), Val: value}
					short.flags.dirty = true
					short.flags.t = blockNr
					short.adjustTod(blockNr)
					t.joinGeneration(blockNr)
					n.child1 = short
				}
				c.updated = true
				n.flags.dirty = true
				done = true
			} else {
				done = t.insert(n.child1, key, pos+1, value, c, blockNr)
				if c.updated {
					n.child1 = c.n
					n.flags.dirty = true
				}
			}
			c.n = n
		case i2:
			adjust = n.child2 != nil && n.tod(blockNr) == n.child2.tod(blockNr)
			if n.child2 == nil {
				if len(key) == pos+1 {
					n.child2 = value
				} else {
					short := &shortNode{Key: hexToCompact(key[pos+1:]), Val: value}
					short.flags.dirty = true
					short.flags.t = blockNr
					short.adjustTod(blockNr)
					t.joinGeneration(blockNr)
					n.child2 = short
				}
				c.updated = true
				n.flags.dirty = true
				done = true
			} else {
				done = t.insert(n.child2, key, pos+1, value, c, blockNr)
				if c.updated {
					n.child2 = c.n
					n.flags.dirty = true
				}
			}
			c.n = n
		default:
			var child node
			if len(key) == pos+1 {
				child = value
			} else {
				short := &shortNode{Key: hexToCompact(key[pos+1:]), Val: value}
				short.flags.dirty = true
				short.flags.t = blockNr
				short.adjustTod(blockNr)
				t.joinGeneration(blockNr)
				child = short
			}
			newnode := &fullNode{}
			newnode.Children[i1] = n.child1
			newnode.Children[i2] = n.child2
			newnode.flags.dirty = true
			newnode.flags.t = blockNr
			newnode.adjustTod(blockNr)
			adjust = false
			newnode.Children[key[pos]] = child
			c.updated = true
			c.n = newnode // current node leaves the generation but newnode joins it
			done = true
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return done

	case *fullNode:
		if t.resolveReads {
			t.addWriteProof(t.prefix, key, pos, n.mask() &^ (uint32(1) << key[pos]), n.hashesExcept(key[pos]))
		}
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		child := n.Children[key[pos]]
		adjust := child != nil && n.tod(blockNr) == child.tod(blockNr)
		var done bool
		if child == nil {
			if len(key) == pos+1 {
				n.Children[key[pos]] = value
			} else {
				short := &shortNode{Key: hexToCompact(key[pos+1:]), Val: value}
				short.flags.dirty = true
				short.flags.t = blockNr
				short.adjustTod(blockNr)
				t.joinGeneration(blockNr)
				n.Children[key[pos]] = short
			}
			c.updated = true
			n.flags.dirty = true
			done = true
		} else {
			done = t.insert(child, key, pos+1, value, c, blockNr)
			if c.updated {
				n.Children[key[pos]] = c.n
				n.flags.dirty = true
			}
		}
		c.n = n
		if adjust {
			n.adjustTod(blockNr)
		}
		return done
	case hashNode:
		var done bool
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and insert into it. This leaves all child nodes on
		// the path to the value in the trie.
		if c.resolved == nil || !bytes.Equal(key, c.resolveKey) || pos != c.resolvePos {
			c.resolved = nil
			c.resolveKey = key
			c.resolvePos = pos
			c.resolveHash = common.CopyBytes(n)
			c.updated = false
			done = false // Need resolution
		} else {
			rn := c.resolved
			t.timestampSubTree(rn, blockNr)
			c.resolved = nil
			c.resolveKey = nil
			c.resolvePos = 0
			done = t.insert(rn, key, pos, value, c, blockNr)
			if !c.updated {
				c.updated = true // Substitution of the hashNode with resolved node is an update
				c.n = rn
			}
		}
		return done

	default:
		fmt.Printf("Key: %s, Prefix: %s\n", hex.EncodeToString(key[pos:]), hex.EncodeToString(key[:pos]))
		t.PrintTrie()
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// Delete removes any existing value for key from the trie.
func (t *Trie) Delete(db ethdb.Database, key []byte, blockNr uint64) {
	if err := t.TryDelete(db, key, blockNr); err != nil {
		log.Error(fmt.Sprintf("Unhandled trie error: %v", err))
	}
}

// TryDelete removes any existing value for key from the trie.
// If a node was not found in the database, a MissingNodeError is returned.
func (t *Trie) TryDelete(db ethdb.Database, key []byte, blockNr uint64) error {
	tc := t.DeleteAction(key)
	for !tc.RunWithDb(db, blockNr) {
		r := NewResolver(db, false, t.accounts)
		r.AddContinuation(tc)
		if err := r.ResolveWithDb(db, blockNr); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trie) DeleteAction(key []byte) *TrieContinuation {
	var tc TrieContinuation
	tc.t = t
	tc.key = keybytesToHex(key)
	tc.action = TrieActionDelete
	return &tc
}

func (t *Trie) convertToShortNode(key []byte, keyStart int, child node, pos uint, c *TrieContinuation, blockNr uint64, done bool) bool {
	cnode := child
	if pos != 16 {
		// If the remaining entry is a short node, it replaces
		// n and its key gets the missing nibble tacked to the
		// front. This avoids creating an invalid
		// shortNode{..., shortNode{...}}.  Since the entry
		// might not be loaded yet, resolve it just for this
		// check.
		//rkey := make([]byte, len(key))
		rkey := make([]byte, keyStart+1)
		copy(rkey, key[:keyStart])
		rkey[keyStart] = byte(pos)
		if childHash, ok := child.(hashNode); ok {
			if c.resolved == nil || !bytes.Equal(rkey, c.resolveKey) || keyStart+1 != c.resolvePos {
				// It is either unresolved or resolved by other request
				c.resolved = nil
				c.resolveKey = rkey
				c.resolvePos = keyStart+1
				c.resolveHash = common.CopyBytes(childHash)
				return false // Need resolution
			}
			cnode = c.resolved
			t.timestampSubTree(cnode, blockNr)
			c.resolved = nil
			c.resolveKey = nil
			c.resolvePos = 0
		}
		if cnode, ok := cnode.(*shortNode); ok {
			c.touched = append(c.touched, Touch{n: cnode, key: rkey, pos: keyStart+1})
			k := append([]byte{byte(pos)}, compactToHex(cnode.Key)...)
			newshort := &shortNode{Key: hexToCompact(k)}
			t.leftGeneration(cnode.flags.t)
			newshort.Val = cnode.Val
			newshort.flags.dirty = true
			newshort.flags.t = blockNr
			newshort.adjustTod(blockNr)
			// cnode gets removed, but newshort gets added
			c.updated = true
			c.n = newshort
			if t.resolveReads && done {
				t.addShort(t.prefix, key, keyStart, k)
				proofKey := make([]byte, len(key))
				copy(proofKey, key[:keyStart])
				copy(proofKey[keyStart:], k)
				if v, isValue := newshort.Val.(valueNode); isValue {
					t.addValue(t.prefix, proofKey, keyStart+len(k), v)
				} else {
					t.addWriteProof(t.prefix, proofKey, keyStart+len(k), uint32(0), []common.Hash{})
				}
			}
			return done
		}
	}
	// Otherwise, n is replaced by a one-nibble short node
	// containing the child.
	newshort := &shortNode{Key: hexToCompact([]byte{byte(pos)})}
	newshort.Val = cnode
	newshort.flags.dirty = true
	newshort.flags.t = blockNr
	newshort.adjustTod(blockNr)
	c.updated = true
	c.n = newshort
	if t.resolveReads && done {
		t.addShort(t.prefix, key, keyStart, []byte{byte(pos)})
		proofKey := make([]byte, len(key))
		copy(proofKey, key[:keyStart])
		proofKey[keyStart] = byte(pos)
		if v, isValue := newshort.Val.(valueNode); isValue {
			t.addValue(t.prefix, proofKey, keyStart+1, v)
		} else {
			t.addWriteProof(t.prefix, proofKey, keyStart+1, uint32(0), []common.Hash{})
		}
	}
	return done
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
func (t *Trie) delete(origNode node, key []byte, keyStart int, c *TrieContinuation, blockNr uint64) bool {
	c.touched = append(c.touched, Touch{n: origNode, key: key, pos: keyStart})
	switch n := origNode.(type) {
	case *shortNode:
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var done bool
		nKey := compactToHex(n.Key)
		if t.resolveReads {
			t.addShort(t.prefix, key, keyStart, nKey)
		}
		matchlen := prefixLen(key[keyStart:], nKey)
		if matchlen < len(nKey) {
			c.updated = false
			c.n = n
			done = true // don't replace n on mismatch
		} else if matchlen == len(key) - keyStart {
			t.leftGeneration(n.flags.t)
			c.updated = true
			c.n = nil
			done = true // remove n entirely for whole matches
		} else {
			// The key is longer than n.Key. Remove the remaining suffix
			// from the subtrie. Child can never be nil here since the
			// subtrie must contain at least two other values with keys
			// longer than n.Key.
			done = t.delete(n.Val, key, keyStart+len(nKey), c, blockNr)
			if !c.updated {
				c.n = n
			} else {
				child := c.n
				if child == nil {
					t.leftGeneration(n.flags.t)
					c.n = nil
					done = true
				} else {
					if shortChild, ok := child.(*shortNode); ok {
						// Deleting from the subtrie reduced it to another
						// short node. Merge the nodes to avoid creating a
						// shortNode{..., shortNode{...}}. Use concat (which
						// always creates a new slice) instead of append to
						// avoid modifying n.Key since it might be shared with
						// other nodes.
						childKey := compactToHex(shortChild.Key)
						newnode := &shortNode{Key: hexToCompact(concat(nKey, childKey...))}
						newnode.Val = shortChild.Val
						newnode.flags.dirty = true
						newnode.flags.t = blockNr
						newnode.adjustTod(blockNr)
						// We do not increase generation count here, because one short node comes, but another one 
						t.leftGeneration(shortChild.flags.t) // But shortChild goes away
						c.touched = append(c.touched, Touch{n: shortChild, key: key, pos: keyStart+len(nKey)})
						c.n = newnode
					} else {
						n.Val = child
						n.flags.dirty = true
						n.adjustTod(blockNr)
						c.n = n
					}
				}
				c.updated = true
			}
		}
		return done

	case *duoNode:
		if t.resolveReads {
			t.addWriteProof(t.prefix, key, keyStart, n.mask &^ (uint32(1) << key[keyStart]), n.hashesExcept(key[keyStart]))
		}
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		var done bool
		var adjust bool
		i1, i2 := n.childrenIdx()
		switch key[keyStart] {
		case i1:
			adjust = n.child1 != nil && n.tod(blockNr) == n.child1.tod(blockNr)
			done = t.delete(n.child1, key, keyStart+1, c, blockNr)
			if !c.updated && !done {
				c.n = n
			} else {
				nn := c.n
				n.child1 = nn
				if nn == nil {
					if n.child2 == nil {
						adjust = false
						t.leftGeneration(n.flags.t)
						c.n = nil
						c.updated = true
						done = true
					} else {
						done = t.convertToShortNode(key, keyStart, n.child2, uint(i2), c, blockNr, done)
					}
				}
				if nn != nil || (nn == nil && !done) {
					if c.updated {
						n.flags.dirty = true
					}
					c.n = n
				}
			}
		case i2:
			adjust = n.child2 != nil && n.tod(blockNr) == n.child2.tod(blockNr)
			done = t.delete(n.child2, key, keyStart+1, c, blockNr)
			if !c.updated && !done {
				c.n = n
			} else {
				nn := c.n
				n.child2 = nn
				if nn == nil {
					if n.child1 == nil {
						adjust = false
						t.leftGeneration(n.flags.t)
						c.n = nil
						c.updated = true
						done = true
					} else {
						done = t.convertToShortNode(key, keyStart, n.child1, uint(i1), c, blockNr, done)
						//if trace {
						//	fmt.Printf("i2, converting to short node, done: %t, keyStart %d\n", done, keyStart)
						//}
					}
				}
				if nn != nil || (nn == nil && !done) {
					if c.updated {
						n.flags.dirty = true
					}
					c.n = n
				}
			}
		default:
			adjust = false
			c.updated = false
			c.n = n
			done = true
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return done

	case *fullNode:
		if t.resolveReads {
			t.addWriteProof(t.prefix, key, keyStart, n.mask() &^ (uint32(1) << key[keyStart]), n.hashesExcept(key[keyStart]))
		}
		n.updateT(blockNr, t.joinGeneration, t.leftGeneration)
		child := n.Children[key[keyStart]]
		adjust := child != nil && n.tod(blockNr) == child.tod(blockNr)
		done := t.delete(child, key, keyStart+1, c, blockNr)
		if !c.updated && !done {
			c.n = n
			done = false
		} else {
			nn := c.n
			n.Children[key[keyStart]] = nn
			// Check how many non-nil entries are left after deleting and
			// reduce the full node to a short node if only one entry is
			// left. Since n must've contained at least two children
			// before deletion (otherwise it would not be a full node) n
			// can never be reduced to nil.
			//
			// When the loop is done, pos contains the index of the single
			// value that is left in n or -2 if n contains at least two
			// values.
			var pos1, pos2 int
			count := 0
			for i, cld := range &n.Children {
				if cld != nil {
					if count == 0 {
						pos1 = i
					}
					if count == 1 {
						pos2 = i
					}
					count++
					if count > 2 {
						break
					}
				}
			}
			if count == 0 {
				t.leftGeneration(n.flags.t)
				c.n = nil
				c.updated = true
				done = true
			} else if count == 1 {
				done = t.convertToShortNode(key, keyStart, n.Children[pos1], uint(pos1), c, blockNr, done)
			} else if count == 2 {
				duo := &duoNode{}
				if pos1 == int(key[keyStart]) {
					duo.child1 = nn
				} else {
					duo.child1 = n.Children[pos1]
				}
				if pos2 == int(key[keyStart]) {
					duo.child2 = nn
				} else {
					duo.child2 = n.Children[pos2]
				}
				duo.flags.dirty = true
				duo.mask = (1 << uint(pos1)) | (uint32(1) << uint(pos2))
				duo.flags.t = blockNr
				duo.adjustTod(blockNr)
				adjust = false
				c.updated = true
				c.n = duo
			}
			if count > 2 || (count == 1 && !done) {
				if c.updated {
					// n still contains at least three values and cannot be reduced.
					n.flags.dirty = true
				}
				c.n = n
			}
		}
		if adjust {
			n.adjustTod(blockNr)
		}
		return done

	case valueNode:
		c.updated = true
		c.n = nil
		return true

	case nil:
		c.updated = false
		c.n = nil
		return true

	case hashNode:
		var done bool
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and delete from it. This leaves all child nodes on
		// the path to the value in the trie.
		if c.resolved == nil || !bytes.Equal(key, c.resolveKey) || keyStart != c.resolvePos {
			// It is either unresolved, or resolved by other request
			c.resolved = nil
			c.resolveKey = key
			c.resolvePos = keyStart
			c.resolveHash = common.CopyBytes(n)
			c.updated = false
			done = false // Need resolution
		} else {
			rn := c.resolved
			t.timestampSubTree(rn, blockNr)
			c.resolved = nil
			c.resolveKey = nil
			c.resolvePos = 0
			done = t.delete(rn, key, keyStart, c, blockNr)
			if !c.updated {
				c.updated = true // Substitution is an update
				c.n = rn
			}
		}
		return done

	default:
		panic(fmt.Sprintf("%T: invalid node: %v (%v)", n, n, key[:keyStart]))
	}
}

func (t *Trie) PrepareToRemove() {
	t.prepareToRemove(t.root)
}

func (t *Trie) prepareToRemove(n node) {
	switch n := n.(type) {
	case *shortNode:
		t.leftGeneration(n.flags.t)
		t.prepareToRemove(n.Val)
	case *duoNode:
		t.leftGeneration(n.flags.t)
		t.prepareToRemove(n.child1)
		t.prepareToRemove(n.child2)
	case *fullNode:
		t.leftGeneration(n.flags.t)
		for _, child := range n.Children {
			if child != nil {
				t.prepareToRemove(child)
			}
		}
	}	
}

// Timestamp given node and all descendants
func (t *Trie) timestampSubTree(n node, blockNr uint64) {
	switch n := n.(type) {
	case *shortNode:
		if n.flags.t == 0 {
			n.flags.t = blockNr
			n.flags.tod = blockNr
			t.joinGeneration(blockNr)
			t.timestampSubTree(n.Val, blockNr)
		}
	case *duoNode:
		if n.flags.t == 0 {
			n.flags.t = blockNr
			n.flags.tod = blockNr
			t.joinGeneration(blockNr)
			t.timestampSubTree(n.child1, blockNr)
			t.timestampSubTree(n.child2, blockNr)
		}
	case *fullNode:
		if n.flags.t == 0 {
			n.flags.t = blockNr
			n.flags.tod = blockNr
			t.joinGeneration(blockNr)
			for _, child := range n.Children {
				if child != nil {
					t.timestampSubTree(child, blockNr)
				}
			}
		}
	}
}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

func (t *Trie) resolveHash(db ethdb.Database, n hashNode, key []byte, pos int, blockNr uint64) (node, error) {
	root, gotHash, err := t.rebuildHashes(db, key, pos, blockNr, t.accounts, n)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(n, gotHash) {
		fmt.Printf("Resolving wrong hash for prefix %x, bucket %x prefix %x block %d\n", key[:pos], t.bucket, t.prefix, blockNr)
		fmt.Printf("Expected hash %s\n", n)
		fmt.Printf("Got hash %s\n", hashNode(gotHash))
		fmt.Printf("Stack: %s\n", debug.Stack())
		return nil, &MissingNodeError{NodeHash: common.BytesToHash(n), Path: key[:pos]}
	}
	return root, err
}

// Root returns the root hash of the trie.
// Deprecated: use Hash instead.
func (t *Trie) Root() []byte { return t.Hash().Bytes() }

// Hash returns the root hash of the trie. It does not write to the
// database and can be used even if the trie doesn't have one.
func (t *Trie) Hash() common.Hash {
	hash, _ := t.hashRoot()
	return common.BytesToHash(hash.(hashNode))
}

func (t *Trie) UnloadOlderThan(gen uint64) bool {
	h := newHasher(t.encodeToBytes)
	defer returnHasherToPool(h)
	hn, unloaded := unloadOlderThan(t.root, gen, h, true)
	if unloaded {
		t.root = hn
		return true
	}
	return false
}

func unloadOlderThan(n node, gen uint64, h *hasher, isRoot bool) (hashNode, bool) {
	if n == nil {
		return nil, false
	}
	switch n := (n).(type) {
	case *shortNode:
		if n.flags.t < gen {
			if n.flags.dirty {
				var hn common.Hash
				if h.hash(n, isRoot, hn[:]) == 32 {
					return hashNode(hn[:]), true
				} else {
					// Embedded node does not have a hash and cannot be unloaded
					return nil, false
				}
			}
			return hashNode(common.CopyBytes(n.hash())), true
		}
		if n.flags.tod < gen {
			if hn, unloaded := unloadOlderThan(n.Val, gen, h, false); unloaded {
				n.Val = hn
			}
		}
	case *duoNode:
		if n.flags.t < gen {
			if n.flags.dirty {
				var hn common.Hash
				if h.hash(n, isRoot, hn[:]) == 32 {
					return hashNode(hn[:]), true
				} else {
					// Embedded node does not have a hash and cannot be unloaded
					return nil, false
				}
			}
			return hashNode(common.CopyBytes(n.hash())), true
		}
		if n.flags.tod < gen {
			if hn, unloaded := unloadOlderThan(n.child1, gen, h, false); unloaded {
				n.child1 = hn
			}
			if hn, unloaded := unloadOlderThan(n.child2, gen, h, false); unloaded {
				n.child2 = hn
			}
		}
	case *fullNode:
		if n.flags.t < gen {
			if n.flags.dirty {
				var hn common.Hash
				if h.hash(n, isRoot, hn[:]) == 32 {
					return hashNode(hn[:]), true
					// Embedded node does not have a hash and cannot be unloaded
					return nil, false
				}
			}
			return hashNode(common.CopyBytes(n.hash())), true
		}
		for i, child := range n.Children {
			if child != nil {
				if hn, unloaded := unloadOlderThan(child, gen, h, false); unloaded {
					n.Children[i] = hn
				}
			}
		}
	}
	return nil, false
}

func (t *Trie) CountNodes(m map[uint64]int) int {
	return countNodes(t.root, m)
}

func countNodes(n node, m map[uint64]int) int {
	if n == nil {
		return 0
	}
	switch n := (n).(type) {
	case *shortNode:
		c := countNodes(n.Val, m)
		m[n.flags.t]++
		return 1 + c
	case *duoNode:
		c1 := countNodes(n.child1, m)
		c2 := countNodes(n.child2, m)
		m[n.flags.t]++
		return 1 + c1 + c2
	case *fullNode:
		count := 1
		for _, child := range n.Children {
			if child != nil {
				count += countNodes(child, m)
			}
		}
		m[n.flags.t]++
		return count
	}
	return 0
}

func (t *Trie) CountOccupancies(db ethdb.Database, blockNr uint64, o map[int]map[int]int) {
	if hn, ok := t.root.(hashNode); ok {
		n, err := t.resolveHash(db, hn, []byte{}, 0, blockNr)
		if err != nil {
			panic(err)
		}
		t.root = n
	}
	t.countOccupancies(t.root, 0, o)
}

func (t *Trie) countOccupancies(n node, level int, o map[int]map[int]int) {
	if n == nil {
		return
	}
	switch n := (n).(type) {
	case *shortNode:
		t.countOccupancies(n.Val, level+1, o)
		if _, exists := o[level]; !exists {
			o[level] = make(map[int]int)
		}
		o[level][18] = o[level][18]+1
	case *duoNode:
		t.countOccupancies(n.child1, level+1, o)
		t.countOccupancies(n.child2, level+1, o)
		if _, exists := o[level]; !exists {
			o[level] = make(map[int]int)
		}
		o[level][2] = o[level][2]+1
	case *fullNode:
		count := 0
		for i := 0; i<=16; i++ {
			if n.Children[i] != nil {
				count++
				t.countOccupancies(n.Children[i], level+1, o)
			}
		}
		if _, exists := o[level]; !exists {
			o[level] = make(map[int]int)
		}
		o[level][count] = o[level][count]+1
	}
	return
}

func (t *Trie) hashRoot() (node, error) {
	if t.root == nil {
		return hashNode(emptyRoot.Bytes()), nil
	}
	h := newHasher(t.encodeToBytes)
	defer returnHasherToPool(h)
	var hn common.Hash
	h.hash(t.root, true, hn[:])
	return hashNode(hn[:]), nil
}
