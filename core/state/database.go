// Copyright 2017 The go-ethereum Authors
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

package state

import (
	"bytes"
	"fmt"
	"hash"
	"io"
	"runtime"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	lru "github.com/hashicorp/golang-lru"
	"golang.org/x/crypto/sha3"
)

// Trie cache generation limit after which to evict trie nodes from memory.
var MaxTrieCacheGen = uint32(4*1024*1024)

var AccountsBucket = []byte("AT")
var AccountsHistoryBucket = []byte("hAT")
var StorageBucket = []byte("ST")
var StorageHistoryBucket = []byte("hST")
var CodeBucket = []byte("CODE")

const (
	// Number of past tries to keep. This value is chosen such that
	// reasonable chain reorg depths will hit an existing trie.
	maxPastTries = 12

	// Number of codehash->size associations to keep.
	codeSizeCacheSize = 100000
)

type StateReader interface {
	ReadAccountData(address common.Address) (*Account, error)
	ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error)
	ReadAccountCode(codeHash common.Hash) ([]byte, error)
	ReadAccountCodeSize(codeHash common.Hash) (int, error)
}

type StateWriter interface {
	UpdateAccountData(address common.Address, original, account *Account) error
	UpdateAccountCode(codeHash common.Hash, code []byte) error
	DeleteAccount(address common.Address, original *Account) error
	WriteAccountStorage(address common.Address, key, original, value *common.Hash) error
}

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

type hasher struct {
	sha     keccakState
}

var hasherPool = make(chan *hasher, 128)

func newHasher() *hasher {
	var h *hasher
	select {
		case h = <- hasherPool:
		default:
			h = &hasher{sha: sha3.NewLegacyKeccak256().(keccakState)}
	}
	return h
}

func returnHasherToPool(h *hasher) {
	select {
		case hasherPool <- h:
		default:
			fmt.Printf("Allowing hasher to be garbage collected, pool is full\n")
	}
}

type NoopWriter struct {
}

func NewNoopWriter() *NoopWriter {
	return &NoopWriter{}
}

func (nw *NoopWriter) UpdateAccountData(address common.Address, original, account *Account) error {
	return nil
}

func (nw *NoopWriter) DeleteAccount(address common.Address, original *Account) error {
	return nil
}

func (nw *NoopWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (nw *NoopWriter) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	return nil
}

// Implements StateReader by wrapping a trie and a database, where trie acts as a cache for the database
type TrieDbState struct {
	t                *trie.Trie
	db               ethdb.Database
	blockNr          uint64
	storageTries     map[common.Hash]*trie.Trie
	storageUpdates   map[common.Address]map[common.Hash][]byte
	accountUpdates   map[common.Hash]*Account
	deleted          map[common.Hash]struct{}
	codeCache        *lru.Cache
	codeSizeCache    *lru.Cache
	historical       bool
	generationCounts map[uint64]int
	nodeCount        int
	oldestGeneration uint64
	noHistory        bool
	resolveReads     bool
	readProofMasks   map[string]uint32
	readProofHashes  map[string][16]common.Hash
	//writeProofMasks  map[string]uint32
	//writeProofHashes map[string][16]common.Hash
	proofShorts      map[string]string
	proofValues      [][]byte
	proofCodes       map[common.Hash]struct{}
}

func NewTrieDbState(root common.Hash, db ethdb.Database, blockNr uint64) (*TrieDbState, error) {
	csc, err := lru.New(100000)
	if err != nil {
		return nil, err
	}
	cc, err := lru.New(10000)
	if err != nil {
		return nil, err
	}
	t := trie.New(root, AccountsBucket, nil, false)
	tds := TrieDbState{
		t: t,
		db: db,
		blockNr: blockNr,
		storageTries: make(map[common.Hash]*trie.Trie),
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Hash]*Account),
		deleted: make(map[common.Hash]struct{}),
		readProofMasks: make(map[string]uint32),
		readProofHashes: make(map[string][16]common.Hash),
		//writeProofMasks: make(map[string]uint32),
		//writeProofHashes: make(map[string][16]common.Hash),
		proofShorts: make(map[string]string),
		//proofValues: make(map[string][]byte),
		proofCodes: make(map[common.Hash]struct{}),
		codeCache: cc,
		codeSizeCache: csc,
	}
	t.MakeListed(tds.joinGeneration, tds.leftGeneration, tds.addReadProof, tds.addWriteProof, tds.addValue, tds.addShort)
	tds.generationCounts = make(map[uint64]int, 4096)
	tds.oldestGeneration = blockNr
	return &tds, nil
}

func (tds *TrieDbState) SetHistorical(h bool) {
	tds.historical = h
	tds.t.SetHistorical(h)
}

func (tds *TrieDbState) SetResolveReads(rr bool) {
	tds.resolveReads = rr
	tds.t.SetResolveReads(rr)
}

func (tds *TrieDbState) SetNoHistory(nh bool) {
	tds.noHistory = nh
}

func (tds *TrieDbState) Copy() *TrieDbState {
	tcopy := *tds.t
	cpy := TrieDbState{
		t: &tcopy,
		db: tds.db,
		blockNr: tds.blockNr,
		storageTries: make(map[common.Hash]*trie.Trie),
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Hash]*Account),
		deleted: make(map[common.Hash]struct{}),
		readProofMasks: make(map[string]uint32),
		readProofHashes: make(map[string][16]common.Hash),
		//writeProofMasks: make(map[string]uint32),
		//writeProofHashes: make(map[string][16]common.Hash),
		proofShorts: make(map[string]string),
		//proofValues: make(map[string][]byte),
		proofCodes: make(map[common.Hash]struct{}),
	}
	return &cpy
}

func (tds *TrieDbState) Database() ethdb.Database {
	return tds.db
}

func (tds *TrieDbState) AccountTrie() *trie.Trie {
	return tds.t
}

func (tds *TrieDbState) TrieRoot() (common.Hash, error) {
	root, err := tds.trieRoot(true)
	tds.clearUpdates()
	return root, err
}

func (tds *TrieDbState) ExtractProofs() (masks []uint32, hashes []common.Hash, shortLens []int, values [][]byte) {
	fmt.Printf("Extracting proofs for block %d\n", tds.blockNr)
	// Collect all the strings
	keys := []string{}
	keySet := make(map[string]struct{})
	storageKeys := []string{}
	storageKeySet := make(map[string]struct{})
	for key := range tds.readProofMasks {
		if len(key) <= 65 {
			if _, ok := keySet[key]; !ok {	
				keys = append(keys, key)
				keySet[key] = struct{}{}
			}
		} else {
			if _, ok := storageKeySet[key]; !ok {	
				storageKeys = append(storageKeys, key)
				storageKeySet[key] = struct{}{}
			}
		}
	}
	/*
	for key := range tds.writeProofMasks {
		if len(key) <= 65 {
			if _, ok := keySet[key]; !ok {	
				keys = append(keys, key)
				keySet[key] = struct{}{}
			}
		} else {
			if _, ok := storageKeySet[key]; !ok {	
				storageKeys = append(storageKeys, key)
				storageKeySet[key] = struct{}{}
			}
		}
	}
	*/
	for key := range tds.proofShorts {
		if len(key) <= 65 {
			if _, ok := keySet[key]; !ok {	
				keys = append(keys, key)
				keySet[key] = struct{}{}
			}
		} else {
			if _, ok := storageKeySet[key]; !ok {	
				storageKeys = append(storageKeys, key)
				storageKeySet[key] = struct{}{}
			}
		}
	}
	/*
	for key := range tds.proofValues {
		if len(key) <= 65 {
			if _, ok := keySet[key]; !ok {	
				keys = append(keys, key)
				keySet[key] = struct{}{}
			}
		} else {
			if _, ok := storageKeySet[key]; !ok {	
				storageKeys = append(storageKeys, key)
				storageKeySet[key] = struct{}{}
			}
		}
	}
	*/
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%x\n", key)
		var rwMask uint32
		var maskPresent bool = false
		var harray [16]common.Hash
		if mask, ok := tds.readProofMasks[key]; ok {
			rwMask |= mask
			h := tds.readProofHashes[key]
			for i := byte(0); i < 16; i++ {
				if mask & (uint32(1) << i) != 0 {
					harray[i] = h[i]
				}
			}
			maskPresent = true
		}
		/*
		if mask, ok := tds.writeProofMasks[key]; ok {
			rwMask |= mask
			h := tds.writeProofHashes[key]
			for i := byte(0); i < 16; i++ {
				if mask & (uint32(1) << i) != 0 {
					harray[i] = h[i]
				}
			}
			maskPresent = true
		}
		*/
		if maskPresent {
			fmt.Printf("Mask %16b\n", rwMask)
			// Determine the downward mask
			for i := byte(0); i < 16; i++ {
				if rwMask & (uint32(1) << i) != 0 {
					hashes = append(hashes, harray[i])
				}
			}
			var downmask uint32
			for nibble := byte(0); nibble < 16; nibble++ {
				if _, ok1 := keySet[key + string(nibble)]; ok1 {
					downmask |= (uint32(1) << nibble)
				}
			}
			fmt.Printf("Down %16b\n", downmask)
			masks = append(masks, rwMask | (downmask << 16))
		}
		if short, ok := tds.proofShorts[key]; ok {
			fmt.Printf("Short %x\n", short)
			var downmask uint32
			if len(key) + len(short) < 65 {
				for nibble := byte(0); nibble < 16; nibble++ {
					if _, ok1 := keySet[key + short + string(nibble)]; ok1 {
						downmask |= (uint32(1) << nibble)
					}
				}
				fmt.Printf("Down %16b\n", downmask)
			}
			masks = append(masks, (downmask << 16))
			shortLens = append(shortLens, len(short))
		}
		/*
		if value, ok := tds.proofValues[key]; ok {
			fmt.Printf("Value %x\n", value)
		}
		*/
	}
	fmt.Printf("Masks:")
	for _, mask := range masks {
		fmt.Printf(" %32b", mask)
	}
	fmt.Printf("\n")
	fmt.Printf("Shorts:")
	for _, shortLen := range shortLens {
		fmt.Printf(" %d", shortLen)
	}
	fmt.Printf("\n")
	//hashes1 := tds.t.ExtractProofs(masks, shortLens, tds.db, tds.blockNr)
	fmt.Printf("Hashes:")
	for _, hash := range hashes {
		fmt.Printf(" %x", hash)
	}
	fmt.Printf("\n")
	values = tds.proofValues
	fmt.Printf("Values:")
	for _, value := range values {
		if value == nil {
			fmt.Printf(" nil")
		} else {
			fmt.Printf(" %x", value)
		}
	}
	fmt.Printf("\n")
	//fmt.Printf("Hashes1:")
	//for i, hash := range hashes1 {
	//	if hash != hashes[i] {
	//		fmt.Printf("  %d: %x!=%x", i, hash, hashes[i])
	//	}
	//}
	//fmt.Printf("\n")
	tds.readProofMasks = make(map[string]uint32)
	tds.readProofHashes = make(map[string][16]common.Hash)
	//tds.writeProofMasks = make(map[string]uint32)
	//tds.writeProofHashes = make(map[string][16]common.Hash)
	tds.proofShorts = make(map[string]string)
	tds.proofValues = nil
	tds.proofCodes = make(map[common.Hash]struct{})
	return masks, hashes, shortLens, values
}

func (tds *TrieDbState) PrintTrie(w io.Writer) {
	tds.t.Print(w)
	for _, storageTrie := range tds.storageTries {
		storageTrie.Print(w)
	}
}

func (tds *TrieDbState) trieRoot(forward bool) (common.Hash, error) {
	if len(tds.storageUpdates) == 0 && len(tds.accountUpdates) == 0 {
		return tds.t.Hash(), nil
	}
	//for address, account := range tds.accountUpdates {
	//	fmt.Printf("%x %d %x %x\n", address[:], account.Balance, account.CodeHash, account.Root[:])
	//}
	//fmt.Printf("=================\n")
	oldContinuations := []*trie.TrieContinuation{}
	newContinuations := []*trie.TrieContinuation{}
	for address, m := range tds.storageUpdates {
		addrHash, err := tds.HashAddress(&address, false /*save*/)
		if err != nil {
			return common.Hash{}, nil
		}
		if _, ok := tds.deleted[addrHash]; ok {
			continue
		}
		storageTrie, err := tds.getStorageTrie(address, addrHash, true)
		if err != nil {
			return common.Hash{}, err
		}
		for keyHash, v := range m {
			var c *trie.TrieContinuation
			if len(v) > 0 {
				c = storageTrie.UpdateAction(keyHash[:], v)
			} else {
				c = storageTrie.DeleteAction(keyHash[:])
			}
			oldContinuations = append(oldContinuations, c)
		}
	}
	it := 0
	for len(oldContinuations) > 0 {
		var resolver *trie.TrieResolver
		for _, c := range oldContinuations {
			if !c.RunWithDb(tds.db, tds.blockNr) {
				newContinuations = append(newContinuations, c)
				if resolver == nil {
					resolver = trie.NewResolver(tds.db, false, false)
					resolver.SetHistorical(tds.historical)
				}
				resolver.AddContinuation(c)
			}
		}
		if len(newContinuations) > 0 {
			if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
				return common.Hash{}, err
			}
			resolver = nil
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
		it++
	}
	if it > 3 {
		fmt.Printf("Resolved storage in %d iterations\n", it)
	}
	oldContinuations = []*trie.TrieContinuation{}
	newContinuations = []*trie.TrieContinuation{}
	for addrHash, account := range tds.accountUpdates {
		var c *trie.TrieContinuation
		// first argument to getStorageTrie is not used unless the last one == true
		storageTrie, err := tds.getStorageTrie(common.Address{}, addrHash, false)
		if err != nil {
			return common.Hash{}, err
		}
		deleteStorageTrie := false
		if account != nil {
			if _, ok := tds.deleted[addrHash]; ok {
				deleteStorageTrie = true
				account.Root = emptyRoot
			} else if storageTrie != nil && forward {
				account.Root = storageTrie.Hash()
			}
			//fmt.Printf("Set root %x %x\n", address[:], account.Root[:])
			data, err := rlp.EncodeToBytes(account)
			if err != nil {
				return common.Hash{}, err
			}
			c = tds.t.UpdateAction(addrHash[:], data)
		} else {
			deleteStorageTrie = true
			c = tds.t.DeleteAction(addrHash[:])
		}
		if deleteStorageTrie && storageTrie != nil {
			delete(tds.storageTries, addrHash)
			storageTrie.PrepareToRemove()
		}
		oldContinuations = append(oldContinuations, c)
	}
	it = 0
	for len(oldContinuations) > 0 {
		var resolver *trie.TrieResolver
		for _, c := range oldContinuations {
			if !c.RunWithDb(tds.db, tds.blockNr) {
				newContinuations = append(newContinuations, c)
				if resolver == nil {
					resolver = trie.NewResolver(tds.db, false, true)
					resolver.SetHistorical(tds.historical)
				}
				resolver.AddContinuation(c)
			}
		}
		if len(newContinuations) > 0 {
			if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
				return common.Hash{}, err
			}
			resolver = nil
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
		it++
	}
	if it > 3 {
		fmt.Printf("Resolved in %d iterations\n", it)
	}
	hash := tds.t.Hash()
	tds.t.SaveHashes(tds.db, tds.blockNr)
	return hash, nil
}

func (tds *TrieDbState) clearUpdates() {
	tds.storageUpdates = make(map[common.Address]map[common.Hash][]byte)
	tds.accountUpdates = make(map[common.Hash]*Account)
	tds.deleted = make(map[common.Hash]struct{})
}

func (tds *TrieDbState) Rebuild() {
	tr := tds.AccountTrie()
	tr.Rebuild(tds.db, tds.blockNr)
}

func (tds *TrieDbState) SetBlockNr(blockNr uint64) {
	tds.blockNr = blockNr
}

func (tds *TrieDbState) UnwindTo(blockNr uint64) error {
	fmt.Printf("Rewinding from block %d to block %d\n", tds.blockNr, blockNr)
	var accountPutKeys [][]byte
	var accountPutVals [][]byte
	var accountDelKeys [][]byte
	var storagePutKeys [][]byte
	var storagePutVals [][]byte
	var storageDelKeys [][]byte
	if err := tds.db.RewindData(tds.blockNr, blockNr, func (bucket, key, value []byte) error {
		//var pre []byte
		if len(key) == 32 {
			//pre, _ = tds.db.Get(trie.SecureKeyPrefix, key)
		} else {
			//pre, _ = tds.db.Get(trie.SecureKeyPrefix, key[20:52])
		}
		//fmt.Printf("Rewind with key %x (%x) value %x\n", key, pre, value)
		var err error
		if bytes.Equal(bucket, AccountsHistoryBucket) {
			var addrHash common.Hash
			copy(addrHash[:], key)
			if len(value) > 0 {
				tds.accountUpdates[addrHash], err = encodingToAccount(value)
				if err != nil {
					return err
				}
				accountPutKeys = append(accountPutKeys, key)
				accountPutVals = append(accountPutVals, value)
			} else {
				//fmt.Printf("Deleted account\n")
				tds.accountUpdates[addrHash] = nil
				tds.deleted[addrHash] = struct{}{}
				accountDelKeys = append(accountDelKeys, key)
			}
		} else if bytes.Equal(bucket, StorageHistoryBucket) {
			var address common.Address
			copy(address[:], key[:20])
			var keyHash common.Hash
			copy(keyHash[:], key[20:])
			m, ok := tds.storageUpdates[address]
			if !ok {
				m = make(map[common.Hash][]byte)
				tds.storageUpdates[address] = m
			}
			m[keyHash] = value
			if len(value) > 0 {
				storagePutKeys = append(storagePutKeys, key)
				storagePutVals = append(storagePutVals, value)
			} else {
				//fmt.Printf("Deleted storage item\n")
				storageDelKeys = append(storageDelKeys, key)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if _, err := tds.trieRoot(false); err != nil {
		return err
	}
	for addrHash, account := range tds.accountUpdates {
		if account == nil {
			if err := tds.db.Delete(AccountsBucket, addrHash[:]); err != nil {
				return err
			}
		} else {
			value, err := accountToEncoding(account)
			if err != nil {
				return err
			}
			if err := tds.db.Put(AccountsBucket, addrHash[:], value); err != nil {
				return err
			}
		}
	}
	for address, m := range tds.storageUpdates {
		for keyHash, value := range m {
			if len(value) == 0 {
				if err := tds.db.Delete(StorageBucket, append(address[:], keyHash[:]...)); err != nil {
					return err
				}
			} else {
				if err := tds.db.Put(StorageBucket, append(address[:], keyHash[:]...), value); err != nil {
					return err
				}
			}
		}
	}
	for i := tds.blockNr; i > blockNr; i-- {
		if err := tds.db.DeleteTimestamp(i); err != nil {
			return err
		}
	}
	tds.clearUpdates()
	tds.blockNr = blockNr
	return nil
}

func accountToEncoding(account *Account) ([]byte, error) {
	var data []byte
	var err error
	if (account.CodeHash == nil || bytes.Equal(account.CodeHash, emptyCodeHash)) && (account.Root == emptyRoot || account.Root == common.Hash{}) {
		if (account.Balance == nil || account.Balance.Sign() == 0) && account.Nonce == 0 {
			data = []byte{byte(192)}
		} else {
			var extAccount ExtAccount
			extAccount.Nonce = account.Nonce
			extAccount.Balance = account.Balance
			if extAccount.Balance == nil {
				extAccount.Balance = new(big.Int)
			}
			data, err = rlp.EncodeToBytes(extAccount)
			if err != nil {
				return nil, err
			}
		}
	} else {
		a := *account
		if a.Balance == nil {
			a.Balance = new(big.Int)
		}
		if a.CodeHash == nil {
			a.CodeHash = emptyCodeHash
		}
		if a.Root == (common.Hash{}) {
			a.Root = emptyRoot
		}
		data, err = rlp.EncodeToBytes(a)
		if err != nil {
			return nil, err
		}
	}
	return data, err
}

func encodingToAccount(enc []byte) (*Account, error) {
	if enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var data Account
	// Kind of hacky
	if len(enc) == 1 {
		data.Balance = new(big.Int)
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else if len(enc) < 60 {
		var extData ExtAccount
		if err := rlp.DecodeBytes(enc, &extData); err != nil {
			return nil, err
		}
		data.Nonce = extData.Nonce
		data.Balance = extData.Balance
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else {
		if err := rlp.DecodeBytes(enc, &data); err != nil {
			return nil, err
		}
	}
	return &data, nil
}

func (tds *TrieDbState) joinGeneration(gen uint64) {
	tds.nodeCount++
	tds.generationCounts[gen]++

}

func (tds *TrieDbState) leftGeneration(gen uint64) {
	tds.nodeCount--
	tds.generationCounts[gen]--
}

func (tds *TrieDbState) addReadProof(prefix, key []byte, pos int, mask uint32, hashes []common.Hash) {
	if tds.resolveReads {
		k := make([]byte, len(prefix) + pos)
		copy(k, prefix)
		copy(k[len(prefix):], key[:pos])
		ks := string(k)
		if m, ok := tds.readProofMasks[ks]; ok {
			intersection := m & mask
			tds.readProofMasks[ks] = intersection
			h := tds.readProofHashes[ks]
			idx := 0
			for i := byte(0); i < 16; i++ {
				if intersection & (uint32(1) << i) != 0 {
					h[i] = hashes[idx]
					idx++
				} else {
					h[i] = common.Hash{}
				}
			}
			tds.readProofHashes[ks] = h
		} else {
			tds.readProofMasks[ks] = mask
			var h [16]common.Hash
			idx := 0
			for i := byte(0); i < 16; i++ {
				if mask & (uint32(1) << i) != 0 {
					h[i] = hashes[idx]
					idx++
				}
			}
			tds.readProofHashes[ks] = h
		}
	}
}

func (tds *TrieDbState) addWriteProof(prefix, key []byte, pos int, mask uint32, hashes []common.Hash) {
	if tds.resolveReads {
		k := make([]byte, len(prefix) + pos)
		copy(k, prefix)
		copy(k[len(prefix):], key[:pos])
		ks := string(k)
		if m, ok := tds.readProofMasks[ks]; ok {
			intersection := m & mask
			tds.readProofMasks[ks] = intersection
			h := tds.readProofHashes[ks]
			idx := 0
			for i := byte(0); i < 16; i++ {
				if intersection & (uint32(1) << i) != 0 {
					h[i] = hashes[idx]
					idx++
				} else {
					h[i] = common.Hash{}
				}
			}
			// Not update
			//tds.readProofHashes[ks] = h
		} else {
			tds.readProofMasks[ks] = mask
			var h [16]common.Hash
			idx := 0
			for i := byte(0); i < 16; i++ {
				if mask & (uint32(1) << i) != 0 {
					h[i] = hashes[idx]
					idx++
				}
			}
			tds.readProofHashes[ks] = h
		}
	}
}

func (tds *TrieDbState) addValue(prefix, key []byte, pos int, value []byte) {
	if tds.resolveReads {
		k := make([]byte, len(prefix) + pos)
		copy(k, prefix)
		copy(k[len(prefix):], key[:pos])
		//ks := string(k)
		tds.proofValues = append(tds.proofValues, value)
		/*
		if _, ok := tds.proofValues[ks]; !ok {
			tds.proofValues[string(k)] = value
		}
		*/
	}
}

func (tds *TrieDbState) addShort(prefix, key []byte, pos int, short []byte) {
	if tds.resolveReads {
		k := make([]byte, len(prefix) + pos)
		copy(k, prefix)
		copy(k[len(prefix):], key[:pos])
		ks := string(k)
		if _, ok := tds.proofShorts[ks]; !ok {
			tds.proofShorts[string(k)] = string(common.CopyBytes(short))
		}
	}
}

func (tds *TrieDbState) ReadAccountData(address common.Address) (*Account, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := tds.t.TryGet(tds.db, buf[:], tds.blockNr)
	if err != nil {
		return nil, err
	}
	return encodingToAccount(enc)
}

func (tds *TrieDbState) savePreimage(save bool, hash, preimage []byte) error {
	if !save {
		return nil
	}
	return tds.db.Put(trie.SecureKeyPrefix, hash, preimage)
}

func (tds *TrieDbState) HashAddress(address *common.Address, save bool) (common.Hash, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	return buf, tds.savePreimage(save, buf[:], address[:])
}

func (tds *TrieDbState) HashKey(key *common.Hash, save bool) (common.Hash, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	return buf, tds.savePreimage(save, buf[:], key[:])
}

func (tds *TrieDbState) GetKey(shaKey []byte) []byte {
	key, _ := tds.db.Get(trie.SecureKeyPrefix, shaKey)
	return key
}

func (tds *TrieDbState) getStorageTrie(address common.Address, addrHash common.Hash, create bool) (*trie.Trie, error) {
	t, ok := tds.storageTries[addrHash]
	if !ok && create {
		account, err := tds.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if account == nil {
			t = trie.New(common.Hash{}, StorageBucket, address[:], true)
		} else {
			t = trie.New(account.Root, StorageBucket, address[:], true)
		}
		t.SetHistorical(tds.historical)
		t.SetResolveReads(tds.resolveReads)
		t.MakeListed(tds.joinGeneration, tds.leftGeneration, tds.addReadProof, tds.addWriteProof, tds.addValue, tds.addShort)
		tds.storageTries[addrHash] = t
	}
	return t, nil
}

func (tds *TrieDbState) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	addrHash, err := tds.HashAddress(&address, false /*save*/)
	if err != nil {
		return nil, err
	}
	t, err := tds.getStorageTrie(address, addrHash, true)
	if err != nil {
		return nil, err
	}
	seckey, err := tds.HashKey(key, false /*save*/)
	if err != nil {
		return nil, err
	}
	enc, err := t.TryGet(tds.db, seckey[:], tds.blockNr)
	if err != nil {
		return nil, err
	}
	return enc, nil
}

func (tds *TrieDbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if tds.resolveReads {
		tds.proofCodes[codeHash] = struct{}{}
	}
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if cached, ok := tds.codeCache.Get(codeHash); ok {
		return cached.([]byte), nil
	}
	code, err := tds.db.Get(CodeBucket, codeHash[:])
	if err == nil {
		tds.codeSizeCache.Add(codeHash, len(code))
		tds.codeCache.Add(codeHash, code)
	}
	return code, err
}

func (tds *TrieDbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	if tds.resolveReads {
		tds.proofCodes[codeHash] = struct{}{}
	}
	if cached, ok := tds.codeSizeCache.Get(codeHash); ok {
		return cached.(int), nil
	}
	code, err := tds.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

var prevMemStats runtime.MemStats

func (tds *TrieDbState) PruneTries() {
	if tds.nodeCount > int(MaxTrieCacheGen) {
		toRemove := 0
		excess := tds.nodeCount - int(MaxTrieCacheGen)
		gen := tds.oldestGeneration
		for excess > 0 {
			excess -= tds.generationCounts[gen]
			toRemove += tds.generationCounts[gen]
			delete(tds.generationCounts, gen)
			gen++
		}
		// Unload all nodes with touch timestamp < gen
		for addrHash, storageTrie := range tds.storageTries {
			empty := storageTrie.UnloadOlderThan(gen)
			if empty {
				delete(tds.storageTries, addrHash)
			}
		}
		tds.t.UnloadOlderThan(gen)
		tds.oldestGeneration = gen
		tds.nodeCount -= toRemove
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Info("Memory", "nodes", tds.nodeCount, "alloc", int(m.Alloc / 1024), "sys", int(m.Sys / 1024), "numGC", int(m.NumGC))
	}
}

type TrieStateWriter struct {
	tds *TrieDbState
}

type DbStateWriter struct {
	tds *TrieDbState
}

func (tds *TrieDbState) TrieStateWriter() *TrieStateWriter {
	return &TrieStateWriter{tds: tds}
}

func (tds *TrieDbState) DbStateWriter() *DbStateWriter {
	return &DbStateWriter{tds: tds}
}

var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

func accountsEqual(a1, a2 *Account) bool {
	if a1.Nonce != a2.Nonce {
		return false
	}
	if a1.Balance == nil {
		if a2.Balance != nil {
			return false
		}
	} else if a2.Balance == nil {
		return false
	} else if a1.Balance.Cmp(a2.Balance) != 0 {
		return false
	}
	if a1.Root != a2.Root {
		return false
	}
	if a1.CodeHash == nil {
		if a2.CodeHash != nil {
			return false
		}
	} else if a2.CodeHash == nil {
		return false
	} else if !bytes.Equal(a1.CodeHash, a2.CodeHash) {
		return false
	}
	return true
}

func (tsw *TrieStateWriter) UpdateAccountData(address common.Address, original, account *Account) error {
	addrHash, err := tsw.tds.HashAddress(&address, false /*save*/)
	if err != nil {
		return err
	}
	tsw.tds.accountUpdates[addrHash] = account
	return nil
}

func (dsw *DbStateWriter) UpdateAccountData(address common.Address, original, account *Account) error {
	data, err := accountToEncoding(account)
	if err != nil {
		return err
	}
	addrHash, err := dsw.tds.HashAddress(&address, true /*save*/)
	if err != nil {
		return err
	}
	if err = dsw.tds.db.Put(AccountsBucket, addrHash[:], data); err != nil {
		return err
	}
	if dsw.tds.noHistory {
		return nil
	}
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
	}
	var originalData []byte
	if original.Balance == nil {
		originalData = []byte{}
	} else {
		originalData, err = accountToEncoding(original)
		if err != nil {
			return err
		}
	}
	return dsw.tds.db.PutS(AccountsHistoryBucket, addrHash[:], originalData, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) DeleteAccount(address common.Address, original *Account) error {
	addrHash, err := tsw.tds.HashAddress(&address, false /*save*/)
	if err != err {
		return err
	}
	tsw.tds.accountUpdates[addrHash] = nil
	tsw.tds.deleted[addrHash] = struct{}{}
	return nil
}

func (dsw *DbStateWriter) DeleteAccount(address common.Address, original *Account) error {
	addrHash, err := dsw.tds.HashAddress(&address, true /*save*/)
	if err != nil {
		return err
	}
	if err := dsw.tds.db.Delete(AccountsBucket, addrHash[:]); err != nil {
		return err
	}
	if dsw.tds.noHistory {
		return nil
	}
	var originalData []byte
	if original.Balance == nil {
		// Account has been created and deleted in the same block
		originalData = []byte{}
	} else {
		originalData, err = accountToEncoding(original)
		if err != nil {
			return err
		}
	}
	return dsw.tds.db.PutS(AccountsHistoryBucket, addrHash[:], originalData, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (dsw *DbStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return dsw.tds.db.Put(CodeBucket, codeHash[:], code)
}

func (tsw *TrieStateWriter) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	v := bytes.TrimLeft(value[:], "\x00")
	m, ok := tsw.tds.storageUpdates[address]
	if !ok {
		m = make(map[common.Hash][]byte)
		tsw.tds.storageUpdates[address] = m
	}
	seckey, err := tsw.tds.HashKey(key, false /*save*/)
	if err != nil {
		return err
	}
	if len(v) > 0 {
		m[seckey] = common.CopyBytes(v)
	} else {
		m[seckey] = nil
	}
	return nil
}

func (dsw *DbStateWriter) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	if *original == *value {
		return nil
	}
	seckey, err := dsw.tds.HashKey(key, true /*save*/)
	if err != nil {
		return err
	}
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	compositeKey := append(address[:], seckey[:]...)
	if len(v) == 0 {
		err = dsw.tds.db.Delete(StorageBucket, compositeKey)
	} else {
		err = dsw.tds.db.Put(StorageBucket, compositeKey, vv)
	}
	if err != nil {
		return err
	}
	if dsw.tds.noHistory {
		return nil
	}
	o := bytes.TrimLeft(original[:], "\x00")
	oo := make([]byte, len(o))
	copy(oo, o)
	return dsw.tds.db.PutS(StorageHistoryBucket, compositeKey, oo, dsw.tds.blockNr)
}

// Database wraps access to tries and contract code.
type Database interface {
	// OpenTrie opens the main account trie.
	OpenTrie(root common.Hash) (Trie, error)

	// OpenStorageTrie opens the storage trie of an account.
	OpenStorageTrie(addrHash, root common.Hash) (Trie, error)

	// CopyTrie returns an independent copy of the given trie.
	CopyTrie(Trie) Trie

	// ContractCode retrieves a particular contract's code.
	ContractCode(addrHash, codeHash common.Hash) ([]byte, error)

	// ContractCodeSize retrieves a particular contracts code's size.
	ContractCodeSize(addrHash, codeHash common.Hash) (int, error)

	// TrieDB retrieves the low level trie database used for data storage.
	TrieDB() ethdb.Database
}

// Trie is a Ethereum Merkle Trie.
type Trie interface {
	Prove(db ethdb.Database, key []byte, fromLevel uint, proofDb ethdb.Putter, blockNr uint64) error
	TryGet(db ethdb.Database, key []byte, blockNr uint64) ([]byte, error)
	TryUpdate(db ethdb.Database, key, value []byte, blockNr uint64) error
	TryDelete(db ethdb.Database, key []byte, blockNr uint64) error
	Hash() common.Hash
	NodeIterator(db ethdb.Database, startKey []byte, blockNr uint64) trie.NodeIterator
	GetKey(trie.DatabaseReader, []byte) []byte // TODO(fjl): remove this when SecureTrie is removed
}

// NewDatabase creates a backing store for state. The returned database is safe for
// concurrent use and retains a few recent expanded trie nodes in memory. To keep
// more historical state in memory, use the NewDatabaseWithCache constructor.
func NewDatabase(db ethdb.Database) Database {
	return NewDatabaseWithCache(db, 0)
}

// NewDatabase creates a backing store for state. The returned database is safe for
// concurrent use and retains both a few recent expanded trie nodes in memory, as
// well as a lot of collapsed RLP trie nodes in a large memory cache.
func NewDatabaseWithCache(db ethdb.Database, cache int) Database {
	csc, _ := lru.New(codeSizeCacheSize)
	return &cachingDB{
		db:            db,
		codeSizeCache: csc,
	}
}

type cachingDB struct {
	db            ethdb.Database
	codeSizeCache *lru.Cache
}

// OpenTrie opens the main account trie.
func (db *cachingDB) OpenTrie(root common.Hash) (Trie, error) {
	return trie.NewSecure(root, AccountsBucket, nil, false)
}

// OpenStorageTrie opens the storage trie of an account.
func (db *cachingDB) OpenStorageTrie(addrHash, root common.Hash) (Trie, error) {
	return trie.NewSecure(root, StorageBucket, addrHash[:], true)
}

// CopyTrie returns an independent copy of the given trie.
func (db *cachingDB) CopyTrie(t Trie) Trie {
	switch t := t.(type) {
	case *trie.SecureTrie:
		return t.Copy()
	default:
		panic(fmt.Errorf("unknown trie type %T", t))
	}
}

// ContractCode retrieves a particular contract's code.
func (db *cachingDB) ContractCode(addrHash, codeHash common.Hash) ([]byte, error) {
	code, err := db.db.Get(CodeBucket, codeHash[:])
	if err == nil {
		db.codeSizeCache.Add(codeHash, len(code))
	}
	return code, err
}

// ContractCodeSize retrieves a particular contracts code's size.
func (db *cachingDB) ContractCodeSize(addrHash, codeHash common.Hash) (int, error) {
	if cached, ok := db.codeSizeCache.Get(codeHash); ok {
		return cached.(int), nil
	}
	code, err := db.ContractCode(addrHash, codeHash)
	return len(code), err
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *cachingDB) TrieDB() ethdb.Database {
	return db.db
}
