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
	"encoding/binary"
	//"runtime/debug"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	lru "github.com/hashicorp/golang-lru"
	"github.com/petar/GoLLRB/llrb"
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

type storageItem struct {
	key, seckey, value common.Hash
}

func (a *storageItem) Less(b llrb.Item) bool {
	bi := b.(*storageItem)
	return bytes.Compare(a.seckey[:], bi.seckey[:]) < 0
}

// Implements StateReader by wrapping database only, without trie
type DbState struct {
	db ethdb.Getter
	blockNr uint64
	storage map[common.Address]*llrb.LLRB
}

func NewDbState(db ethdb.Getter, blockNr uint64) *DbState {
	return &DbState{
		db: db,
		blockNr: blockNr,
		storage: make(map[common.Address]*llrb.LLRB),
	}
}

func (dbs *DbState) SetBlockNr(blockNr uint64) {
	dbs.blockNr = blockNr
}

func (dbs *DbState) ForEachStorage(addr common.Address, start []byte, cb func(key, seckey, value common.Hash) bool, maxResults int) {
	st := llrb.New()
	var s [20+32]byte
	copy(s[:], addr[:])
	copy(s[20:], start)
	var lastSecKey common.Hash
	overrideCounter := 0
	emptyHash := common.Hash{}
	min := &storageItem{seckey: common.BytesToHash(start)}
	if t, ok := dbs.storage[addr]; ok {
		t.AscendGreaterOrEqual1(min, func(i llrb.Item) bool {
			item := i.(*storageItem)
			st.ReplaceOrInsert(item)
			if item.value != emptyHash {
				copy(lastSecKey[:], item.seckey[:])
				// Only count non-zero items
				overrideCounter++
			}
			return overrideCounter < maxResults
		})
	}
	numDeletes := st.Len() - overrideCounter
	dbs.db.WalkAsOf(StorageBucket, StorageHistoryBucket, s[:], 0, dbs.blockNr+1, func(ks, vs []byte) (bool, error) {
		if !bytes.HasPrefix(ks, addr[:]) {
			return false, nil
		}
		if vs == nil || len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		seckey := ks[20:]
		//fmt.Printf("seckey: %x\n", seckey)
		si := storageItem{}
		copy(si.seckey[:], seckey)
		if st.Has(&si) {
			return true, nil
		}
		si.value.SetBytes(vs)
		st.InsertNoReplace(&si)
		if bytes.Compare(seckey[:], lastSecKey[:]) > 0 {
			// Beyond overrides
			return st.Len() < maxResults + numDeletes, nil
		}
		return st.Len() < maxResults + overrideCounter + numDeletes, nil
	})
	results := 0
	st.AscendGreaterOrEqual1(min, func(i llrb.Item) bool {
		item := i.(*storageItem)
		if item.value != emptyHash {
			// Skip if value == 0
			if item.key == emptyHash {
				key, err := dbs.db.Get(trie.SecureKeyPrefix, item.seckey[:])
				if err == nil {
					copy(item.key[:], key)
				} else {
					log.Error("Error getting preimage", "err", err)
				}
			}
			cb(item.key, item.seckey, item.value)
			results++
		}
		return results < maxResults
	})
}

func (dbs *DbState) ReadAccountData(address common.Address) (*Account, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := dbs.db.GetAsOf(AccountsBucket, AccountsHistoryBucket, buf[:], dbs.blockNr+1)
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	return encodingToAccount(enc)
}

func (dbs *DbState) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := dbs.db.GetAsOf(StorageBucket, StorageHistoryBucket, append(address[:], buf[:]...), dbs.blockNr+1)
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (dbs *DbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return dbs.db.Get(CodeBucket, codeHash[:])
}

func (dbs *DbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	code, err := dbs.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (dbs *DbState) UpdateAccountData(address common.Address, original, account *Account) error {
	return nil
}

func (dbs *DbState) DeleteAccount(address common.Address, original *Account) error {
	return nil
}

func (dbs *DbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (dbs *DbState) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	t, ok := dbs.storage[address]
	if !ok {
		t = llrb.New()
		dbs.storage[address] = t
	}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	i := &storageItem{key: *key, value: *value}
	h.sha.Read(i.seckey[:])
	t.ReplaceOrInsert(i)
	return nil
}

type TraceDbState struct {
	currentDb ethdb.Database
}

func NewTraceDbState(db ethdb.Database) *TraceDbState {
	return &TraceDbState{
		currentDb: db,
	}
}

func (tds *TraceDbState) ReadAccountData(address common.Address) (*Account, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := tds.currentDb.Get(AccountsBucket, buf[:])
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	return encodingToAccount(enc)
}

func (tds *TraceDbState) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := tds.currentDb.Get(StorageBucket, append(address[:], buf[:]...))
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (tds *TraceDbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return tds.currentDb.Get(CodeBucket, codeHash[:])
}

func (tds *TraceDbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	code, err := tds.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (tds *TraceDbState) UpdateAccountData(address common.Address, original, account *Account) error {
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
	}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	data, err := accountToEncoding(account)
	if err != nil {
		return err
	}
	return tds.currentDb.Put(AccountsBucket, addrHash[:], data)
}

func (tds *TraceDbState) DeleteAccount(address common.Address, original *Account) error {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	return tds.currentDb.Delete(AccountsBucket, addrHash[:])
}

func (tds *TraceDbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return tds.currentDb.Put(CodeBucket, codeHash[:], code)
}

func (tds *TraceDbState) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	if *original == *value {
		return nil
	}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var seckey common.Hash
	h.sha.Read(seckey[:])
	compositeKey := append(address[:], seckey[:]...)
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	if len(v) == 0 {
		return tds.currentDb.Delete(StorageBucket, compositeKey)
	} else {
		return tds.currentDb.Put(StorageBucket, compositeKey, vv)
	}
}

type RepairDbState struct {
	currentDb ethdb.Database
	historyDb ethdb.Database
	blockNr uint64
	storageTries     map[common.Address]*trie.Trie
	storageUpdates   map[common.Address]map[common.Hash][]byte
	generationCounts map[uint64]int
	nodeCount        int
	oldestGeneration uint64
	accountsKeys map[string]struct{}
	storageKeys map[string]struct{}
}

func NewRepairDbState(currentDb ethdb.Database, historyDb ethdb.Database, blockNr uint64) *RepairDbState {
	return &RepairDbState{
		currentDb: currentDb,
		historyDb: historyDb,
		blockNr: blockNr,
		storageTries: make(map[common.Address]*trie.Trie),
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		generationCounts: make(map[uint64]int, 4096),
		oldestGeneration: blockNr,
		accountsKeys: make(map[string]struct{}),
		storageKeys: make(map[string]struct{}),
	}
}

func (rds *RepairDbState) SetBlockNr(blockNr uint64) {
	rds.blockNr = blockNr
	rds.accountsKeys = make(map[string]struct{})
	rds.storageKeys = make(map[string]struct{})
}

// If highZero is true, the most significant bits of every byte is left zero
func encodeTimestamp(timestamp uint64) []byte {
	var suffix []byte
	var limit uint64
	limit = 32
	for bytecount := 1; bytecount <=8; bytecount++ {
		if timestamp < limit {
			suffix = make([]byte, bytecount)
			b := timestamp
			for i := bytecount - 1; i > 0; i-- {
				suffix[i] = byte(b&0xff)
				b >>= 8
			}
			suffix[0] = byte(b) | (byte(bytecount)<<5) // 3 most significant bits of the first byte are bytecount
			break
		}
		limit <<= 8
	}
	return suffix
}

func (rds *RepairDbState) CheckKeys() {
	aSet := make(map[string]struct{})
	suffix := encodeTimestamp(rds.blockNr)
	{
		suffixkey := make([]byte, len(suffix) + len(AccountsHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], AccountsHistoryBucket)
		v, _ := rds.historyDb.Get(ethdb.SuffixBucket, suffixkey)
		if len(v) > 0 {
			keycount := int(binary.BigEndian.Uint32(v))
			for i, ki := 4, 0; ki < keycount; ki++ {
				l := int(v[i])
				i++
				aSet[string(v[i:i+l])] = struct{}{}
				i += l
			}
		}
	}
	aDiff := len(aSet) != len(rds.accountsKeys)
	if !aDiff {
		for a, _ := range aSet {
			if _, ok := rds.accountsKeys[a]; !ok {
				aDiff = true
				break
			}
		}
	}
	if aDiff {
		fmt.Printf("Accounts key set does not match for block %d\n", rds.blockNr)
		newlen := 4 + len(rds.accountsKeys)
		for key, _ := range rds.accountsKeys {
			newlen += len(key)
		}
		dv := make([]byte, newlen)
		binary.BigEndian.PutUint32(dv, uint32(len(rds.accountsKeys)))
		i := 4
		for key, _ := range rds.accountsKeys {
			dv[i] = byte(len(key))
			i++
			copy(dv[i:], key)
			i += len(key)
		}
		suffixkey := make([]byte, len(suffix) + len(AccountsHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], AccountsHistoryBucket)
		if err := rds.historyDb.Put(ethdb.SuffixBucket, suffixkey, dv); err != nil {
			panic(err)
		}
	}
	sSet := make(map[string]struct{})
	{
		suffixkey := make([]byte, len(suffix) + len(StorageHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], StorageHistoryBucket)
		v, _ := rds.historyDb.Get(ethdb.SuffixBucket, suffixkey)
		if len(v) > 0 {
			keycount := int(binary.BigEndian.Uint32(v))
			for i, ki := 4, 0; ki < keycount; ki++ {
				l := int(v[i])
				i++
				sSet[string(v[i:i+l])] = struct{}{}
				i += l
			}
		}
	}
	sDiff := len(sSet) != len(rds.storageKeys)
	if !sDiff {
		for s, _ := range sSet {
			if _, ok := rds.storageKeys[s]; !ok {
				sDiff = true
				break
			}
		}
	}
	if sDiff {
		fmt.Printf("Storage key set does not match for block %d\n", rds.blockNr)
		newlen := 4 + len(rds.storageKeys)
		for key, _ := range rds.storageKeys {
			newlen += len(key)
		}
		dv := make([]byte, newlen)
		binary.BigEndian.PutUint32(dv, uint32(len(rds.storageKeys)))
		i := 4
		for key, _ := range rds.storageKeys {
			dv[i] = byte(len(key))
			i++
			copy(dv[i:], key)
			i += len(key)
		}
		suffixkey := make([]byte, len(suffix) + len(StorageHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], StorageHistoryBucket)
		if err := rds.historyDb.Put(ethdb.SuffixBucket, suffixkey, dv); err != nil {
			panic(err)
		}
	}
}

func (rds *RepairDbState) ReadAccountData(address common.Address) (*Account, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := rds.currentDb.Get(AccountsBucket, buf[:])
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	return encodingToAccount(enc)
}

func (rds *RepairDbState) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := rds.currentDb.Get(StorageBucket, append(address[:], buf[:]...))
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (rds *RepairDbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return rds.currentDb.Get(CodeBucket, codeHash[:])
}

func (rds *RepairDbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	code, err := rds.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (rds *RepairDbState) getStorageTrie(address common.Address, create bool) (*trie.Trie, error) {
	t, ok := rds.storageTries[address]
	if !ok && create {
		account, err := rds.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if account == nil {
			t = trie.New(common.Hash{}, StorageBucket, address[:], true)
		} else {
			t = trie.New(account.Root, StorageBucket, address[:], true)
		}
		t.MakeListed(rds.joinGeneration, rds.leftGeneration)
		rds.storageTries[address] = t
	}
	return t, nil
}

func (rds *RepairDbState) UpdateAccountData(address common.Address, original, account *Account) error {
	oldContinuations := []*trie.TrieContinuation{}
	newContinuations := []*trie.TrieContinuation{}
	var storageTrie *trie.Trie
	var err error
	if m, ok := rds.storageUpdates[address]; ok {
		storageTrie, err = rds.getStorageTrie(address, true)
		if err != nil {
			return err
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
		delete(rds.storageUpdates, address)
	}
	it := 0
	for len(oldContinuations) > 0 {
		var resolver *trie.TrieResolver
		for _, c := range oldContinuations {
			if !c.RunWithDb(rds.currentDb, rds.blockNr) {
				newContinuations = append(newContinuations, c)
				if resolver == nil {
					resolver = trie.NewResolver(rds.currentDb, false, false)
				}
				resolver.AddContinuation(c)
			}
		}
		if len(newContinuations) > 0 {
			if err := resolver.ResolveWithDb(rds.currentDb, rds.blockNr); err != nil {
				return err
			}
			resolver = nil
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
		it++
	}
	if it > 3 {
		fmt.Printf("Resolved storage in %d iterations\n", it)
	}
	if storageTrie != nil {
		account.Root = storageTrie.Hash()
	}
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
	}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	rds.accountsKeys[string(addrHash[:])]=struct{}{}
	data, err := accountToEncoding(account)
	if err != nil {
		return err
	}
	if err = rds.currentDb.Put(AccountsBucket, addrHash[:], data); err != nil {
		return err
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
	v, _ := rds.historyDb.GetS(AccountsHistoryBucket, addrHash[:], rds.blockNr)
	if !bytes.Equal(v, originalData) {
		fmt.Printf("REPAIR (UpdateAccountData): At block %d, address: %x, expected %x, found %x\n", rds.blockNr, address, originalData, v)
		return rds.historyDb.PutS(AccountsHistoryBucket, addrHash[:], originalData, rds.blockNr)
	}
	return nil
}

func (rds *RepairDbState) DeleteAccount(address common.Address, original *Account) error {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	rds.accountsKeys[string(addrHash[:])]=struct{}{}

	delete(rds.storageUpdates, address)

	if err := rds.currentDb.Delete(AccountsBucket, addrHash[:]); err != nil {
		return err
	}
	var originalData []byte
	var err error
	if original.Balance == nil {
		// Account has been created and deleted in the same block
		originalData = []byte{}
	} else {
		originalData, err = accountToEncoding(original)
		if err != nil {
			return err
		}
	}
	v, _ := rds.historyDb.GetS(AccountsHistoryBucket, addrHash[:], rds.blockNr)
	if !bytes.Equal(v, originalData) {
		fmt.Printf("REPAIR (DeleteAccount): At block %d, address: %x, expected %x, found %x\n", rds.blockNr, address, originalData, v)
		return rds.historyDb.PutS(AccountsHistoryBucket, addrHash[:], originalData, rds.blockNr)
	}
	return nil
}

func (rds *RepairDbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return rds.currentDb.Put(CodeBucket, codeHash[:], code)
}

func (rds *RepairDbState) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var seckey common.Hash
	h.sha.Read(seckey[:])
	compositeKey := append(address[:], seckey[:]...)
	if *original == *value {
		val, _ := rds.historyDb.GetS(StorageHistoryBucket, compositeKey, rds.blockNr)
		if val != nil {
			fmt.Printf("REPAIR (WriteAccountStorage): At block %d, address: %x, key %x, expected nil, found %x\n", rds.blockNr, address, key, val)
			suffix := encodeTimestamp(rds.blockNr)
			return rds.historyDb.Delete(StorageHistoryBucket, append(compositeKey, suffix...))			
		}
		return nil
	}
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	m, ok := rds.storageUpdates[address]
	if !ok {
		m = make(map[common.Hash][]byte)
		rds.storageUpdates[address] = m
	}
	if len(v) > 0 {
		m[seckey] = vv
	} else {
		m[seckey] = nil
	}

	rds.storageKeys[string(compositeKey)] = struct{}{}
	var err error
	if len(v) == 0 {
		err = rds.currentDb.Delete(StorageBucket, compositeKey)
	} else {
		err = rds.currentDb.Put(StorageBucket, compositeKey, vv)
	}
	if err != nil {
		return err
	}

	o := bytes.TrimLeft(original[:], "\x00")
	oo := make([]byte, len(o))
	copy(oo, o)
	val, _ := rds.historyDb.GetS(StorageHistoryBucket, compositeKey, rds.blockNr)
	if !bytes.Equal(val, oo) {
		fmt.Printf("REPAIR (WriteAccountStorage): At block %d, address: %x, key %x, expected %x, found %x\n", rds.blockNr, address, key, oo, val)
		return rds.historyDb.PutS(StorageHistoryBucket, compositeKey, oo, rds.blockNr)
	}
	return nil
}

func (rds *RepairDbState) joinGeneration(gen uint64) {
	rds.nodeCount++
	rds.generationCounts[gen]++

}

func (rds *RepairDbState) leftGeneration(gen uint64) {
	rds.nodeCount--
	rds.generationCounts[gen]--
}

func (rds *RepairDbState) PruneTries() {
	if rds.nodeCount > int(MaxTrieCacheGen) {
		toRemove := 0
		excess := rds.nodeCount - int(MaxTrieCacheGen)
		gen := rds.oldestGeneration
		for excess > 0 {
			excess -= rds.generationCounts[gen]
			toRemove += rds.generationCounts[gen]
			delete(rds.generationCounts, gen)
			gen++
		}
		// Unload all nodes with touch timestamp < gen
		for address, storageTrie := range rds.storageTries {
			empty := storageTrie.UnloadOlderThan(gen)
			if empty {
				delete(rds.storageTries, address)
			}
		}
		rds.oldestGeneration = gen
		rds.nodeCount -= toRemove
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Memory: nodes=%d, alloc=%d, sys=%d\n", rds.nodeCount, int(m.Alloc / 1024), int(m.Sys / 1024))
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
		codeCache: cc,
		codeSizeCache: csc,
	}
	t.MakeListed(tds.joinGeneration, tds.leftGeneration)
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
		t.MakeListed(tds.joinGeneration, tds.leftGeneration)
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
