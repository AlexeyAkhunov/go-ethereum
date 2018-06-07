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
	"runtime"
	"math/big"
	//"runtime/debug"
	//"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	lru "github.com/hashicorp/golang-lru"
)

// Trie cache generation limit after which to evict trie nodes from memory.
var MaxTrieCacheGen = uint32(4*1024*1024)

var AccountsBucket = []byte("AT")
var CodeBucket = []byte("CODE")

const (
	// Number of past tries to keep. This value is chosen such that
	// reasonable chain reorg depths will hit an existing trie.
	maxPastTries = 12

	// Number of codehash->size associations to keep.
	codeSizeCacheSize = 100000
)

type StateReader interface {
	ReadAccountData(address *common.Address) (*Account, error)
	ReadAccountStorage(address *common.Address, key *common.Hash) ([]byte, error)
	ReadAccountCode(codeHash common.Hash) ([]byte, error)
	ReadAccountCodeSize(codeHash common.Hash) (int, error)
}

type StateWriter interface {
	UpdateAccountData(address *common.Address, account *Account) error
	UpdateAccountCode(codeHash common.Hash, code []byte) error
	DeleteAccount(address *common.Address) error
	WriteAccountStorage(address *common.Address, key, value *common.Hash) error
}

// Implements StateReader by wrapping database only, without trie
type DbState struct {
	db ethdb.Mutation
	blockNr uint64
}

func NewDbState(db ethdb.Database, blockNr uint64) *DbState {
	return &DbState{
		db: db.NewBatch(),
		blockNr: blockNr,
	}
}

func (dbs *DbState) SetBlockNr(blockNr uint64) {
	dbs.db.DeleteTimestamp(blockNr)
	dbs.blockNr = blockNr
}

func (dbs *DbState) ForEachStorage(addr common.Address, start []byte, cb func(key, seckey, value common.Hash) bool) {
	var s [32]byte
	copy(s[:], start)
	dbs.db.WalkAsOf(addr[:], s[:], 0, dbs.blockNr, func(ks, vs []byte) (bool, error) {
		if vs == nil || len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		key, err := dbs.db.Get(trie.SecureKeyPrefix, ks)
		if err != nil {
			return false, err
		}
		return cb(common.BytesToHash(key), common.BytesToHash(ks), common.BytesToHash(vs)), nil
	})
}

func (dbs *DbState) ReadAccountData(address *common.Address) (*Account, error) {
	seckey := crypto.Keccak256Hash(address[:])
	enc, err := dbs.db.GetAsOf(AccountsBucket, seckey[:], dbs.blockNr)
	if err != nil || enc == nil || len(enc) == 0 {
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

func (dbs *DbState) ReadAccountStorage(address *common.Address, key *common.Hash) ([]byte, error) {
	seckey := crypto.Keccak256Hash(key[:])
	enc, err := dbs.db.GetAsOf(address[:], seckey[:], dbs.blockNr)
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

func (dbs *DbState) UpdateAccountData(address *common.Address, account *Account) error {
	var data []byte
	var err error
	if bytes.Equal(account.CodeHash, emptyCodeHash) && (account.Root == emptyRoot || common.EmptyHash(account.Root)) {
		if account.Balance.Sign() == 0 && account.Nonce == 0 {
			data = []byte{byte(192)}
		} else {
			var extAccount ExtAccount
			extAccount.Nonce = account.Nonce
			extAccount.Balance = account.Balance
			data, err = rlp.EncodeToBytes(extAccount)
			if err != nil {
				return err
			}
		}
	} else {
		data, err = rlp.EncodeToBytes(account)
		if err != nil {
			return err
		}
	}
	seckey := crypto.Keccak256Hash(address[:])
	return dbs.db.PutS(AccountsBucket, seckey[:], data, dbs.blockNr)
}

func (dbs *DbState) DeleteAccount(address *common.Address) error {
	seckey := crypto.Keccak256Hash(address[:])
	return dbs.db.PutS(AccountsBucket, seckey[:], []byte{}, dbs.blockNr)
}

func (dbs *DbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return dbs.db.Put(CodeBucket, codeHash[:], code)
}

func (dbs *DbState) WriteAccountStorage(address *common.Address, key, value *common.Hash) error {
	seckey := crypto.Keccak256Hash(key[:])
	v := bytes.TrimLeft(value[:], "\x00")
	return dbs.db.PutS(address[:], seckey[:], v, dbs.blockNr)
}

// Implements StateReader by wrapping a trie and a database, where trie acts as a cache for the database
type TrieDbState struct {
	t                *trie.Trie
	addrHashCache    *lru.Cache
	keyHashCache     *lru.Cache
	db               ethdb.Database
	nodeList         *trie.List
	blockNr          uint64
	storageTries     map[common.Address]*trie.Trie
	storageUpdates   map[common.Address]map[common.Hash][]byte
	accountUpdates   map[common.Address]*Account
	deleted          map[common.Address]struct{}
	codeCache        *lru.Cache
	codeSizeCache    *lru.Cache
}

func NewTrieDbState(root common.Hash, db ethdb.Database, blockNr uint64) (*TrieDbState, error) {
	addrHashCache, err := lru.New(128*1024)
	if err != nil {
		return nil, err
	}
	keyHashCache, err := lru.New(128*1024)
	if err != nil {
		return nil, err
	}
	csc, err := lru.New(100000)
	if err != nil {
		return nil, err
	}
	cc, err := lru.New(10000)
	if err != nil {
		return nil, err
	}
	t := trie.New(root, AccountsBucket, false)
	tds := TrieDbState{
		t: t,
		addrHashCache: addrHashCache,
		keyHashCache: keyHashCache,
		db: db,
		nodeList: trie.NewList(),
		blockNr: blockNr,
		storageTries: make(map[common.Address]*trie.Trie),
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Address]*Account),
		deleted: make(map[common.Address]struct{}),
		codeCache: cc,
		codeSizeCache: csc,
	}
	t.MakeListed(tds.nodeList)
	return &tds, nil
}

func (tds *TrieDbState) Copy() *TrieDbState {
	addrHashCache, err := lru.New(128*1024)
	if err != nil {
		panic(err)
	}
	keyHashCache, err := lru.New(128*1024)
	if err != nil {
		panic(err)
	}
	tcopy := *tds.t
	cpy := TrieDbState{
		t: &tcopy,
		addrHashCache: addrHashCache,
		keyHashCache: keyHashCache,
		db: tds.db,
		nodeList: nil,
		blockNr: tds.blockNr,
		storageTries: make(map[common.Address]*trie.Trie),
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Address]*Account),
		deleted: make(map[common.Address]struct{}),
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
	if len(tds.storageUpdates) == 0 && len(tds.accountUpdates) == 0 {
		return tds.t.Hash(), nil
	}
	//for address, account := range tds.accountUpdates {
	//	fmt.Printf("%x %d %x %x\n", address[:], account.Balance, account.CodeHash, account.Root[:])
	//}
	//fmt.Printf("=================\n")
	oldStorageC := [][]*trie.TrieContinuation{}
	newStorageC := [][]*trie.TrieContinuation{}
	relist := []*trie.Trie{}
	for address, m := range tds.storageUpdates {
		if _, ok := tds.deleted[address]; ok {
			continue
		}
		continuations := []*trie.TrieContinuation{}
		storageTrie, err := tds.getStorageTrie(&address, true)
		for key, v := range m {
			if err != nil {
				return common.Hash{}, err
			}
			seckey, err := tds.HashKey(key)
			if err != nil {
				return common.Hash{}, err
			}
			var c *trie.TrieContinuation
			if len(v) > 0 {
				c = storageTrie.UpdateAction(seckey, v)
			} else {
				c = storageTrie.DeleteAction(seckey)
			}
			continuations = append(continuations, c)
		}
		if len(continuations) > 0 {
			oldStorageC = append(oldStorageC, continuations)
		}
		relist = append(relist, storageTrie)
	}
	it := 0
	for len(oldStorageC) > 0 {
		for _, oldContinuations := range oldStorageC {
			newContinuations := []*trie.TrieContinuation{}
			var resolver *trie.TrieResolver
			for _, c := range oldContinuations {
				if !c.RunWithDb(tds.db) {
					newContinuations = append(newContinuations, c)
					if resolver == nil {
						resolver = c.Trie().NewResolver(tds.db, false)
					}
					resolver.AddContinuation(c)
				}
			}
			if len(newContinuations) > 0 {
				newStorageC = append(newStorageC, newContinuations)
				if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
					return common.Hash{}, err
				}
			}
		}
		oldStorageC, newStorageC = newStorageC, [][]*trie.TrieContinuation{}
		it++
	}
	if it > 3 {
		fmt.Printf("Resolved storage in %d iterations\n", it)
	}
	for _, storageTrie := range relist {
		storageTrie.Relist()
	}
	oldContinuations := []*trie.TrieContinuation{}
	newContinuations := []*trie.TrieContinuation{}
	tds.storageUpdates = make(map[common.Address]map[common.Hash][]byte)
	for address, account := range tds.accountUpdates {
		addrHash, err := tds.HashAddress(address)
		if err != nil {
			return common.Hash{}, err
		}
		var c *trie.TrieContinuation
		storageTrie, err := tds.getStorageTrie(&address, false)
		if err != nil {
			return common.Hash{}, err
		}
		deleteStorageTrie := false
		if account != nil {
			if _, ok := tds.deleted[address]; ok {
				deleteStorageTrie = true
				account.Root = emptyRoot
			} else if storageTrie != nil {
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
			storageTrie.Unlink()
			delete(tds.storageTries, address)
		}
		oldContinuations = append(oldContinuations, c)
	}
	tds.accountUpdates = make(map[common.Address]*Account)
	tds.deleted = make(map[common.Address]struct{})
	it = 0
	for len(oldContinuations) > 0 {
		var resolver *trie.TrieResolver
		for _, c := range oldContinuations {
			if !c.RunWithDb(tds.db) {
				newContinuations = append(newContinuations, c)
				if resolver == nil {
					resolver = tds.t.NewResolver(tds.db, false)
				}
				resolver.AddContinuation(c)
			}
		}
		if len(newContinuations) > 0 {
			if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
				return common.Hash{}, err
			}
		}
		oldContinuations, newContinuations = newContinuations, []*trie.TrieContinuation{}
		it++
	}
	if it > 3 {
		fmt.Printf("Resolved in %d iterations\n", it)
	}
	hash := tds.t.Hash()
	tds.t.SaveHashes(tds.db)
	tds.t.Relist()
	return hash, nil
}


func (tds *TrieDbState) Rebuild() error {
	tr := tds.AccountTrie()
	tr.Rebuild(tds.db, tds.blockNr)
	return nil
}

func (tds *TrieDbState) SetBlockNr(blockNr uint64) {
	tds.blockNr = blockNr
}

func (tds *TrieDbState) UnwindTo(blockNr uint64) error {
	if err := tds.db.RewindData(tds.blockNr, blockNr, func (bucket, key, value []byte) error {
		//var c *trie.TrieContinuation
		//var a *common.Address
		if bytes.Equal(bucket, AccountsBucket) {
			if len(value) > 0 {
				//c = t.UpdateAction(a, key, value)
			} else {
				//c = t.DeleteAction(a, key)
			}
		} else {
			if len(value) > 0 {
				//c = t.UpdateAction(a, key, value)
			} else {
				//c = t.DeleteAction(a, key)
			}
		}
		//tds.continuations = append(tds.continuations, c)
		return nil
	}); err != nil {
		return err
	}
	if _, err := tds.TrieRoot(); err != nil {
		return err
	}
	tds.blockNr = blockNr
	return nil
}

func (tds *TrieDbState) ReadAccountData(address *common.Address) (*Account, error) {
	addrHash, err := tds.HashAddress(*address)
	if err != nil {
		return nil, err
	}
	enc, gotValue, err := tds.t.TryGet(tds.db, addrHash, tds.blockNr)
	if err != nil {
		return nil, err
	}
	if !gotValue {
		//fmt.Printf("DBREAD %x\n", address[:])
		//fmt.Printf("%s\n", debug.Stack())
	}
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

func (tds *TrieDbState) HashAddress(address common.Address) ([]byte, error) {
	if cached, ok := tds.addrHashCache.Get(address); ok {
		return cached.([]byte), nil
	}
	hash := crypto.Keccak256Hash(address[:])
	tds.addrHashCache.Add(address, hash[:])
	if err := tds.db.Put(trie.SecureKeyPrefix, hash[:], address[:]); err != nil {
		return nil, err
	}
	return hash[:], nil
}

func (tds *TrieDbState) HashKey(key common.Hash) ([]byte, error) {
	if cached, ok := tds.keyHashCache.Get(key); ok {
		return cached.([]byte), nil
	}
	hash := crypto.Keccak256Hash(key[:])
	tds.keyHashCache.Add(key, hash[:])
	if err := tds.db.Put(trie.SecureKeyPrefix, hash[:], key[:]); err != nil {
		return nil, err
	}
	return hash[:], nil
}

func (tds *TrieDbState) GetKey(shaKey []byte) []byte {
	key, _ := tds.db.Get(trie.SecureKeyPrefix, shaKey)
	return key
}

func (tds *TrieDbState) getStorageTrie(address *common.Address, create bool) (*trie.Trie, error) {
	t, ok := tds.storageTries[*address]
	if !ok && create {
		account, err := tds.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if account == nil {
			t = trie.New(common.Hash{}, common.CopyBytes(address[:]), true)
		} else {
			t = trie.New(account.Root, common.CopyBytes(address[:]), true)
		}
		t.MakeListed(tds.nodeList)
		tds.storageTries[*address] = t
	}
	return t, nil
}

func (tds *TrieDbState) ReadAccountStorage(address *common.Address, key *common.Hash) ([]byte, error) {
	t, err := tds.getStorageTrie(address, true)
	if err != nil {
		return nil, err
	}
	seckey, err := tds.HashKey(*key)
	if err != nil {
		return nil, err
	}
	enc, _, err := t.TryGet(tds.db, seckey, tds.blockNr)
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
	listLen := tds.nodeList.Len()
	if listLen > int(MaxTrieCacheGen) {
		tds.nodeList.ShrinkTo(int(MaxTrieCacheGen))
		nodeCount := 0
		for address, storageTrie := range tds.storageTries {
			count, empty := storageTrie.TryPrune()
			nodeCount += count
			if empty {
				delete(tds.storageTries, address)
			}
		}
		count, _ := tds.t.TryPrune()
		nodeCount += count
		log.Info("Nodes", "trie", nodeCount, "list", tds.nodeList.Len(), "list before pruning", listLen)
	} else {
		log.Info("Nodes", "list", tds.nodeList.Len())
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	churn := (m.TotalAlloc - prevMemStats.TotalAlloc) - (prevMemStats.Alloc - m.Alloc)
	log.Info("Memory", "alloc", int(m.Alloc / 1024), "churn", int(churn / 1024), "sys", int(m.Sys / 1024), "numGC", int(m.NumGC))
	prevMemStats = m
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

func (tsw *TrieStateWriter) UpdateAccountData(address *common.Address, account *Account) error {
	tsw.tds.accountUpdates[*address] = account
	return nil
}

func (dsw *DbStateWriter) UpdateAccountData(address *common.Address, account *Account) error {
	var data []byte
	var err error
	if bytes.Equal(account.CodeHash, emptyCodeHash) && (account.Root == emptyRoot || common.EmptyHash(account.Root)) {
		if account.Balance.Sign() == 0 && account.Nonce == 0 {
			data = []byte{byte(192)}
		} else {
			var extAccount ExtAccount
			extAccount.Nonce = account.Nonce
			extAccount.Balance = account.Balance
			data, err = rlp.EncodeToBytes(extAccount)
			if err != nil {
				return err
			}
		}
	} else {
		data, err = rlp.EncodeToBytes(account)
		if err != nil {
			return err
		}
	}
	seckey, err := dsw.tds.HashAddress(*address)
	if err != nil {
		return err
	}
	return dsw.tds.db.PutS(AccountsBucket, seckey, data, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) DeleteAccount(address *common.Address) error {
	tsw.tds.accountUpdates[*address] = nil
	tsw.tds.deleted[*address] = struct{}{}
	return nil
}

func (dsw *DbStateWriter) DeleteAccount(address *common.Address) error {
	seckey, err := dsw.tds.HashAddress(*address)
	if err != nil {
		return err
	}
	return dsw.tds.db.PutS(AccountsBucket, seckey, []byte{}, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (dsw *DbStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return dsw.tds.db.Put(CodeBucket, codeHash[:], code)
}

func (tsw *TrieStateWriter) WriteAccountStorage(address *common.Address, key, value *common.Hash) error {
	v := bytes.TrimLeft(value[:], "\x00")
	m, ok := tsw.tds.storageUpdates[*address]
	if !ok {
		m = make(map[common.Hash][]byte)
		tsw.tds.storageUpdates[*address] = m
	}
	if len(v) > 0 {
		m[*key] = common.CopyBytes(v)
	} else {
		m[*key] = nil
	}
	return nil
}

func (dsw *DbStateWriter) WriteAccountStorage(address *common.Address, key, value *common.Hash) error {
	seckey, err := dsw.tds.HashKey(*key)
	if err != nil {
		return err
	}
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	return dsw.tds.db.PutS(address[:], seckey, vv, dsw.tds.blockNr)
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
	PrintTrie()
	MakeListed(*trie.List)
	Unlink()
}

// NewDatabase creates a backing store for state. The returned database is safe for
// concurrent use and retains cached trie nodes in memory. The pool is an optional
// intermediate trie-node memory pool between the low level storage layer and the
// high level trie abstraction.
func NewDatabase(db ethdb.Database) Database {
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
	return trie.NewSecure(root, AccountsBucket, false)
}

// OpenStorageTrie opens the storage trie of an account.
func (db *cachingDB) OpenStorageTrie(addrHash, root common.Hash) (Trie, error) {
	return trie.NewSecure(root, addrHash[:], true)
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
