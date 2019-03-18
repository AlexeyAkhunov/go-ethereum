package main

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func state_snapshot() {
	startTime := time.Now()
	var blockNum uint64 = uint64(*block)
	//ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	ethDb, err := ethdb.NewLDBDatabase("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	check(err)
	defer ethDb.Close()
	stateDb, db := ethdb.NewMemDatabase2()
	defer stateDb.Close()
	var startKey [32]byte
	tx, err := db.Begin(true)
	check(err)
	b, err := tx.CreateBucket(state.AccountsBucket)
	check(err)
	count := 0
	err = ethDb.WalkAsOf(state.AccountsBucket, state.AccountsHistoryBucket, startKey[:], 0, blockNum+1,
		func(key []byte, value []byte) (bool, error) {
			if len(value) == 0 {
				return true, nil
			}
			if err := b.Put(key, value); err != nil {
				return false, err
			}
			count++
			if count % 1000 == 0 {
				if err := tx.Commit(); err != nil {
					return false, err
				}
				fmt.Printf("Committed %d records\n", count)
				var err error
				tx, err = db.Begin(true)
				if err != nil {
					return false, err
				}
				b = tx.Bucket(state.AccountsBucket)
			}
			return true, nil
		},
	)
	check(err)
	err = tx.Commit()
	check(err)
	tx, err = db.Begin(true)
	check(err)
	b = tx.Bucket(state.AccountsBucket)
	sb, err := tx.CreateBucket(state.StorageBucket)
	check(err)	
	count = 0
	var address common.Address
	//var hash common.Hash
	exist := make(map[common.Address]bool)
	var sk [52]byte
	err = ethDb.WalkAsOf(state.StorageBucket, state.StorageHistoryBucket, sk[:], 0, blockNum,
		func(key []byte, value []byte) (bool, error) {
			if len(value) == 0 {
				return true, nil
			}
			copy(address[:], key[:20])
			if e, ok := exist[address]; ok {
				if !e {
					return true, nil
				}
			} else {
				exist[address] = (b.Get(crypto.Keccak256(address[:])) != nil)
			}
			if err := sb.Put(key, value); err != nil {
				return false, err
			}
			count++
			if count % 1000 == 0 {
				if err := tx.Commit(); err != nil {
					return false, err
				}
				fmt.Printf("Committed %d records\n", count)
				var err error
				tx, err = db.Begin(true)
				if err != nil {
					return false, err
				}
				b = tx.Bucket(state.AccountsBucket)
				sb = tx.Bucket(state.StorageBucket)
			}
			return true, nil
		},
	)
	check(err)
	err = tx.Commit()
	check(err)
	fmt.Printf("Snapshot took %v\n", time.Since(startTime))
	startTime = time.Now()
	bc, err := core.NewBlockChain(ethDb, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil)
	check(err)
	block := bc.GetBlockByNumber(blockNum)
	fmt.Printf("Block number: %d\n", blockNum)
	fmt.Printf("Block root hash: %x\n", block.Root())
	t := trie.New(common.Hash{}, state.AccountsBucket, nil, false)
	r := trie.NewResolver(stateDb, false, true)
	key := []byte{}
	rootHash := block.Root()
	tc := t.NewContinuation(key, 0, rootHash[:])
	r.AddContinuation(tc)
	err = r.ResolveWithDb(stateDb, blockNum)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	fmt.Printf("Trie computation took %v\n", time.Since(startTime))
	startTime = time.Now()
	tx, err = db.Begin(false)
	if err != nil {
		panic(err)
	}
	b = tx.Bucket(state.AccountsBucket)
	for address, e := range exist {
		if e {
			account, err := encodingToAccount(b.Get(crypto.Keccak256(address[:])))
			if err != nil {
				panic(err)
			}
			if account.Root != emptyRoot {
				st := trie.New(common.Hash{}, state.StorageBucket, address[:], true)
				sr := trie.NewResolver(stateDb, false, false)
				key := []byte{}
				stc := st.NewContinuation(key, 0, account.Root[:])
				sr.AddContinuation(stc)
				err = sr.ResolveWithDb(stateDb, blockNum)
				if err != nil {
					fmt.Printf("%x: %v\n", address, err)
				}				
			}
		}
	}
	fmt.Printf("Storage trie computation took %v\n", time.Since(startTime))
}