package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	"math/big"
	"math/rand"

	"github.com/boltdb/bolt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
)

func randKeyHash(r *rand.Rand) common.Hash {
	var b common.Hash
	for i := 0; i < 32; i+=8 {
		binary.BigEndian.PutUint64(b[i:], r.Uint64())
	}
	return b
}

func estimateContractSize(db *bolt.DB, contract common.Address, probes int, probeWidth int) (int, int, error) {
	var fk [52]byte
	copy(fk[:], contract[:])
	actual := 0
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.StorageBucket)
		c := b.Cursor()
		for k, _ := c.Seek(fk[:]); k != nil && bytes.HasPrefix(k, contract[:]); k, _ = c.Next() {
			actual++
		}
		return nil
	}); err != nil {
		return 0, 0, err
	}
	if actual == 0 {
		return 0, 0, nil
	}
	r := rand.New(rand.NewSource(4589489854))
	var seekkey [52]byte
	copy(seekkey[:], contract[:])
	total := big.NewInt(0)
	var large [32]byte
	for i := 0; i < 32; i++ {
		large[i] = 0xff
	}
	largeInt := big.NewInt(0)
	largeInt = largeInt.SetBytes(large[:])
	fmt.Printf("Large int: %d\n", largeInt)
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.StorageBucket)
		for i := 0; i < probes; i++ {
			probeKeyHash := randKeyHash(r)
			fmt.Printf("Random keyhash: %x\n", probeKeyHash)
			c := b.Cursor()
			copy(seekkey[20:], probeKeyHash[:])
			first := big.NewInt(0)
			last := big.NewInt(0)
			k, _ := c.Seek(seekkey[:])
			for j := 0; j < probeWidth+1; j++ {
				if k == nil || !bytes.HasPrefix(k, contract[:]) {
					k, _ := c.Seek(fk[:])
					if k == nil || !bytes.HasPrefix(k, contract[:]) {
						panic("")
					}
				}
				fmt.Printf("probe %d key %d: %x\n", i, j, k[20:])
				if j == 0 {
					first = first.SetBytes(k[20:])
				}
				if j == probeWidth {
					last = last.SetBytes(k[20:])
				}
				k, _ = c.Next()
			}
			diff := big.NewInt(0)
			if first.Cmp(last) < 0 {
				diff = diff.Sub(last, first)
			} else {
				diff = diff.Sub(first, last)
				diff = diff.Sub(largeInt, diff)
			}
			fmt.Printf("Diff: %d\n", diff)
			total = total.Add(total, diff)
		}
		return nil
	}); err != nil {
		return 0, 0, err
	}
	fmt.Printf("Total: %d\n", total)
	estimatedInt := big.NewInt(0).Mul(largeInt, big.NewInt(int64(probes)))
	estimatedInt = estimatedInt.Mul(estimatedInt, big.NewInt(int64(probeWidth)))
	estimatedInt = estimatedInt.Div(estimatedInt, total)
	fmt.Printf("Estimation: %d\n", estimatedInt)
	estimated := 0
	return actual, estimated, nil
}

func estimate() {
	startTime := time.Now()
	db, err := bolt.Open("/Volumes/tb4/turbo-geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	actual, estimated, err := estimateContractSize(db, common.HexToAddress("0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208"), 100, 20)
	check(err)
	fmt.Printf("Size of IDEX_1 is %d, estimated %d\n", actual, estimated)
	fmt.Printf("Estimation took %s\n", time.Since(startTime))
}