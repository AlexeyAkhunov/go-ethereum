package main

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"

	"github.com/ethereum/go-ethereum/common"
)

func estimateContractSize(db *bolt.DB, contract common.Address, probes int, probeWidth int) (int, error) {
	return 0, nil
}

func estimate() {
	startTime := time.Now()
	db, err := bolt.Open("/Volumes/tb41/turbo-geth-10/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	size, err := estimateContractSize(db, common.HexToAddress("0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208"), 1, 2)
	fmt.Printf("Size of IDEX_1 is %d\n", size)
	fmt.Printf("Estimation took %s\n", time.Since(startTime))
}