package main

import (
	"fmt"
	"os"
	"bufio"
	"time"
	"syscall"
	"os/signal"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
)

func stateless() {
	//state.MaxTrieCacheGen = 100000
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	//ethDb, err := ethdb.NewLDBDatabase("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	//ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	ethDb, err := ethdb.NewLDBDatabase("/home/akhounov/.ethereum/geth/chaindata")
	check(err)
	defer ethDb.Close()
	chainConfig := params.MainnetChainConfig
	//slFile, err := os.OpenFile("/Volumes/tb4/turbo-geth/stateless.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	slFile, err := os.OpenFile("stateless.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer slFile.Close()
	w := bufio.NewWriter(slFile)
	defer w.Flush()
	vmConfig := vm.Config{}
	engine := ethash.NewFullFaker()
	bcb, err := core.NewBlockChain(ethDb, nil, chainConfig, engine, vm.Config{}, nil)
	check(err)
	stateDb := ethdb.NewMemDatabase()
	defer stateDb.Close()
	_, _, _, err = core.SetupGenesisBlock(stateDb, core.DefaultGenesisBlock())
	check(err)
	bc, err := core.NewBlockChain(stateDb, nil, chainConfig, engine, vmConfig, nil)
	check(err)
	var preRoot common.Hash
	genesisBlock, _, _, err := core.DefaultGenesisBlock().ToBlock(nil)
	check(err)
	preRoot = genesisBlock.Header().Root
	//check(err)
	bc.SetNoHistory(true)
	bc.SetResolveReads(true)
	blockNum := uint64(1)
	interrupt := false
	for !interrupt {
		block := bcb.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		trace := false//blockNum == 843551
		if trace {
			filename := fmt.Sprintf("right_%d.txt", blockNum-1)
			f, err1 := os.Create(filename)
			if err1 == nil {
				defer f.Close()
				bc.GetTrieDbState().PrintTrie(f)
				//bc.GetTrieDbState().PrintStorageTrie(f, common.BytesToHash(common.FromHex("0x1570aaebc55e1a8067cc4a5c3ba451d196bf91544f183168b51bb1d306bda995")))
			}
		}
		_, err = bc.InsertChain(types.Blocks{block})
		if err != nil {
			panic(fmt.Sprintf("Failed on block %d, error: %v\n", blockNum, err))
		}
		check(err)
		contracts, cMasks, cHashes, cShortKeys, cValues, codes, masks, hashes, shortKeys, values := bc.GetTrieDbState().ExtractProofs(trace)
		dbstate, err := state.NewStateless(preRoot,
			contracts, cMasks, cHashes, cShortKeys, cValues,
			codes,
			masks, hashes, shortKeys, values,
			block.NumberU64()-1, trace,
		)
		if err != nil {
			fmt.Printf("Error making state for block %d: %v\n", err)
		}
		statedb := state.New(dbstate)
		gp := new(core.GasPool).AddGas(block.GasLimit())
		header := block.Header()
		usedGas := new(uint64)
		var receipts types.Receipts
		if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
			misc.ApplyDAOHardFork(statedb)
		}
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			//msg, _ := tx.AsMessage(signer)
			//context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			//vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			receipt, _, err := core.ApplyTransaction(chainConfig, bc, nil, gp, statedb, dbstate, header, tx, usedGas, vmConfig)
			if err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
			receipts = append(receipts, receipt)
			//if _, _, _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
			//	panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			//}
		}
		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		_, err = engine.Finalize(bcb, header, statedb, block.Transactions(), block.Uncles(), receipts)
		if err != nil {
			panic(fmt.Errorf("Finalize of block %d failed: %v", blockNum, err))
		}
		statedb.Commit(chainConfig.IsEIP158(header.Number), dbstate)
		err = dbstate.CheckRoot(header.Root)
		if err != nil {
			filename := fmt.Sprintf("right_%d.txt", blockNum)
			f, err1 := os.Create(filename)
			if err1 == nil {
				defer f.Close()
				bc.GetTrieDbState().PrintTrie(f)
			}
			fmt.Printf("Error processing block %d: %v\n", blockNum, err)
		}
		preRoot = header.Root
		var totalCShorts, totalCValues, totalCodes, totalShorts, totalValues int
		for _, short := range cShortKeys {
			totalCShorts += len(short)
		}
		for _, value := range cValues {
			totalCValues += len(value)
		}
		for _, code := range codes {
			totalCodes += len(code)
		}
		for _, short := range shortKeys {
			totalShorts += len(short)
		}
		for _, value := range values {
			totalValues += len(value)
		}
		fmt.Fprintf(w, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
			blockNum, len(contracts), len(cMasks), len(cHashes), len(cShortKeys), len(cValues), len(codes),
			len(masks), len(hashes), len(shortKeys), len(values), totalCShorts, totalValues, totalCodes, totalShorts, totalValues,
		)
		blockNum++
		if blockNum == 300000 {
			//break
		}
		if /*blockNum > 843000 || */(blockNum % 1000 == 0) {
			fmt.Printf("Processed %d blocks\n", blockNum)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Processed %d blocks\n", blockNum)
	fmt.Printf("Next time specify -block %d\n", blockNum)
	fmt.Printf("Stateless client analysis took %s\n", time.Since(startTime))

}