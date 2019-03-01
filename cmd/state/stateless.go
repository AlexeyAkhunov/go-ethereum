package main

import (
	"math/big"
	"fmt"
	"os"
	"bufio"
	"time"
	"syscall"
	"os/signal"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
)

type StatelessTracer struct {
	accountsReadSet map[common.Address]struct{}
	accountsWriteSet map[common.Address]struct{}
	accountsWriteSetFrame map[common.Address]struct{}
	storageReadSet map[common.Address]map[common.Hash]struct{}
	storageWriteSet map[common.Address]map[common.Hash]struct{}
	storageWriteSetFrame map[common.Address]map[common.Hash]struct{}
}

func NewStatelessTracer() *StatelessTracer {
	return &StatelessTracer{
		accountsReadSet: make(map[common.Address]struct{}),
		accountsWriteSet: make(map[common.Address]struct{}),
		accountsWriteSetFrame: make(map[common.Address]struct{}),
		storageReadSet: make(map[common.Address]map[common.Hash]struct{}),
		storageWriteSet: make(map[common.Address]map[common.Hash]struct{}),
		storageWriteSetFrame: make(map[common.Address]map[common.Hash]struct{}),
	}
}

func (slt *StatelessTracer) ResetCounters() {
}

func (slt *StatelessTracer) ResetSets() {
	slt.accountsReadSet = make(map[common.Address]struct{})
	slt.accountsWriteSet = make(map[common.Address]struct{})
	slt.accountsWriteSetFrame = make(map[common.Address]struct{})
	slt.storageReadSet = make(map[common.Address]map[common.Hash]struct{})
	slt.storageWriteSet = make(map[common.Address]map[common.Hash]struct{})
	slt.storageWriteSetFrame = make(map[common.Address]map[common.Hash]struct{})
}

func (slt *StatelessTracer) CaptureStart(depth int, from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	return nil
}
func (slt *StatelessTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	if op == vm.SSTORE {
		addr := contract.Address()
		if stack.Len() == 0 {
			return nil
		}
		loc := common.BigToHash(stack.Back(0))
		if smap, ok := slt.storageWriteSetFrame[addr]; ok {
			smap[loc] = struct{}{}
		} else {
			smap = make(map[common.Hash]struct{})
			smap[loc] = struct{}{}
			slt.storageWriteSetFrame[addr] = smap
		}
	} else if op == vm.SLOAD {
		addr := contract.Address()
		if stack.Len() == 0 {
			return nil
		}
		loc := common.BigToHash(stack.Back(0))
		if smap, ok := slt.storageReadSet[addr]; ok {
			smap[loc] = struct{}{}
		} else {
			smap = make(map[common.Hash]struct{})
			smap[loc] = struct{}{}
			slt.storageReadSet[addr] = smap
		}
	}
	return nil
}
func (slt *StatelessTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *vm.Stack, contract *vm.Contract, depth int, err error) error {
	slt.accountsWriteSetFrame = make(map[common.Address]struct{})
	slt.storageWriteSetFrame = make(map[common.Address]map[common.Hash]struct{})
	return nil
}
func (slt *StatelessTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	if err == nil {
		// Merge frame writes with the tx writes
		for addr, _ := range slt.accountsWriteSetFrame {
			slt.accountsWriteSet[addr] = struct{}{}
		}
		for addr, smap := range slt.storageWriteSetFrame {
			if smap_dest, ok := slt.storageWriteSet[addr]; ok {
				for loc, _ := range smap {
					smap_dest[loc] = struct{}{}
				}
			} else {
				slt.storageWriteSet[addr] = smap
			}
		}
	}
	slt.accountsWriteSetFrame = make(map[common.Address]struct{})
	slt.storageWriteSetFrame = make(map[common.Address]map[common.Hash]struct{})
	return nil
}
func (slt *StatelessTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	return nil
}
func (slt *StatelessTracer) CaptureAccountRead(account common.Address) error {
	slt.accountsReadSet[account] = struct{}{}
	return nil
}
func (slt *StatelessTracer) CaptureAccountWrite(account common.Address) error {
	slt.accountsWriteSetFrame[account] = struct{}{}
	return nil
}

type ProofSizer struct {
	ethDb ethdb.Database
}

func NewProofSizer(ethDb ethdb.Database) *ProofSizer {
	return &ProofSizer{
		ethDb: ethDb,
	}
}

func (ps *ProofSizer) addAccount(account common.Address) {
}

func (ps *ProofSizer) addStorage(account common.Address, loc common.Hash) {
}

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

	ethDb, err := ethdb.NewLDBDatabase("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	check(err)
	defer ethDb.Close()
	chainConfig := params.MainnetChainConfig
	slFile, err := os.OpenFile("/Volumes/tb4/turbo-geth/stateless.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer slFile.Close()
	w := bufio.NewWriter(slFile)
	defer w.Flush()
	slt := NewStatelessTracer()
	vmConfig := vm.Config{Tracer: slt, Debug: false}
	bcb, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFullFaker(), vm.Config{}, nil)
	check(err)
	stateDb := ethdb.NewMemDatabase()
	defer stateDb.Close()
	_, _, _, err = core.SetupGenesisBlock(stateDb, core.DefaultGenesisBlock())
	check(err)
	bc, err := core.NewBlockChain(stateDb, nil, chainConfig, ethash.NewFullFaker(), vmConfig, nil)
	check(err)
	bc.SetNoHistory(true)
	bc.SetResolveReads(true)
	blockNum := uint64(1)
	interrupt := false
	for !interrupt {
		block := bcb.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		_, err = bc.InsertChain(types.Blocks{block})
		if err != nil {
			fmt.Printf("Failed on block %d\n", blockNum)
		}
		check(err)
		masks, shortLens, hashes := bc.GetTrieDbState().ExtractProofs()
		dbstate := state.NewStateless(block.Root(), masks, shortLens, hashes, block.NumberU64()-1)

		statedb := state.New(dbstate)
		//statedb.SetTracer(slt)
		signer := types.MakeSigner(chainConfig, block.Number())
		slt.ResetCounters()
		slt.ResetSets()
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if _, _, _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				//panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}
		/*
		ps := &ProofSizer{ethDb: bc.GetTrieDbState().Database()}
		for account := range slt.accountsReadSet {
			ps.addAccount(account)
		}
		for account := range slt.accountsWriteSet {
			ps.addAccount(account)
		}
		for account, smap := range slt.storageReadSet {
			for loc := range smap {
				ps.addStorage(account, loc)
			}
		}
		for account, smap := range slt.storageWriteSet {
			for loc := range smap {
				ps.addStorage(account, loc)
			}
		}

		fmt.Fprintf(w, "%d,%d,%d\n", blockNum, len(slt.accountsWriteSet), len(slt.storageWriteSet))
		*/
		blockNum++
		if blockNum == 10 {
			break
		}
		if blockNum % 1000 == 0 {
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