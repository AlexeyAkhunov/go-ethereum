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

func stateless() {
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
	vmConfig := vm.Config{Tracer: slt, Debug: true}
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil)
	check(err)
	blockNum := uint64(*block)
	interrupt := false
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewDbState(ethDb, block.NumberU64()-1)

		// First pass - execute transactions in sequence
		statedb := state.New(dbstate)
		statedb.SetTracer(slt)
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
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}	

		fmt.Fprintf(w, "%d,%d,%d\n", blockNum, len(slt.accountsWriteSet), len(slt.storageWriteSet))
		blockNum++
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