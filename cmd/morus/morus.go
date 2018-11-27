package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/signal"
	"time"
	"syscall"

	"github.com/ethereum/go-ethereum/avl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

var (
	cpuprofile = flag.String("cpu-profile", "", "write cpu profile `file`")
	blockchain = flag.String("blockchain", "data/blockchain", "file containing blocks to load")
	hashlen    = flag.Int("hashlen", 32, "size of the hashes for inter-page references")
	pagefile   = flag.String("pagefile", "pages", "name of the page file")
	valuefile  = flag.String("valuefile", "values", "name of the value file")
	codefile   = flag.String("codefile", "codes", "name of the code file")
	verfile    = flag.String("verfile", "versions", "name of the versions file")
	load       = flag.Bool("load", false, "load blocks into pages")
	spacescan  = flag.Bool("spacescan", false, "perform space scan")
)

// ChainContext implements Ethereum's core.ChainContext and consensus.Engine
// interfaces. It is needed in order to apply and process Ethereum
// transactions. There should only be a single implementation in Ethermint. For
// the purposes of Ethermint, it should be support retrieving headers and
// consensus parameters from  the current blockchain to be used during
// transaction processing.
//
// NOTE: Ethermint will distribute the fees out to validators, so the structure
// and functionality of this is a WIP and subject to change.
type ChainContext struct {
	Coinbase        common.Address
	headersByNumber map[uint64]*types.Header
}

func NewChainContext() *ChainContext {
	return &ChainContext{
		headersByNumber: make(map[uint64]*types.Header),
	}
}

// Engine implements Ethereum's core.ChainContext interface. As a ChainContext
// implements the consensus.Engine interface, it is simply returned.
func (cc *ChainContext) Engine() consensus.Engine {
	return cc
}

// SetHeader implements Ethereum's core.ChainContext interface. It sets the
// header for the given block number.
func (cc *ChainContext) SetHeader(number uint64, header *types.Header) {
	cc.headersByNumber[number] = header
}

// GetHeader implements Ethereum's core.ChainContext interface.
//
// TODO: The Cosmos SDK supports retreiving such information in contexts and
// multi-store, so this will be need to be integrated.
func (cc *ChainContext) GetHeader(_ common.Hash, number uint64) *types.Header {
	if header, ok := cc.headersByNumber[number]; ok {
		return header
	}

	return nil
}

// Author implements Ethereum's consensus.Engine interface. It is responsible
// for returned the address of the validtor to receive any fees. This function
// is only invoked if the given author in the ApplyTransaction call is nil.
//
// NOTE: Ethermint will distribute the fees out to validators, so the structure
// and functionality of this is a WIP and subject to change.
func (cc *ChainContext) Author(_ *types.Header) (common.Address, error) {
	return cc.Coinbase, nil
}

// APIs implements Ethereum's consensus.Engine interface. It currently performs
// a no-op.
//
// TODO: Do we need to support such RPC APIs? This will tie into a bigger
// discussion on if we want to support web3.
func (cc *ChainContext) APIs(_ consensus.ChainReader) []rpc.API {
	return nil
}

// CalcDifficulty implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
func (cc *ChainContext) CalcDifficulty(_ consensus.ChainReader, _ uint64, _ *types.Header) *big.Int {
	return nil
}

// Finalize implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the ABCI?
func (cc *ChainContext) Finalize(
	_ consensus.ChainReader, _ *types.Header, _ *state.StateDB,
	_ []*types.Transaction, _ []*types.Header, _ []*types.Receipt,
) (*types.Block, error) {
	return nil, nil
}

// Prepare implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the ABCI?
func (cc *ChainContext) Prepare(_ consensus.ChainReader, _ *types.Header) error {
	return nil
}

// Seal implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the ABCI?
func (cc *ChainContext) Seal(_ consensus.ChainReader, _ *types.Block, _ chan<- *types.Block, _ <-chan struct{}) error {
	return nil
}

// SealHash implements Ethereum's consensus.Engine interface. It returns the
// hash of a block prior to it being sealed.
func (cc *ChainContext) SealHash(header *types.Header) common.Hash {
	return common.Hash{}
}

// VerifyHeader implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the Cosmos SDK
// handlers?
func (cc *ChainContext) VerifyHeader(_ consensus.ChainReader, _ *types.Header, _ bool) error {
	return nil
}

// VerifyHeaders implements Ethereum's consensus.Engine interface. It
// currently performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the Cosmos SDK
// handlers?
func (cc *ChainContext) VerifyHeaders(_ consensus.ChainReader, _ []*types.Header, _ []bool) (chan<- struct{}, <-chan error) {
	return nil, nil
}

// VerifySeal implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
//
// TODO: Figure out if this needs to be hooked up to any part of the Cosmos SDK
// handlers?
func (cc *ChainContext) VerifySeal(_ consensus.ChainReader, _ *types.Header) error {
	return nil
}

// VerifyUncles implements Ethereum's consensus.Engine interface. It currently
// performs a no-op.
func (cc *ChainContext) VerifyUncles(_ consensus.ChainReader, _ *types.Block) error {
	return nil
}

// Close implements Ethereum's consensus.Engine interface. It terminates any
// background threads maintained by the consensus engine. It currently performs
// a no-op.
func (cc *ChainContext) Close() error {
	return nil
}

type MorusDb struct {
	db *avl.Avl1
}

func NewMorusDb(pagefile, valuefile, verfile string, hashlen int) *MorusDb {
	db := avl.NewAvl1()
	db.SetHashLength(uint32(hashlen))
	db.UseFiles(pagefile, valuefile, verfile, false)
	return &MorusDb{db: db}
}

func (md *MorusDb) LatestVersion() int64 {
	return int64(md.db.CurrentVersion())
}

func (md *MorusDb) Commit() uint64 {
	return md.db.Commit()
}

func (md *MorusDb) PrintStats() {
	md.db.PrintStats()
}

func (md *MorusDb) ReadAccountData(address common.Address) (*state.Account, error) {
	return nil, nil
}

func (md *MorusDb) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	return nil, nil
}

func (md *MorusDb) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	return nil, nil
}

func (md *MorusDb) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	return 0, nil
}

func (md *MorusDb) UpdateAccountData(address common.Address, original, account *state.Account) error {
	return nil
}

func (md *MorusDb) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (md *MorusDb) DeleteAccount(address common.Address, original *state.Account) error {
	return nil
}

func (md *MorusDb) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	return nil
}

// Some weird constants to avoid constant memory allocs for them.
var (
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)
)

// accumulateRewards credits the coinbase of the given block with the mining
// reward. The total reward consists of the static block reward and rewards for
// included uncles. The coinbase of each uncle block is also rewarded.
func accumulateRewards(config *params.ChainConfig, state *state.StateDB, header *types.Header, uncles []*types.Header) {
	// select the correct block reward based on chain progression
	blockReward := ethash.FrontierBlockReward
	if config.IsByzantium(header.Number) {
		blockReward = ethash.ByzantiumBlockReward
	}

	// accumulate the rewards for the miner and any included uncles
	reward := new(big.Int).Set(blockReward)
	r := new(big.Int)

	for _, uncle := range uncles {
		r.Add(uncle.Number, big8)
		r.Sub(r, header.Number)
		r.Mul(r, blockReward)
		r.Div(r, big8)
		state.AddBalance(uncle.Coinbase, r)
		r.Div(blockReward, big32)
		reward.Add(reward, r)
	}

	state.AddBalance(header.Coinbase, reward)
}

func main() {
	flag.Parse()
	morus := NewMorusDb(*pagefile, *valuefile, *verfile, *hashlen)
	if morus.LatestVersion() == 0 {
		statedb := state.New(morus)
		genBlock := core.DefaultGenesisBlock()
		for addr, account := range genBlock.Alloc {
			statedb.AddBalance(addr, account.Balance)
			statedb.SetCode(addr, account.Code)
			statedb.SetNonce(addr, account.Nonce)

			for key, value := range account.Storage {
				statedb.SetState(addr, key, value)
			}
		}
		if err := statedb.Commit(false, morus); err != nil {
			panic(err)
		}
		cp := morus.Commit()

		fmt.Printf("Committed pages for genesis state: %d\n", cp)
	}
	// file with blockchain data exported from geth by using "geth exportdb"
	// command.
	input, err := os.Open(*blockchain)
	if err != nil {
		panic(err)
	}
	defer input.Close()

	// ethereum mainnet config
	chainConfig := params.MainnetChainConfig

	// create RLP stream for exported blocks
	stream := rlp.NewStream(input, 0)

	var block types.Block

	var prevRoot common.Hash
	binary.BigEndian.PutUint64(prevRoot[:8], uint64(morus.LatestVersion()))

	chainContext := NewChainContext()
	vmConfig := vm.Config{}

	startTime := time.Now()
	interrupt := false

	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	var lastSkipped uint64
	var cpRun uint64
	for !interrupt {
		if err = stream.Decode(&block); err == io.EOF {
			err = nil
			break
		} else if err != nil {
			panic(fmt.Errorf("failed to decode at block %d: %s", block.NumberU64(), err))
		}

		// don't import blocks already imported
		if block.NumberU64() < uint64(morus.LatestVersion()) {
			lastSkipped = block.NumberU64()
			continue
		}

		if lastSkipped > 0 {
			fmt.Printf("skipped blocks up to %d\n", lastSkipped)
			lastSkipped = 0
		}

		header := block.Header()
		chainContext.Coinbase = header.Coinbase
		chainContext.SetHeader(block.NumberU64(), header)

		statedb := state.New(morus)

		var (
			receipts types.Receipts
			usedGas  = new(uint64)
			allLogs  []*types.Log
			gp       = new(core.GasPool).AddGas(block.GasLimit())
		)

		if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
			misc.ApplyDAOHardFork(statedb)
		}

		for i, tx := range block.Transactions() {
			statedb.Prepare(tx.Hash(), block.Hash(), i)

			receipt, _, err := core.ApplyTransaction(chainConfig, chainContext, nil, gp, statedb, morus, header, tx, usedGas, vmConfig)

			if err != nil {
				panic(fmt.Errorf("at block %d, tx %x: %v", block.NumberU64(), tx.Hash(), err))
			}

			receipts = append(receipts, receipt)
			allLogs = append(allLogs, receipt.Logs...)
		}

		// apply mining rewards to the geth stateDB

		accumulateRewards(chainConfig, statedb, header, block.Uncles())

		// commit block in geth
		err = statedb.Commit(chainConfig.IsEIP158(block.Number()), morus)
		if err != nil {
			panic(fmt.Errorf("at block %d: %v", block.NumberU64(), err))
		}

		// commit block in Ethermint
		cp := morus.Commit()
		cpRun += cp

		if (block.NumberU64() % 10000) == 0 {
			fmt.Printf("processed %d blocks, time so far: %v\n", block.NumberU64(), time.Since(startTime))
			fmt.Printf("committed pages: %d, Mb %.3f\n", cpRun, float64(cpRun)*float64(avl.PageSize)/1024.0/1024.0)
			morus.PrintStats()
			cpRun = 0
		}

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}

	fmt.Printf("processed %d blocks\n", block.NumberU64())
}