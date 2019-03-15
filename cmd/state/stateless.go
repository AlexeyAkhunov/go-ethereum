package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"bufio"
	"time"
	"syscall"
	"os/signal"
	"encoding/csv"

	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"

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

var chartColors = []drawing.Color{
	chart.ColorBlue,
	chart.ColorGreen,
	chart.ColorRed,
	chart.ColorYellow,
	chart.ColorOrange,
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

	//ethDb, err := ethdb.NewLDBDatabase("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	//ethDb, err := ethdb.NewLDBDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	ethDb, err := ethdb.NewLDBDatabase("/home/akhounov/.ethereum/geth/chaindata1")
	check(err)
	defer ethDb.Close()
	chainConfig := params.MainnetChainConfig
	//slFile, err := os.OpenFile("/Volumes/tb4/turbo-geth/stateless.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	slFile, err := os.OpenFile("stateless2.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
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
	bc.SetResolveReads(false)
	blockNum := uint64(1)
	interrupt := false
	for !interrupt {
		block := bcb.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		trace := blockNum == 3943607
		if trace {
			filename := fmt.Sprintf("right_%d.txt", blockNum-1)
			f, err1 := os.Create(filename)
			if err1 == nil {
				defer f.Close()
				bc.GetTrieDbState().PrintTrie(f)
				//bc.GetTrieDbState().PrintStorageTrie(f, common.BytesToHash(common.FromHex("0x1570aaebc55e1a8067cc4a5c3ba451d196bf91544f183168b51bb1d306bda995")))
			}
		}
		if blockNum >= 3943607 {
			bc.SetResolveReads(true)
		} else {
			bc.SetResolveReads(false)
		}
		_, err = bc.InsertChain(types.Blocks{block})
		if err != nil {
			panic(fmt.Sprintf("Failed on block %d, error: %v\n", blockNum, err))
		}
		check(err)
		header := block.Header()
		if blockNum >= 3943607 {
			contracts, cMasks, cHashes, cShortKeys, cValues, codes, masks, hashes, shortKeys, values := bc.GetTrieDbState().ExtractProofs(trace)
			dbstate, err := state.NewStateless(preRoot,
				contracts, cMasks, cHashes, cShortKeys, cValues,
				codes,
				masks, hashes, shortKeys, values,
				block.NumberU64()-1, trace,
			)
			if err != nil {
				fmt.Printf("Error making state for block %d: %v\n", blockNum, err)
			} else {
				statedb := state.New(dbstate)
				gp := new(core.GasPool).AddGas(block.GasLimit())
				usedGas := new(uint64)
				var receipts types.Receipts
				if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
					misc.ApplyDAOHardFork(statedb)
				}
				for _, tx := range block.Transactions() {
					receipt, _, err := core.ApplyTransaction(chainConfig, bc, nil, gp, statedb, dbstate, header, tx, usedGas, vmConfig)
					if err != nil {
						panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
					}
					receipts = append(receipts, receipt)
				}
				// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
				_, err = engine.Finalize(bcb, header, statedb, block.Transactions(), block.Uncles(), receipts)
				if err != nil {
					panic(fmt.Errorf("Finalize of block %d failed: %v", blockNum, err))
				}
				err = statedb.Commit(chainConfig.IsEIP158(header.Number), dbstate)
				if err != nil {
					panic(fmt.Errorf("Commiting block %d failed: %v", blockNum, err))
				}
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
					len(masks), len(hashes), len(shortKeys), len(values), totalCShorts, totalCValues, totalCodes, totalShorts, totalValues,
				)
			}
		}
		preRoot = header.Root
		blockNum++
		if blockNum == 2416461 {
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

func stateless_chart_key_values(right []int, chartFileName string, start int) {
	file, err := os.Open("stateless1.csv")
	check(err)
	defer file.Close()
	reader := csv.NewReader(bufio.NewReader(file))
	var blocks []float64
	var vals [15][]float64
	count := 0
	for records, _ := reader.Read(); records != nil; records, _ = reader.Read() {
		count++
		if count < start {
			continue
		}
		blocks = append(blocks, parseFloat64(records[0])/1000000.0)
		for i := 0; i < 15; i++ {
			switch i {
			case 11:
				vals[i] = append(vals[i], 32.0*parseFloat64(records[4]))
			case 1,6:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[i+1]))
			case 2,7:
				vals[i] = append(vals[i], 32.0*parseFloat64(records[i+1]))
			default:
				vals[i] = append(vals[i], parseFloat64(records[i+1]))
			}
		}
	}
	var windowSums [15] float64
	var window int = 1024
	var movingAvgs [15][]float64
	for i := 0; i < 15; i++ {
		movingAvgs[i] = make([]float64, len(blocks)-(window-1))
	}
	for j := 0; j < len(blocks); j++ {
		for i := 0; i < 15; i++ {
			windowSums[i] += vals[i][j]
		}
		if j >= window {
			for i := 0; i < 15; i++ {
				windowSums[i] -= vals[i][j-window]
			}
		}
		if j >= window-1 {
			for i := 0; i < 15; i++ {
				movingAvgs[i][j-window+1] = windowSums[i]/float64(window)
			}
		}
	}
	movingBlock := blocks[window-1:]
	seriesNames := [15]string{
		"Number of contracts",
		"Contract masks",
		"Contract hashes",
		"Number of contract leaf keys",
		"Number of contract leaf vals",
		"Number of contract codes",
		"Masks",
		"Hashes",
		"Number of leaf keys",
		"Number of leaf values",
		"Total size of contract leaf keys",
		"Total size of contract leaf vals",
		"Total size of codes",
		"Total size of leaf keys",
		"Total size of leaf vals",
	}
	var currentColor int
	var series []chart.Series
	for _, r := range right {
		s := &chart.ContinuousSeries{
			Name: seriesNames[r],
			Style: chart.Style{
				Show:        true,
				StrokeColor: chartColors[currentColor],
				//FillColor:   chartColors[currentColor].WithAlpha(100),
			},
			XValues: movingBlock,
			YValues: movingAvgs[r],
		}
		currentColor++
		series = append(series, s)
	}

	graph1 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "kBytes",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d kB", int(v.(float64)/1024.0))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlack,
				StrokeWidth: 1.0,
			},
			//GridLines: days(),
		},
		/*
		YAxisSecondary: chart.YAxis{
			NameStyle: chart.StyleShow(),
			Style: chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d", int(v.(float64)))
			},
		},
		*/
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: blockMillions(),
		},
		Series: series,
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile(chartFileName, buffer.Bytes(), 0644)
    check(err)
}