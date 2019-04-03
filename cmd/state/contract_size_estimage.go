package main

import (
	"bytes"
	"fmt"
	"time"
	"math"
	"math/big"

	"github.com/boltdb/bolt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dimg"
	"image"
	"image/color"
	//"sort"
)

func storageRoot(db *bolt.DB, contract common.Address) (common.Hash, error) {
	var storageRoot common.Hash
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.AccountsBucket)
		if b == nil {
			return fmt.Errorf("Could not find accounts bucket")
		}
		enc := b.Get(crypto.Keccak256(contract[:]))
		if enc == nil {
			return fmt.Errorf("Could find account %x\n", contract)
		}
		account, err := encodingToAccount(enc)
		if err != nil {
			return err
		}
		storageRoot = account.Root
		return nil
	})
	return storageRoot, err
}

func actualContractSize(db *bolt.DB, contract common.Address) (int, error) {
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
		return 0, err
	}
	return actual, nil
}

func estimateContractSize(seed common.Hash, db *bolt.DB, contract common.Address, probes int, probeWidth int) (int, error) {
	var fk [52]byte
	copy(fk[:], contract[:])
	var seekkey [52]byte
	copy(seekkey[:], contract[:])
	total := big.NewInt(0)
	var large [33]byte
	large[0] = 1
	largeInt := big.NewInt(0).SetBytes(large[:])
	step := big.NewInt(0).Div(largeInt, big.NewInt(int64(probes)))
	probeKeyHash := seed[:]
	probe := big.NewInt(0).SetBytes(probeKeyHash)
	sampleCount := 0
	exact := 0
	seenK := make(map[[52]byte]struct{})
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.StorageBucket)
		for i := 0; i < probes; i++ {
			c := b.Cursor()
			for c := 20; c < 20+32-len(probeKeyHash); c++ {
				seekkey[c] = 0
			}
			copy(seekkey[20+32-len(probeKeyHash):], probeKeyHash)
			prev := big.NewInt(0)
			curr := big.NewInt(0)
			diff := big.NewInt(0)
			k, _ := c.Seek(seekkey[:])
			allSteps := big.NewInt(0)
			for j := 0; j < probeWidth+1; j++ {
				if k == nil || !bytes.HasPrefix(k, contract[:]) {
					k, _ = c.Seek(fk[:])
					if k == nil || !bytes.HasPrefix(k, contract[:]) {
						panic("")
					}
				}
				var sk [52]byte
				copy(sk[:], k)
				//if j == 0 {
					if _, ok := seenK[sk]; ok {
						//fmt.Printf("Break with i == %d, j == %d\n", i, j)
						total.Sub(total, allSteps)
						sampleCount -= j
						//fmt.Printf("Adding %d to exact\n", j)
						exact += j
						break
					} else {
						//fmt.Printf("Go on: %x\n", k[20:])
					}
				//}
				if j > 0 {
					curr.SetBytes(k[20:])
					if prev.Cmp(curr) < 0 {
						diff.Sub(curr, prev)
					} else {
						diff.Sub(prev, curr)
						diff.Sub(largeInt, diff)
					}
					total.Add(total, diff)
					sampleCount++
					allSteps.Add(allSteps, diff)
					if allSteps.Cmp(largeInt) >= 0 {
						exact = j
						sampleCount = 0
						return nil
					}
					if allSteps.Cmp(step) >= 0 {
						// Adjust to the step
						total.Sub(total, allSteps)
						sampleCount -= j
						//fmt.Printf("Adding %d to exact\n", j)
						exact += j
						break
					}
				}
				//if j < probeWidth {
					seenK[sk] = struct{}{}
				//}
				prev.SetBytes(k[20:])
				k, _ = c.Next()
			}
			probe.Add(probe, step)
			if probe.Cmp(largeInt) >= 0 {
				probe.Sub(probe, largeInt)
			}
			probeKeyHash = probe.Bytes()
		}
		return nil
	}); err != nil {
		return 0, err
	}
	estimatedInt := big.NewInt(0)
	if sampleCount > 0 {
		estimatedInt.Mul(largeInt, big.NewInt(int64(sampleCount)))
		estimatedInt.Div(estimatedInt, total)
	}
	//fmt.Printf("probes: %d, probeWidth: %d, estimate: %d, exact: %d\n", probes, probeWidth, estimatedInt, exact)
	return int(estimatedInt.Int64()) + exact, nil
}

func getHeatMapColor(value float64) (red, green, blue float64) {
  const NUM_COLORS int = 4
  color := [NUM_COLORS][3]float64{
  	{0,0,1},
  	{0,1,0},
  	{1,1,0},
  	{1,0,0},
  	}
    // A static array of 4 colors:  (blue,   green,  yellow,  red) using {r,g,b} for each.
  
  var idx1 int        // |-- Our desired color will be between these two indexes in "color".
  var idx2 int        // |
  var fractBetween float64  // Fraction between "idx1" and "idx2" where our value is.
  
  if value <= 0 {
  	idx1 = 0
  	idx2 = 0
  } else if value >= 1 {
  	idx1 = NUM_COLORS-1
  	idx2 = NUM_COLORS-1
  } else {
    value = value * float64(NUM_COLORS-1)        // Will multiply value by 3.
    idx1  = int(value)                  // Our desired color will be after this index.
    idx2  = idx1+1                        // ... and before this index (inclusive).
    fractBetween = value - float64(idx1)    // Distance between the two indexes (0-1).
  }
   
  if idx1 >= len(color) || idx1 < 0 {
  	fmt.Printf("value: %f, idx1: %d\n", value, idx1)
  }
  if idx2 >= len(color) || idx2 < 0 {
  	fmt.Printf("value: %f, idx2: %d\n", value, idx2)
  }
  red   = (color[idx2][0] - color[idx1][0])*fractBetween + color[idx1][0]
  green = (color[idx2][1] - color[idx1][1])*fractBetween + color[idx1][1]
  blue  = (color[idx2][2] - color[idx1][2])*fractBetween + color[idx1][2]
  return
}

func itemsByAddress(db *bolt.DB) map[common.Address]int {
	// Go through the current state
	var addr common.Address
	itemsByAddress := make(map[common.Address]int)
	deleted := make(map[common.Address]bool) // Deleted contracts
	numDeleted := 0
	//itemsByCreator := make(map[common.Address]int)
	count := 0
	err := db.View(func(tx *bolt.Tx) error {
		a := tx.Bucket(state.AccountsBucket)
		b := tx.Bucket(state.StorageBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			copy(addr[:], k[:20])
			del, ok := deleted[addr]
			if !ok {
				del = a.Get(crypto.Keccak256(addr[:])) == nil
				deleted[addr] = del
				if del {
					numDeleted++
				}
			}
			if del {
				continue
			}
			itemsByAddress[addr]++
			count++
			if count%100000 == 0 {
				fmt.Printf("Processed %d storage records, deleted contracts: %d\n", count, numDeleted)
			}
		}
		return nil
	})
	check(err)
	return itemsByAddress
}

func estimate() {
	startTime := time.Now()
	//db, err := bolt.Open("/Volumes/tb4/turbo-geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	db, err := bolt.Open("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	contractMap := itemsByAddress(db)
	fmt.Printf("Collected itemsByAddress: %d\n", len(contractMap))
	maxi := 30
	maxj := 20
	valMap := make(map[int][][]float64)
	count := 0
	for addr, actual := range contractMap {
		if actual < 2 {
			continue
		}
		category := int(math.Log2(float64(actual)))
		if category != 1 {
			continue
		}
		vals, ok := valMap[category]
		if !ok {
			vals = make([][]float64, maxi)
			for i := 1; i < maxi; i++ {
				vals[i] = make([]float64, maxj)
			}
			valMap[category] = vals
		}
		seed, err := storageRoot(db, addr)
		check(err)
		for i := 1; i < maxi; i++ {
			for j := 1; j < maxj; j++ {
				estimated, err := estimateContractSize(seed, db, addr, i, j)
				check(err)
				e := math.Abs((float64(actual)-float64(estimated))/float64(actual))
				if e > vals[i][j] {
					vals[i][j] = e
				}
			}
		}
		count++
		if count % 1000 == 0 {
			fmt.Printf("Processed contracts: %d\n", count)
		}
		fmt.Printf("Actual size: %d\n", actual)
		break
	}
	fmt.Printf("Generating images...\n")
	for category, vals := range valMap {
		var maxe float64
		var mine float64 = 100000000.0
		for i := 1; i < maxi; i++ {
			for j := 1; j < maxj; j++ {
				if vals[i][j] > maxe {
					maxe = vals[i][j]
				}
				if vals[i][j] < mine {
					mine = vals[i][j]
				}
			}
		}
		if maxe > 1.0 {
			maxe = 1.0
		}
		if maxe == mine {
			maxe = mine + 1.0
		}
		// Initialize the graphic context on an RGBA image
		imageWidth := 1280
		imageHeight := 720
		dest := image.NewRGBA(image.Rect(0, 0, imageWidth, imageHeight))
		gc := draw2dimg.NewGraphicContext(dest)

		// Set some properties
		gc.SetFillColor(color.RGBA{0x44, 0xff, 0x44, 0xff})
		gc.SetStrokeColor(color.RGBA{0x44, 0x44, 0x44, 0xff})
		gc.SetLineWidth(1)
		cellWidth := float64(imageWidth) / float64(maxi+1)
		cellHeight := float64(imageHeight) / float64(maxj+1)
		for i := 1; i < maxi; i++ {
			fi := float64(i)
			gc.SetFontData(draw2d.FontData{Name: "luxi", Family: draw2d.FontFamilyMono})
			gc.SetFillColor(image.Black)
			gc.SetFontSize(12)
			gc.FillStringAt(fmt.Sprintf("%d", i), fi*cellWidth+5, 0.5*cellHeight)
		}
		for j := 1; j < maxj; j++ {
			fj := float64(j)
			gc.SetFontData(draw2d.FontData{Name: "luxi", Family: draw2d.FontFamilyMono})
			gc.SetFillColor(image.Black)
			gc.SetFontSize(12)
			gc.FillStringAt(fmt.Sprintf("%d", j), 0*cellWidth+5, (fj+0.5)*cellHeight)
		}
		for i := 1; i < maxi; i++ {
			for j := 1; j < maxj; j++ {
				e := vals[i][j]
				heat := math.Abs(e)
				if heat > 1.0 {
					heat = 1.0
				}
				heat = (heat-mine)/(maxe-mine)
				red, green, blue := getHeatMapColor(heat)
				txt := fmt.Sprintf("%.1f%%", e*100.0)
				fi := float64(i)
				fj := float64(j)
				gc.BeginPath() // Initialize a new path
				gc.MoveTo(fi*cellWidth, fj*cellHeight)
				gc.LineTo((fi+1)*cellWidth, fj*cellHeight)
				gc.LineTo((fi+1)*cellWidth, (fj+1)*cellHeight)
				gc.LineTo(fi*cellWidth, (fj+1)*cellHeight)
				gc.LineTo(fi*cellWidth, fj*cellHeight)
				gc.Close()
				gc.SetFillColor(color.RGBA{byte(255.0*red), byte(255.0*green), byte(255.0*blue), 0xff})
				gc.FillStroke()
				gc.SetFontData(draw2d.FontData{Name: "luxi", Family: draw2d.FontFamilyMono})
				gc.SetFillColor(image.Black)
				gc.SetFontSize(8)
				gc.FillStringAt(txt, fi*cellWidth+5, (fj+0.5)*cellHeight)
			}
		}
		// Save to file
		draw2dimg.SaveToPngFile(fmt.Sprintf("heap_%d.png", category), dest)
	}
	fmt.Printf("Estimation took %s\n", time.Since(startTime))
}
