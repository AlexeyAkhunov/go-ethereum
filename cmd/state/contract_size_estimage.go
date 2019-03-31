package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	"math"
	"math/big"
	"math/rand"

	"github.com/boltdb/bolt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"

	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dimg"
	"image"
	"image/color"
	"sort"
)

func randKeyHash(r *rand.Rand) common.Hash {
	var b common.Hash
	for i := 0; i < 32; i+=8 {
		binary.BigEndian.PutUint64(b[i:], r.Uint64())
	}
	return b
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

func estimateContractSize(db *bolt.DB, contract common.Address, probes int, probeWidth int) (int, error) {
	var fk [52]byte
	copy(fk[:], contract[:])
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
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(state.StorageBucket)
		for i := 0; i < probes; i++ {
			probeKeyHash := randKeyHash(r)
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
			total = total.Add(total, diff)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	estimatedInt := big.NewInt(0).Mul(largeInt, big.NewInt(int64(probes)))
	estimatedInt = estimatedInt.Mul(estimatedInt, big.NewInt(int64(probeWidth)))
	estimatedInt = estimatedInt.Div(estimatedInt, total)
	return int(estimatedInt.Int64()), nil
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
    
  red   = (color[idx2][0] - color[idx1][0])*fractBetween + color[idx1][0]
  green = (color[idx2][1] - color[idx1][1])*fractBetween + color[idx1][1]
  blue  = (color[idx2][2] - color[idx1][2])*fractBetween + color[idx1][2]
  return
}

func estimate() {
	startTime := time.Now()
	db, err := bolt.Open("/Volumes/tb4/turbo-geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	//addr := common.HexToAddress("0x8d12a197cb00d4747a1fe03395095ce2a5cc6819")
	addr := common.HexToAddress("0x06012c8cf97bead5deae237070f9587f8e7a266d")
	actual, err := actualContractSize(db, addr)
	//actual, err := actualContractSize(db, common.HexToAddress("0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208"))
	check(err)
	fmt.Printf("Size of IDEX_1 is %d\n", actual)
	maxi := 30
	maxj := 20
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
	vals := make([][]float64, maxi)
	var maxe float64
	var mine float64 = 100000000.0
	var es []float64
	for i := 1; i < maxi; i++ {
		vals[i] = make([]float64, maxj)
		for j := 1; j < maxj; j++ {
			estimated, err := estimateContractSize(db, addr, i, j)
			check(err)
			e := (float64(actual)-float64(estimated))/float64(actual)
			vals[i][j] = e
			if math.Abs(e) > maxe {
				maxe = math.Abs(e)
			}
			if math.Abs(e) < mine {
				mine = math.Abs(e)
			}
			es = append(es, math.Abs(e))
		}
	}
	sort.Float64s(es)
	median := es[len(es)/2]
	fmt.Printf("Median: %f\n", median)
	for i := 1; i < maxi; i++ {
		for j := 1; j < maxj; j++ {
			e := vals[i][j]
			heat := math.Abs(e)
			if heat < median {
				heat = 0.5*(heat - mine)/(median-mine)
			} else {
				heat = 0.5 + 0.5*(heat - median)/(maxe-median)
			}
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
	draw2dimg.SaveToPngFile("hello.png", dest)
	fmt.Printf("Estimation took %s\n", time.Since(startTime))
}
