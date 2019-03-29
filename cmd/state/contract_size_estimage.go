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

	"github.com/llgcode/draw2d/draw2dimg"
	"image"
	"image/color"
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

func estimate() {
	startTime := time.Now()
	db, err := bolt.Open("/Volumes/tb4/turbo-geth/chaindata", 0600, &bolt.Options{ReadOnly: true})
	check(err)
	defer db.Close()
	actual, err := actualContractSize(db, common.HexToAddress("0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208"))
	check(err)
	fmt.Printf("Size of IDEX_1 is %d\n", actual)
	for i := 1; i < 20; i++ {
		for j := 1; j < 20; j++ {
			estimated, err := estimateContractSize(db, common.HexToAddress("0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208"), i, j)
			check(err)
			e := (float64(actual)-float64(estimated))/float64(actual)
			fmt.Printf("probes: %d, width: %d, estimated: %d, error: %f%%\n", i, j, estimated, e*100.0)
		}
	}
	fmt.Printf("Estimation took %s\n", time.Since(startTime))
	// Initialize the graphic context on an RGBA image
	dest := image.NewRGBA(image.Rect(0, 0, 297, 210.0))
	gc := draw2dimg.NewGraphicContext(dest)

	// Set some properties
	gc.SetFillColor(color.RGBA{0x44, 0xff, 0x44, 0xff})
	gc.SetStrokeColor(color.RGBA{0x44, 0x44, 0x44, 0xff})
	gc.SetLineWidth(5)

	// Draw a closed shape
	gc.BeginPath() // Initialize a new path
	gc.MoveTo(10, 10) // Move to a position to start the new path
	gc.LineTo(100, 50)
	gc.QuadCurveTo(100, 10, 10, 10)
	gc.Close()
	gc.FillStroke()

	// Save to file
	draw2dimg.SaveToPngFile("hello.png", dest)
}
