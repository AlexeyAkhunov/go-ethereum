// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/petar/GoLLRB/llrb"
)

var EndSuffix []byte = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// Generates rewind data for all buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func rewindData(db Getter, timestampSrc, timestampDst uint64, df func(bucket, key, value []byte) error) error {
	// Collect list of buckets and keys that need to be considered
	m := make(map[string]*llrb.LLRB)
	suffixDst := encodeTimestamp(timestampDst+1)
	if err := db.Walk(SuffixBucket, suffixDst, 0, func (k, v []byte) (bool, error) {
		timestamp, bucket := decodeTimestamp(k)
		if timestamp > timestampSrc {
			return false, nil
		}
		var t *llrb.LLRB
		var ok bool
		keycount := int(binary.BigEndian.Uint32(v))
		if keycount > 0 {
			bucketStr := string(common.CopyBytes(bucket))
			if t, ok = m[bucketStr]; !ok {
				t = llrb.New()
				m[bucketStr] = t
			}
		}
		for i, ki := 4, 0; ki < keycount; ki++ {
			l := int(v[i])
			i++
			t.ReplaceOrInsert(&PutItem{key: common.CopyBytes(v[i:i+l]), value: nil})
			i += l
		}
		return true, nil
	}); err != nil {
		return err
	}
	//suffixDst := encodeTimestamp(timestampDst)
	for bucketStr, t := range m {
		bucket := []byte(bucketStr)
		//it := t.NewSeekIterator()
		min, _ := t.Min().(*PutItem)
		if min == nil {
			return nil
		}
		/*
		var item *PutItem = it.SeekTo(min).(*PutItem)
		seeking := false
		for !seeking && item != nil {
			startkey := make([]byte, len(item.key) + len(suffixDst))
			copy(startkey[:], item.key)
			copy(startkey[len(item.key):], suffixDst)
			seeking = true
			if err := db.Walk(bucket, startkey, 0, func (k, v []byte) ([]byte, WalkAction, error) {
				if bytes.Compare(k, startkey) < 0 {
					return nil, WalkActionNext, nil
				}
				// Check if we found the "item" in the database
				if bytes.HasPrefix(k, item.key) {
					item.value = common.CopyBytes(v)
					item, _ = it.SeekTo(item).(*PutItem)
				} else {
					// Find the next item that could match
					for bytes.Compare(item.key, k[:len(item.key)]) < 0 {
						item, _ = it.SeekTo(item).(*PutItem)
						if item == nil {
							seeking = false
							return nil, WalkActionStop, nil
						}
					}
					if bytes.HasPrefix(k, item.key) && bytes.Compare(k[len(item.key):], suffixDst) <= 0 {
						item.value = common.CopyBytes(v)
						item, _ = it.SeekTo(item).(*PutItem)
					}
				}
				if item == nil {
					seeking = false
					return nil, WalkActionStop, nil
				}
				wr := make([]byte, len(item.key) + len(suffixDst))
				copy(wr, item.key)
				copy(wr[len(item.key):], suffixDst)
				seeking = true
				return wr, WalkActionSeek, nil
			}); err != nil {
				return err
			}
		}
		*/
		var extErr error
		t.AscendGreaterOrEqual1(min, func(i llrb.Item) bool {
			item := i.(*PutItem)
			var sk []byte
			if len(item.key) == 52 {
				sk = item.key[20:]
			} else {
				sk = item.key
			}
			preimage, _ := db.Get([]byte("secure-key-"), sk)
			fmt.Printf("bucket: %s, key: %x, preimage: %x\n", bucketStr, item.key, preimage)
			value, err := db.GetAsOf(bucket[1:], bucket, item.key, timestampDst+1)
			if err != nil {
				value = nil
			}
			df(bucket, item.key, value)
			return true
		})
		if extErr != nil {
			return extErr
		}
	}
	return nil
}

func GetModifiedAccounts(db Getter, starttimestamp, endtimestamp uint64) ([]common.Address, error) {
	t := llrb.New()
	startCode := encodeTimestamp(starttimestamp)
	if err := db.Walk(SuffixBucket, startCode, 0, func (k, v []byte) (bool, error) {
		timestamp, bucket := decodeTimestamp(k)
		if !bytes.Equal(bucket, []byte("hAT")) {
			return true, nil
		}
		if timestamp > endtimestamp {
			return false, nil
		}
		keycount := int(binary.BigEndian.Uint32(v))
		for i, ki := 4, 0; ki < keycount; ki++ {
			l := int(v[i])
			i++
			t.ReplaceOrInsert(&PutItem{key: common.CopyBytes(v[i:i+l]), value: nil})
			i += l
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	accounts := make([]common.Address, t.Len())
	if t.Len() == 0 {
		return accounts, nil
	}
	idx := 0
	var extErr error
	min, _ := t.Min().(*PutItem)
	if min == nil {
		return accounts, nil
	}
	t.AscendGreaterOrEqual1(min, func(i llrb.Item) bool {
		item := i.(*PutItem)
		value, err := db.Get([]byte("secure-key-"), item.key)
		if err != nil {
			extErr = fmt.Errorf("Could not get preimage for key %x", item.key)
			return false
		}
		copy(accounts[idx][:], value)
		idx++
		return true
	})
	if extErr != nil {
		return nil, extErr
	}
	return accounts, nil
}

var testbucket = []byte("B")

func TestRewindData1Bucket() {
	db := NewMemDatabase()
	batch := db.NewBatch()

	htestbucket := append([]byte("h"), testbucket...)
	batch.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	batch.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	batch.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"))
	batch.PutS(htestbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	batch.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"))
	batch.PutS(htestbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	batch.Put(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)
	batch.Put(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)

	batch.Put(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxzzzzzzzzzzzzzzzzzzzzzzzz"))
	batch.PutS(htestbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxzzzzzzzzzzzzzzzzzzzzzzzz"), 2)
	batch.Put(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	batch.Put(testbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	batch.Put(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaaaaaaaaaaaaaaaaaaaaaaa"))
	batch.PutS(htestbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaaaaaaaaaaaaaaaaaaaaaaa"), 2)
	batch.Put(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)

	batch.Delete(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
	batch.PutS(htestbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), nil, 3)
	batch.Put(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 3)
	if err := batch.Commit(); err != nil {
		fmt.Printf("Could not commit: %v\n", err)
		return
	}

	count := 0
	err := rewindData(db, 3, 2, func(bucket, key, value []byte) error {
		count++
		return nil
	})
	if err != nil {
		fmt.Printf("Could not rewind 3->2 %v\n", err)
		return
	}
	if count != 2 {
		fmt.Printf("Expected %d items in rewind data, got %d\n", 2, count)
		return
	}

	count = 0
	err = rewindData(db, 3, 0, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		fmt.Printf("Could not rewind 3->0 %v\n", err)
		return
	}
	if count != 7 {
		fmt.Printf("Expected %d items in rewind data, got %d\n", 7, count)
		return
	}
}

func TestRewindData2Bucket() {
	db := NewMemDatabase()
	batch := db.NewBatch()

	otherbucket := []byte("OB")
	htestbucket := append([]byte("h"), testbucket...)
	hotherbucket := append([]byte("h"), otherbucket...)

	batch.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	batch.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	batch.Put(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"))
	batch.PutS(htestbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	batch.Put(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"))
	batch.PutS(htestbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	batch.Put(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)
	batch.Put(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)

	batch.Put(otherbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxzzzzzzzzzzzzzzzzzzzzzzzz"))
	batch.PutS(hotherbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxzzzzzzzzzzzzzzzzzzzzzzzz"), 2)
	batch.Put(otherbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(hotherbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	batch.Put(otherbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(hotherbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	batch.Put(otherbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaaaaaaaaaaaaaaaaaaaaaaa"))
	batch.PutS(hotherbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaaaaaaaaaaaaaaaaaaaaaaa"), 2)
	batch.Put(otherbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(hotherbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)

	batch.Delete(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
	batch.PutS(htestbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), nil, 3)
	batch.Put(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	batch.PutS(htestbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 3)
	batch.Commit()

	count := 0
	err := rewindData(db, 3, 2, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		fmt.Printf("Could not rewind 3->2 %v\n", err)
		return
	}
	if count != 2 {
		fmt.Printf("Expected %d items in rewind data, got %d\n", 2, count)
	}

	count = 0
	err = rewindData(db, 3, 0, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		fmt.Printf("Could not rewind 3->0 %v\n", err)
		return
	}
	if count != 11 {
		fmt.Printf("Expected %d items in rewind data, got %d\n", 11, count)
		return
	}
}
