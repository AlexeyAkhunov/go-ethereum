package avl

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/bits"
	"encoding/binary"
	"runtime"
	lru "github.com/hashicorp/golang-lru"
	"os"
)

type PageID uint64
type Version uint64
const PageSize uint32 = 4096     // Page size
const HashLength uint32 = sha256.Size
const InlineValueMax uint32 = 90 // Maximum size of an inline value (normally size of hash output + valueId)
type Hash [HashLength]byte

// AVL+ organised into pages, with history

type Avl1 struct {
	currentVersion Version
	trace bool
	maxPageId PageID
	pageMap map[PageID][]byte // Pages by pageId
	maxValueId uint64
	valueMap map[uint64][]byte // Large values by valueId
	valueHashes map[uint64]Hash // Value hashes
	valueLens map[uint64]uint32 // Value lengths
	freelist []PageID // Free list of pages
	buffer Ref1 // Root of the current update buffer
	versions map[Version]PageID // root pageId for a version
	pageCache *lru.Cache
	commitedCounter uint64
	pageSpace uint64
	pageFile, valueFile, verFile *os.File
	hashLength uint32
	compare func([]byte, []byte) int
}

func NewAvl1() *Avl1 {
	t := &Avl1{
		pageMap: make(map[PageID][]byte),
		valueMap: make(map[uint64][]byte),
		valueHashes: make(map[uint64]Hash),
		valueLens: make(map[uint64]uint32),
		versions: make(map[Version]PageID),
	}
	pageCache, err := lru.New(128*1024)
	if err != nil {
		panic(err)
	}
	t.pageCache = pageCache
	t.hashLength = 32
	t.compare = bytes.Compare
	return t
}

func (t *Avl1) SetCompare(c func([]byte, []byte) int) {
	t.compare = c
}

func (t *Avl1) walkToArrowPoint(r Ref1, key []byte, height uint32) Ref1 {
	current := r
	for {
		switch n := current.(type) {
		case nil:
			return nil
		case *Leaf1:
			if height != 1 || !bytes.Equal(key, n.key) {
				panic(fmt.Sprintf("Leaf1 with key %s, expected height %d key %s", n.key, height, key))
			}
			return n
		case *Fork1:
			if n.height < height {
				panic(fmt.Sprintf("Fork1 with height %d max %s, expected height %d key %s", n.height, n.max, height, key))
			} else if n.height > height {
				switch t.compare(key, n.left.getmax()) {
				case -1, 0:
					current = n.left
				case 1:
					current = n.right
				}
			} else {
				if bytes.Equal(key, n.max) {
					return n
				} else {
					panic(fmt.Sprintf("Fork1 with height %d max %s, expected height %d key %s", n.height, n.max, height, key))
				}
			}
		case *Arrow1:
			panic(fmt.Sprintf("Arrow1 with height %d max %s, expected height %d key %s", n.height, n.max, height, key))
		}
	}
}

func (t *Avl1) UseFiles(pageFileName, valueFileName, verFileName string, read bool) {
	var err error
	if read {
		t.pageFile, err = os.Open(pageFileName)
	} else {
		t.pageFile, err = os.OpenFile(pageFileName, os.O_RDWR|os.O_CREATE, 0600)
	}
	if err != nil {
		panic(err)
	}
	if read {
		t.valueFile, err = os.Open(valueFileName)
	} else {
		t.valueFile, err = os.OpenFile(valueFileName, os.O_RDWR|os.O_CREATE, 0600)
	}
	if err != nil {
		panic(err)
	}
	if read {
		t.verFile, err = os.Open(verFileName)
	} else {
		t.verFile, err = os.OpenFile(verFileName, os.O_RDWR|os.O_CREATE, 0600)
	}
	if err != nil {
		fmt.Printf("Could not open version file: %v\n", err)
		return
	}
	// Read versions
	var verdata [8]byte
	var lastPageID PageID
	for n, _ := t.verFile.ReadAt(verdata[:], int64(t.currentVersion)*int64(8)); n >0; n, _ = t.verFile.ReadAt(verdata[:], int64(t.currentVersion)*int64(8)) {
		lastPageID = PageID(binary.BigEndian.Uint64(verdata[:]))
		t.versions[t.currentVersion] = lastPageID
		t.currentVersion++
	}
	if t.currentVersion > 0 {
		t.maxPageId = lastPageID
		fmt.Printf("Deserialising page %d\n", lastPageID)
		root := t.deserialisePage(lastPageID)
		t.buffer = &Arrow1{pageId: lastPageID, height: root.getheight(), max: root.getmax()}
	}
}

func (t *Avl1) Close() {
	if t.pageFile != nil {
		t.pageFile.Close()
	}
	if t.valueFile != nil {
		t.valueFile.Close()
	}
	if t.verFile != nil {
		t.verFile.Close()
	}
}

func (t *Avl1) CurrentVersion() Version {
	return t.currentVersion
}

func (t *Avl1) Scan() {
	if info, err := t.pageFile.Stat(); err != nil {
		panic(err)
	} else {
		t.maxPageId = PageID(info.Size()/int64(PageSize))
	}
	current := t.deserialisePage(t.maxPageId-1)
	m := make(map[PageID]struct{})
	fmt.Printf("Max page depth: %d\n", t.scan(current, m))
	fmt.Printf("Pages in current state: %d\n", len(m))
}

func (t *Avl1) SpaceScan() {
	if info, err := t.pageFile.Stat(); err != nil {
		panic(err)
	} else {
		t.maxPageId = PageID(info.Size()/int64(PageSize))
	}
	var pCount, arrows, leaves, totalArrowBits, totalStructBits, totalPrefixLen, totalKeyBodies, totalValBodies uint64
	maxPageId := t.maxPageId
	for pageId := PageID(1); pageId < maxPageId; pageId++ {
		p := t.deserialisePage(pageId)
		if p == nil {
			continue
		}
		pCount++
		prefix, keyCount, pageCount, keyBodySize, valBodySize, structBits := p.serialisePass1(t)
		arrows += uint64(pageCount)
		leaves += uint64(keyCount)
		nodeCount := pageCount + keyCount
		totalArrowBits += uint64(4*((nodeCount+31)/32))
		totalStructBits += uint64(4*((structBits+31)/32))
		totalPrefixLen += uint64(len(prefix))
		totalKeyBodies += uint64(keyBodySize - nodeCount*uint32(len(prefix)))
		totalValBodies += uint64(valBodySize)
		if pageId % 10000 == 0 {
			fmt.Printf("Process %d pages\n", pageId)
		}
	}
	totalSize := uint64(pCount)*uint64(PageSize)
	totalPageFixed := uint64(pCount)*uint64(12)
	totalKeyHeader := (arrows+leaves)*uint64(4)
	totalArrowHeader := arrows*uint64(12+t.hashLength)
	totalValueHeader := leaves*uint64(4)
	totalSlack := totalSize - totalPageFixed - totalKeyHeader - totalArrowHeader - totalValueHeader - totalArrowBits - totalStructBits - totalPrefixLen - totalKeyBodies - totalValBodies
	fmt.Printf("Total size: %d\n", totalSize)
	fmt.Printf("Page fixed headers: %d, %.3f percent\n", totalPageFixed, 100.0*float64(totalPageFixed)/float64(totalSize))
	fmt.Printf("Key headers: %d, %.3f percent\n", totalKeyHeader, 100.0*float64(totalKeyHeader)/float64(totalSize))
	fmt.Printf("Arrow headers: %d, %.3f percent\n", totalArrowHeader, 100.0*float64(totalArrowHeader)/float64(totalSize))
	fmt.Printf("Value headers: %d, %.3f percent\n", totalValueHeader, 100.0*float64(totalValueHeader)/float64(totalSize))
	fmt.Printf("Arrow bits: %d, %.3f percent\n", totalArrowBits, 100.0*float64(totalArrowBits)/float64(totalSize))
	fmt.Printf("Struct bits: %d, %.3f percent\n", totalStructBits, 100.0*float64(totalStructBits)/float64(totalSize))
	fmt.Printf("Prefixes: %d, %.3f percent\n", totalPrefixLen, 100.0*float64(totalPrefixLen)/float64(totalSize))
	fmt.Printf("Key bodies: %d, %.3f percent\n", totalKeyBodies, 100.0*float64(totalKeyBodies)/float64(totalSize))
	fmt.Printf("Value bodies: %d, %3.f percent\n", totalValBodies, 100.0*float64(totalValBodies)/float64(totalSize))
	fmt.Printf("Slack: %d, %.3f percent\n", totalSlack, 100.0*float64(totalSlack)/float64(totalSize))
}

func (t *Avl1) scan(r Ref1, m map[PageID]struct{}) int {
	switch r := r.(type) {
	case *Leaf1:
		return 0
	case *Fork1:
		ld := t.scan(r.left, m)
		rd := t.scan(r.right, m)
		if ld > rd {
			return ld
		} else {
			return rd
		}
	case *Arrow1:
		root := t.deserialisePage(r.pageId)
		if _, ok := m[r.pageId]; !ok {
			m[r.pageId] = struct{}{}
			if len(m) % 10000 == 0 {
				fmt.Printf("Read %d pages\n", len(m))
			}
		}
		point := t.walkToArrowPoint(root, r.max, r.height)
		return 1 + t.scan(point, m)
	}
	return 0
}

func (t *Avl1) SetHashLength(hashLength uint32) {
	t.hashLength = hashLength
}

type Leaf1 struct {
	key, value []byte
	valueId uint64
	valueLen uint32
}

type Fork1 struct {
	height uint32 // Height of the entire subtree rooted at this node, including node itself
	left, right Ref1
	max []byte // Largest key in the subtree
}

type Arrow1 struct {
	pageId PageID // Connection of this page to the page on the disk. Normally pageId corresponds to offset pageId*PageSize in the database file
	height uint32 // Height of the entire subtree rooted at this page
	max []byte
}

// Reference can be either a WbtNode2, or WbtArrow1. The latter is used when the leaves of one page reference another page
type Ref1 interface {
	getheight() uint32
	nkey() []byte
	getmax() []byte
	dot(*Avl1, *graphContext, string)
	heightsCorrect(path string) (uint32, bool)
	balanceCorrect() bool
	serialisePass1(t *Avl1) (prefix []byte, keyCount, pageCount, keyBodySize, valBodySize, structBits uint32)
}

func (t *Avl1) nextPageId() PageID {
	if len(t.freelist) > 0 {
		nextId := t.freelist[len(t.freelist)-1]
		t.freelist = t.freelist[:len(t.freelist)-1]
		return nextId
	}
	t.maxPageId++
	return t.maxPageId
}

func (t *Avl1) freePageId(pageId PageID) {
	if _, ok := t.pageMap[pageId]; ok {
		delete(t.pageMap, pageId)
		t.freelist = append(t.freelist, pageId)
	}
}

func (t *Avl1) nextValueId() uint64 {
	return t.maxValueId + 1
}

func (t *Avl1) freeValueId(valueId uint64) {
	if valueId == 0 {
		return
	}
	delete(t.valueMap, valueId)
	delete(t.valueHashes, valueId)
	delete(t.valueLens, valueId)
}

func (t *Avl1) addValue(valueId uint64, value []byte) {
	if t.valueFile == nil {
		t.valueMap[valueId] = value
		t.valueLens[valueId] = uint32(len(value))
		t.maxValueId++
	} else {
		if _, err := t.valueFile.WriteAt(value, int64(valueId)-1); err != nil {
			panic(err)
		}
		t.maxValueId += uint64(len(value))
	}
}

func (t *Avl1) SetTracing(tracing bool) {
	t.trace = tracing
}

func (l *Leaf1) getheight() uint32 {
	return 1
}

func (f *Fork1) getheight() uint32 {
	return f.height
}

func (a *Arrow1) getheight() uint32 {
	return a.height
}

func (l *Leaf1) nkey() []byte {
	return l.key
}

func (f *Fork1) nkey() []byte {
	return f.max
}

func (a *Arrow1) nkey() []byte {
	return a.max
}

func (l *Leaf1) getmax() []byte {
	return l.key
}

func (f *Fork1) getmax() []byte {
	return f.max
}

func (a *Arrow1) getmax() []byte {
	return a.max
}

func (l *Leaf1) nvalue(t *Avl1) []byte {
	if l.valueId == 0 {
		return l.value
	} else if t.valueFile == nil {
		return t.valueMap[l.valueId]
	} else {
		val := make([]byte, l.valueLen)
		if _, err := t.valueFile.ReadAt(val, int64(l.valueId)-1); err != nil {
			panic(err)
		}
		return val
	}
}

func (t *Avl1) Get(key []byte) ([]byte, bool) {
	trace := t.trace
	var current Ref1 = t.buffer
	for {
		switch n := current.(type) {
		case nil:
			return nil, false
		case *Leaf1:
			if trace {
				fmt.Printf("Get %s on leaf %s\n", key, n.key)
			}
			if bytes.Equal(key, n.key) {
				return n.nvalue(t), true
			}
			return nil, false
		case *Fork1:
			if trace {
				fmt.Printf("Get %s on fork %s %d\n", key, n.max, n.height)
			}
			switch t.compare(key, n.left.getmax()) {
			case 0, -1:
				if trace {
					fmt.Printf("Go left\n")
				}	
				current = n.left
			case 1:
				if trace {
					fmt.Printf("Go right\n")
				}
				current = n.right
			}
		case *Arrow1:
			root := t.deserialisePage(n.pageId)
			point := t.walkToArrowPoint(root, n.max, n.height)
			current = point
		}
	}
}

func (t *Avl1) Peek(r Ref1) Ref1 {
	current := r
	for {
		switch r := current.(type) {
		case nil:
			panic("nil")
		case *Arrow1:
			root := t.deserialisePage(r.pageId)
			point := t.walkToArrowPoint(root, r.max, r.height)
			switch point := point.(type) {
			case *Leaf1:
				return point
			case *Fork1:
				return point
			case *Arrow1:
				current = point
			}
		case *Fork1:
			return r
		case *Leaf1:
			return r
		}
	}
	panic("")
}

func (t *Avl1) Peel(r Ref1) Ref1 {
	current := r
	for {
		switch r := current.(type) {
		case nil:
			panic("nil")
		case *Arrow1:
			root := t.deserialisePage(r.pageId)
			point := t.walkToArrowPoint(root, r.max, r.height)
			switch point := point.(type) {
			case *Leaf1:
				if t.trace {
					fmt.Printf("Moving arrow P.%d[%s %d] over the leaf %s: %s (%d)\n", r.pageId, r.max, r.height, point.nkey(), point.nvalue(t), point.valueId)
				}
				l := &Leaf1{key: point.key, value: point.nvalue(t), valueLen: point.valueLen}
				l.valueLen = uint32(len(l.value))
				return l
			case *Fork1:
				if t.trace {
					fmt.Printf("Moving arrow P.%d[%s %d] over the fork %s\n", r.pageId, r.max, r.height, point.nkey())
				}
				lArrow := &Arrow1{pageId: r.pageId, height: point.left.getheight(), max: point.left.getmax()}
				if la, ok := point.left.(*Arrow1); ok {
					lArrow.pageId = la.pageId
				}
				rArrow := &Arrow1{pageId: r.pageId, height: point.right.getheight(), max: point.right.getmax()}
				if ra, ok := point.right.(*Arrow1); ok {
					rArrow.pageId = ra.pageId
				}
				if t.trace {
					fmt.Printf("Left arrow P.%d[%s %d], right arrow P.%d[%s %d]\n", lArrow.pageId, lArrow.max, lArrow.height, rArrow.pageId, rArrow.max, rArrow.height)
				}
				fork := &Fork1{max: point.max, height: point.height, left: lArrow, right: rArrow}
				return fork
			case *Arrow1:
				current = point
			}
		case *Fork1:
			return r
		case *Leaf1:
			return r
		}
	}
	panic("")
}

func (t *Avl1) attach(r Ref1, c int, node Ref1) Ref1 {
	switch r := r.(type) {
	case nil:
		t.buffer = node
	case *Fork1:
		switch c {
		case 0, -1:
			r.left = node
		case 1:
			r.right = node
		}
	case *Arrow1:
		panic("Not implemented")
	}
	return node
}

func maxu32(x, y uint32) uint32 {
	if x > y {
		return x
	} else {
		return y
	}
}

func (t *Avl1) Insert(key, value []byte) bool {
	inserted := true
	var current Ref1 = t.buffer
	loop: for {
		switch n := current.(type) {
		case nil:
			break loop
		case *Leaf1:
			if bytes.Equal(key, n.key) {
				if bytes.Equal(value, n.nvalue(t)) {
					return false
				} else {
					inserted = false
				}
			}
			break loop
		case *Fork1:
			switch t.compare(key, n.left.getmax()) {
			case 0, -1:
				current = n.left
			case 1:
				current = n.right
			}
		case *Arrow1:
			root := t.deserialisePage(n.pageId)
			current = t.walkToArrowPoint(root, n.max, n.height)
		}
	}
	t.buffer = t.insert(t.buffer, key, value)
	return inserted
}

func (t *Avl1) insert(current Ref1, key, value []byte) Ref1 {
	trace := t.trace
	switch n := current.(type) {
	case nil:
		if trace {
			fmt.Printf("Inserting %s, on nil\n", key)
		}
		return &Leaf1{key: key, value: value, valueLen: uint32(len(value))}
	case *Arrow1:
		return t.insert(t.Peel(n), key, value)
	case *Leaf1:
		if trace {
			fmt.Printf("Inserting %s, on Leaf %s\n", key, n.key)
		}
		var newnode *Fork1
		switch t.compare(key, n.key) {
		case 0:
			n.value = value
			t.freeValueId(n.valueId)
			n.valueId = 0
			n.valueLen = uint32(len(value))
			return n
		case -1:
			newnode = &Fork1{max: n.key, height: 2, left: &Leaf1{key: key, value: value, valueLen: uint32(len(value))}, right: n}
		case 1:
			newnode = &Fork1{max: key, height: 2, left: n, right: &Leaf1{key: key, value: value, valueLen: uint32(len(value))}}
		}
		return newnode
	case *Fork1:
		c := t.compare(key, n.left.getmax())
		if trace {
			fmt.Printf("Inserting %s, on node %s, height %d\n", key, n.max, n.height)
		}
		// Prepare it for the next iteraion
		switch c {
		case 0, -1:
			n.left = t.insert(n.left, key, value)
		case 1:
			n.right = t.insert(n.right, key, value)
			n.max = n.right.getmax()
		}
		lHeight := n.left.getheight()
		rHeight := n.right.getheight()
		n.height = 1 + maxu32(lHeight, rHeight)
		if rHeight > lHeight {
			if rHeight - lHeight > 1 {
				// nr is a Fork, because its height is at least 3
				nr := t.Peel(n.right).(*Fork1)
				if nr.right.getheight() >= nr.left.getheight() {
					if trace {
						fmt.Printf("Single rotation from right to left, n %s %d, nr %s %d\n",
							n.nkey(), n.getheight(), nr.nkey(), nr.getheight())
					}
					n.right = nr.left
					n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
					n.max = nr.left.getmax()
					nr.left = n
					nr.height = 1 + maxu32(nr.left.getheight(), nr.right.getheight())
					if trace {
						fmt.Printf("n %s %d, nr %s %d, nrl %s %d\n",
							n.nkey(), n.getheight(), nr.nkey(), nr.getheight(), nr.left.nkey(), nr.left.getheight())
					}
					return nr
				} else {
					if trace {
						fmt.Printf("Double rotation from right to left, n %s %d, nr %s %d, nrl %s %d\n",
							n.nkey(), n.getheight(), nr.nkey(), nr.getheight(), nr.left.nkey(), nr.left.getheight())
					}
					// height of nrl is more than height of nrr. nr has height of at least 3, therefore nrl has a height of at least 2
					// nrl is a Fork
					nrl := t.Peel(nr.left).(*Fork1)
					n.right = nrl.left
					n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
					n.max = nrl.left.getmax()
					nrl.left = n
					nr.left = nrl.right
					nr.height = 1 + maxu32(nr.left.getheight(), nr.right.getheight())
					nrl.right = nr
					nrl.height = 1 + maxu32(nrl.left.getheight(), nrl.right.getheight())
					nrl.max = nr.max
					return nrl
				}
			} else {
				return n
			}
		} else if lHeight - rHeight > 1 {
			nl := t.Peel(n.left).(*Fork1)
			if nl.left.getheight() >= nl.right.getheight() {
				if trace {
					fmt.Printf("Single rotation from left to right, n %s %d, nl %s %d\n",
						n.nkey(), n.getheight(), nl.nkey(), nl.getheight())
				}
				n.left = nl.right
				n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
				nl.right = n
				nl.height = 1 + maxu32(nl.left.getheight(), nl.right.getheight())
				nl.max = n.max
				if t.compare(key, nl.max) == 1 {
					nl.max = key
				}
				return nl
			} else {
				if trace {
					fmt.Printf("Double rotation from left to right, n %s %d, nl %s %d, nlr %s %d\n",
						n.nkey(), n.getheight(), nl.nkey(), nl.getheight(), nl.right.nkey(), nl.right.getheight())
				}
				nlr := t.Peel(nl.right).(*Fork1)
				n.left = nlr.right
				n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
				nlr.right = n
				nlr.max = n.max
				nl.right = nlr.left
				nl.height = 1 + maxu32(nl.left.getheight(), nl.right.getheight())
				nl.max = nlr.left.getmax()
				nlr.left = nl
				nlr.height = 1 + maxu32(nlr.left.getheight(), nlr.right.getheight())
				return nlr
			}
		} else {
			return n
		}
	}
	panic("")
}

func (t *Avl1) Delete(key []byte) bool {
	var current Ref1 = t.buffer
	loop: for {
		switch n := current.(type) {
		case nil:
			return false
		case *Leaf1:
			if bytes.Equal(key, n.key) {
				break loop
			} else {
				return false
			}
		case *Fork1:
			switch t.compare(key, n.left.getmax()) {
			case 0, -1:
				current = n.left
			case 1:
				current = n.right
			}
		case *Arrow1:
			root := t.deserialisePage(n.pageId)
			current = t.walkToArrowPoint(root, n.max, n.height)
			if current.getheight() != n.height {
				panic(fmt.Sprintf("deseailised size %d, arrow height %d", current.getheight(), n.height))
			}
		}
	}
	t.buffer = t.delete(t.buffer, key)
	return true
}

func (t *Avl1) delete(current Ref1, key []byte) Ref1 {
	trace := t.trace
	switch n := current.(type) {
	case nil:
		panic("nil")
	case *Arrow1:
		return t.delete(t.Peel(n), key)
	case *Leaf1:
		// Assuming that key is equal to n.key
		if trace {
			fmt.Printf("Deleting on leaf %s\n", n.nkey())
		}
		t.freeValueId(n.valueId)
		return nil
	case *Fork1:
		c := t.compare(key, n.left.getmax())

		// Special cases when both right and left are leaves (simple of lobed)
		switch nl := t.Peek(n.left).(type) {
		case *Leaf1:
			switch nr := t.Peek(n.right).(type) {
			case *Leaf1:
				if trace {
					fmt.Printf("Special case: Leaf, Leaf\n")
				}
				nl = t.Peel(n.left).(*Leaf1)
				nr = t.Peel(n.right).(*Leaf1)
				switch c {
				case 0, -1:
					t.freeValueId(nl.valueId)
					return nr
				case 1:
					t.freeValueId(nr.valueId)
					return nl
				}
				panic("")
			}
		}
		if trace {
			fmt.Printf("Deleting %s, on node %s, height %d\n", key, n.max, n.height)
		}
		switch c {
		case 0, -1:
			n.left = t.delete(n.left, key)
			if n.left == nil {
				return n.right
			}
		case 1:
			n.right = t.delete(n.right, key)
			if n.right == nil {
				return n.left
			}
			n.max = n.right.getmax()
		}
		lHeight := n.left.getheight()
		rHeight := n.right.getheight()
		n.height = 1 + maxu32(lHeight, rHeight)
		if rHeight > lHeight {
			if rHeight - lHeight > 1 {
				// nr is a Fork, because its height is at least 3
				nr := t.Peel(n.right).(*Fork1)
				if nr.right.getheight() >= nr.left.getheight() {
					if trace {
						fmt.Printf("Single rotation from right to left, n %s, nr %s\n",
							n.nkey(), nr.nkey())
					}
					n.right = nr.left
					n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
					n.max = nr.left.getmax()
					nr.left = n
					nr.height = 1 + maxu32(nr.left.getheight(), n.right.getheight())
					return nr
				} else {
					if trace {
						fmt.Printf("Double rotation from right to left, n %s, nr %s\n",
							n.nkey(), nr.nkey())
					}
					// height of nrl is more than height of nrr. nr has height of at least 3, therefore nrl has a height of at least 2
					// nrl is a Fork
					nrl := t.Peel(nr.left).(*Fork1)
					n.right = nrl.left
					n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
					n.max = nrl.left.getmax()
					nrl.left = n
					nr.left = nrl.right
					nr.height = 1 + maxu32(nr.left.getheight(), nr.right.getheight())
					nrl.right = nr
					nrl.height = 1 + maxu32(nrl.left.getheight(), nrl.right.getheight())
					nrl.max = nr.max
					return nrl
				}
			} else {
				return n
			}
		} else if lHeight - rHeight > 1 {
			nl := t.Peel(n.left).(*Fork1)
			if nl.left.getheight() >= nl.right.getheight() {
				if trace {
					fmt.Printf("Single rotation from left to right, n %s, nl %s\n",
						n.nkey(), nl.nkey())
				}
				n.left = nl.right
				n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
				nl.right = n
				nl.height = 1 + maxu32(nl.left.getheight(), nl.right.getheight())
				nl.max = n.max
				return nl
			} else {
				if trace {
					fmt.Printf("Double rotation from left to right, n %s, nl %s, nlr %s\n",
						n.nkey(), nl.nkey(), nl.right.nkey())
				}
				nlr := t.Peel(nl.right).(*Fork1)
				n.left = nlr.right
				n.height = 1 + maxu32(n.left.getheight(), n.right.getheight())
				nlr.right = n
				nlr.max = n.max
				nl.right = nlr.left
				nl.height = 1 + maxu32(nl.left.getheight(), nl.right.getheight())
				nl.max = nlr.left.getmax()
				nlr.left = nl
				nlr.height = 1 + maxu32(nlr.left.getheight(), nlr.right.getheight())
				return nlr
			}
		} else {
			return n
		}
	}
	panic("")
}

func (t *Avl1) Dot() *graphContext {
	ctx := &graphContext{}
	t.buffer.dot(t, ctx, "b")
	return ctx
}

func (a *Arrow1) dot(t *Avl1, ctx *graphContext, path string) {
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: path,
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel(fmt.Sprintf("P.%d", a.pageId), 16, "sans-serif")
	gn.Label += mkLabel(string(a.max), 16, "sans-serif")
	gn.Label += mkLabel(fmt.Sprintf("%d", a.height), 10, "sans-serif")
	ctx.Nodes = append(ctx.Nodes, gn)
}

func (l *Leaf1) dot(t *Avl1, ctx *graphContext, path string) {
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: path,
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel(string(l.nkey()), 16, "sans-serif")
	gn.Label += mkLabel(string(l.nvalue(t)), 10, "sans-serif")
	ctx.Nodes = append(ctx.Nodes, gn)
}

func (f *Fork1) dot(t *Avl1, ctx *graphContext, path string) {
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: path,
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel(string(f.max), 16, "sans-serif")
	gn.Label += mkLabel(fmt.Sprintf("%d", f.height), 10, "sans-serif")
	ctx.Nodes = append(ctx.Nodes, gn)
	leftPath := fmt.Sprintf("%p", f.left)
	f.left.dot(t, ctx, leftPath)
	ctx.Edges = append(ctx.Edges, &graphEdge{
		From: path,
		To: leftPath,
	})
	rightPath := fmt.Sprintf("%p%T", f.right, f.right)
	f.right.dot(t, ctx, rightPath)
	ctx.Edges = append(ctx.Edges, &graphEdge{
		From: path,
		To: rightPath,
	})
}

func (l *Leaf1) heightsCorrect(path string) (uint32, bool) {
	return 1, true
}

func (f *Fork1) heightsCorrect(path string) (uint32, bool) {
	leftHeight, leftCorrect := f.left.heightsCorrect(path + "l")
	rightHeight, rightCorrect := f.right.heightsCorrect(path + "r")
	height, correct := 1+maxu32(leftHeight, rightHeight), leftCorrect&&rightCorrect&&(1+maxu32(leftHeight, rightHeight) == f.height)
	if !correct {
		fmt.Printf("At path %s, key %s, expected %d, got %d\n", path, f.max, height, f.height)
	}
	return height, correct
}

func (l *Leaf1) balanceCorrect() bool {
	return true
}

func (f *Fork1) balanceCorrect() bool {
	lHeight := f.left.getheight()
	rHeight := f.right.getheight()
	var balanced bool
	if rHeight >= lHeight {
		balanced = (rHeight - lHeight) < 2
	} else {
		balanced = (lHeight - rHeight) < 2
	}
	return balanced && f.left.balanceCorrect() && f.right.balanceCorrect()
}

func (a *Arrow1) heightsCorrect(path string) (uint32, bool) {
	return a.height, true
}

func (a *Arrow1) balanceCorrect() bool {
	return true
}

func (t *Avl1) pageSize(keyCount, pageCount, keyBodySize, valBodySize, structBits uint32, prefix []byte) uint32 {
	nodeCount := keyCount + pageCount
	prefixLen := uint32(len(prefix))

	return 4 /* nodeCount */ +
		4*((nodeCount+31)/32) /* pageBits */ +
		4 /* prefixOffset */ +
		4*nodeCount + 4 /* key header */ +
		(12+t.hashLength)*pageCount /* arrow header */ +
		4*keyCount /* value header */ +
		4*((structBits+31)/32) /* structBits */ +
		prefixLen +
		keyBodySize - nodeCount*prefixLen /* Discount bodySize using prefixLen */ +
		valBodySize
}

// Split current buffer into pages and commit them
func (t *Avl1) Commit() uint64 {
	if t.buffer == nil {
		return 0
	}
	startCounter := t.commitedCounter
	prefix, keyCount, pageCount, keyBodySize, valBodySize, structBits := t.buffer.serialisePass1(t)
	id := t.commitPage(t.buffer, prefix, keyCount, pageCount, keyBodySize, valBodySize, structBits)
	t.versions[t.currentVersion] = id
	if t.verFile != nil {
		var verdata [8]byte
		binary.BigEndian.PutUint64(verdata[:], uint64(id))
		t.verFile.WriteAt(verdata[:], int64(t.currentVersion)*int64(8))
	}
	t.currentVersion++
	t.buffer = &Arrow1{pageId: id, height: t.buffer.getheight(), max: t.buffer.getmax()}
	return t.commitedCounter - startCounter
}

func (t *Avl1) commitPage(r Ref1, prefix []byte, keyCount, pageCount, keyBodySize, valBodySize, structBits uint32) PageID {
	trace := t.trace
	if trace {
		fmt.Printf("commitPage %s\n", r.getmax())
	}
	nodeCount := keyCount + pageCount
	size := t.pageSize(keyCount, pageCount, keyBodySize, valBodySize, structBits, prefix)
	data := make([]byte, PageSize)
	offset := uint32(0)
	binary.BigEndian.PutUint32(data[offset:], nodeCount)
	offset += 4
	pageBitsOffset := offset
	offset += 4*((nodeCount+31)/32)
	prefixOffsetOffset := offset
	offset += 4
	keyHeaderOffset := offset
	offset += 4*nodeCount /* key offset per node */ + 4 /* end key offset */
	arrowHeaderOffset := offset
	offset += (12+t.hashLength)*pageCount /* (pageId, size - minSize, hash) per arrow */
	valueHeaderOffset := offset
	offset += 4*keyCount /* value length per leaf */
	structBitsOffset := offset
	if trace {
		fmt.Printf("StructBitsOffset %d\n", structBitsOffset)
	}
	offset += 4*((structBits+31)/32) // Structure encoding
	// key prefix begins here
	binary.BigEndian.PutUint32(data[prefixOffsetOffset:], uint32(offset))
	copy(data[offset:], prefix)
	offset += uint32(len(prefix))
	keyBodyOffset := offset
	offset += keyBodySize - nodeCount*uint32(len(prefix))
	valBodyOffset := offset

	var nodeIndex uint32
	var structBit uint32
	if trace {
		fmt.Printf("valueHeaderOffset %d\n", valueHeaderOffset)
	}
	if trace {
		fmt.Printf("valBodyOffset %d\n", valBodyOffset)
	}
	t.serialisePass2(r, data, len(prefix), pageBitsOffset, structBitsOffset,
		&nodeIndex, &structBit, &keyHeaderOffset, &arrowHeaderOffset, &valueHeaderOffset, &keyBodyOffset, &valBodyOffset)
	// end key offset
	if trace {
		fmt.Printf("valBodyOffset %d\n", keyBodyOffset)
	}
	binary.BigEndian.PutUint32(data[keyHeaderOffset:], keyBodyOffset)
	if valBodyOffset != size {
		panic(fmt.Sprintf("valBodyOffset %d (%d) != size %d, valBodySize %d, nodeCount %d, len(prefix): %d",
			valBodyOffset, offset, size, valBodySize, nodeCount, len(prefix)))
	}
	if nodeIndex != nodeCount {
		panic("n != nodeCount")
	}
	if structBit != structBits {
		panic("sb != structBits")
	}
	id := t.nextPageId()
	if t.trace {
		fmt.Printf("Committed page %d, nodeCount %d, prefix %s\n", id, nodeCount, prefix)
	}
	if t.pageFile != nil {
		t.pageFile.WriteAt(data, int64(id)*int64(PageSize))
	} else {
		t.pageMap[id] = data
	}
	t.commitedCounter++
	t.pageSpace += uint64(size)
	return id
}

// Computes all the dynamic parameters that allow calculation of the page length and pre-allocation of all buffers
func (l *Leaf1) serialisePass1(t *Avl1) (prefix []byte, keyCount, pageCount, keyBodySize, valBodySize, structBits uint32) {
	keyCount = 1
	keyBodySize = uint32(len(l.key))
	if l.valueLen > InlineValueMax {
		valBodySize = 8 + HashLength // Size of value id + valueHash
	} else {
		valBodySize = l.valueLen
	}
	structBits = 1 // one bit per leaf
	prefix = l.key
	return
}

func commonPrefix(p1, p2 []byte) []byte {
	if p2 == nil {
		return p1
	}
	if p1 == nil {
		return p2
	}
	var i int
	for i = 0; i < len(p1) && i < len(p2) && p1[i] == p2[i]; i++ {}
	return p1[:i]
}

func (f *Fork1) serialisePass1(t *Avl1) (prefix []byte, keyCount, pageCount, keyBodySize, valBodySize, structBits uint32) {
	prefixL, keyCountL, pageCountL, keyBodySizeL, valBodySizeL, structBitsL := f.left.serialisePass1(t)
	prefixR, keyCountR, pageCountR, keyBodySizeR, valBodySizeR, structBitsR := f.right.serialisePass1(t)
	// Fork and both children fit in the page
	{
		keyCountLFR := keyCountL+keyCountR
		pageCountLFR := pageCountL+pageCountR
		keyBodySizeLFR := keyBodySizeL+keyBodySizeR
		valBodySizeLFR := valBodySizeL+valBodySizeR
		structBitsLFR := structBitsL+structBitsR+2 // 2 bits for the fork
		prefixLFR := commonPrefix(prefixL, prefixR)
		sizeLFR := t.pageSize(keyCountLFR, pageCountLFR, keyBodySizeLFR, valBodySizeLFR, structBitsLFR, prefixLFR)
		if sizeLFR < PageSize {
			return prefixLFR, keyCountLFR, pageCountLFR, keyBodySizeLFR, valBodySizeLFR, structBitsLFR
		}
	}
	// Choose the biggest child and make a page out of it
	sizeL := t.pageSize(keyCountL, pageCountL, keyBodySizeL, valBodySizeL, structBitsL, prefixL)
	sizeR := t.pageSize(keyCountR, pageCountR, keyBodySizeR, valBodySizeR, structBitsR, prefixR)
	if sizeL > sizeR {
		lid := t.commitPage(f.left, prefixL, keyCountL, pageCountL, keyBodySizeL, valBodySizeL, structBitsL)
		lArrow := &Arrow1{pageId: lid, height: f.left.getheight(), max: f.left.getmax()}
		f.left = lArrow
		// Check if the fork and the right child still fit into a page
		pageCountFR := pageCountR+1 // 1 for the left arrow
		keyBodySizeFR := keyBodySizeR+uint32(len(lArrow.max))
		structBitsFR := structBitsR+3 // 2 for the fork and 1 for the left arrow
		prefixFR := commonPrefix(prefixR, lArrow.max)
		sizeFR := t.pageSize(keyCountR, pageCountFR, keyBodySizeFR, valBodySizeR, structBitsFR, prefixFR)
		if sizeFR < PageSize {
			return prefixFR, keyCountR, pageCountFR, keyBodySizeFR, valBodySizeR, structBitsFR
		} else {
			// Have to commit right child too
			rid := t.commitPage(f.right, prefixR, keyCountR, pageCountR, keyBodySizeR, valBodySizeR, structBitsR)
			rArrow := &Arrow1{pageId: rid, height: f.right.getheight(), max: f.right.getmax()}
			f.right = rArrow
			return commonPrefix(rArrow.max, lArrow.max), 0, 2, uint32(len(lArrow.max))+uint32(len(rArrow.max)), 0, 4 /* 2 bits for arrows, 2 for the fork */
		}
	} else {
		rid := t.commitPage(f.right, prefixR, keyCountR, pageCountR, keyBodySizeR, valBodySizeR, structBitsR)
		rArrow := &Arrow1{pageId: rid, height: f.right.getheight(), max: f.right.getmax()}
		f.right = rArrow
		// Check if the fork and the let child still fit into a page
		pageCountFL := pageCountL+1 // 1 for the left arrow
		keyBodySizeFL := keyBodySizeL+uint32(len(rArrow.max))
		structBitsFL := structBitsL+3 // 2 for the fork and 1 for the left arrow
		prefixFL := commonPrefix(prefixL, rArrow.max)
		sizeFL := t.pageSize(keyCountL, pageCountFL, keyBodySizeFL, valBodySizeL, structBitsFL, prefixFL)
		if sizeFL < PageSize {
			return prefixFL, keyCountL, pageCountFL, keyBodySizeFL, valBodySizeL, structBitsFL
		} else {
			// Have to commit left child too
			lid := t.commitPage(f.left, prefixL, keyCountL, pageCountL, keyBodySizeL, valBodySizeL, structBitsL)
			lArrow := &Arrow1{pageId: lid, height: f.left.getheight(), max: f.left.getmax()}
			f.left = lArrow
			return commonPrefix(rArrow.max, lArrow.max), 0, 2, uint32(len(lArrow.max))+uint32(len(rArrow.max)), 0, 4 /* 2 bits for arrows, 2 for the fork */
		}
	}
}

func (a *Arrow1) serialisePass1(t *Avl1) (prefix []byte, keyCount, pageCount, keyBodySize, valBodySize, structBits uint32) {
	pageCount = 1
	structBits = 1 // one bit for page reference
	keyBodySize = uint32(len(a.max))
	prefix = a.max
	return
}

func (t *Avl1) serialiseKey(key, data []byte, keyHeaderOffset, keyBodyOffset *uint32) {
	binary.BigEndian.PutUint32(data[*keyHeaderOffset:], *keyBodyOffset)
	*keyHeaderOffset += 4
	copy(data[*keyBodyOffset:], key)
	*keyBodyOffset += uint32(len(key))
}

func (t *Avl1) serialiseVal(value []byte, valueId uint64, valueLen uint32, data []byte, valueHeaderOffset, valBodyOffset *uint32) uint64 {
	binary.BigEndian.PutUint32(data[*valueHeaderOffset:], valueLen)
	*valueHeaderOffset += 4
	if valueLen > InlineValueMax {
		var valueHash Hash
		if valueId == 0 {
			valueId = t.nextValueId()
			//valueHash = sha256.Sum256(value)
			//t.valueHashes[valueId] = valueHash
			t.addValue(valueId, value)
		} else {
			valueHash = t.valueHashes[valueId]
		}
		binary.BigEndian.PutUint64(data[*valBodyOffset:], valueId)
		*valBodyOffset += 8
		copy(data[*valBodyOffset:], valueHash[:])
		*valBodyOffset += HashLength
	} else {
		copy(data[*valBodyOffset:], value)
		*valBodyOffset += valueLen
	}
	return valueId
}

func (t *Avl1) serialisePass2(r Ref1, data []byte, prefixLen int, pageBitsOffset, structBitsOffset uint32,
	nodeIndex, structBit, keyHeaderOffset, arrowHeaderOffset, valueHeaderOffset, keyBodyOffset, valBodyOffset *uint32) {

	switch r := r.(type) {
	case *Leaf1:
		t.serialiseKey(r.key[prefixLen:], data, keyHeaderOffset, keyBodyOffset)
		r.valueId = t.serialiseVal(r.value, r.valueId, r.valueLen, data, valueHeaderOffset, valBodyOffset)
		// Update page bits
		*nodeIndex++
		// Update struct bits
		data[structBitsOffset+(*structBit>>3)] |= (uint8(1)<<(*structBit&7))
		*structBit++
	case *Fork1:
		// Write opening parenthesis "0" (noop)
		t.serialisePass2(r.left, data, prefixLen, pageBitsOffset, structBitsOffset,
			nodeIndex, structBit, keyHeaderOffset, arrowHeaderOffset, valueHeaderOffset, keyBodyOffset, valBodyOffset)
		// Update struct bit
		*structBit++
		t.serialisePass2(r.right, data, prefixLen, pageBitsOffset, structBitsOffset,
			nodeIndex, structBit, keyHeaderOffset, arrowHeaderOffset, valueHeaderOffset, keyBodyOffset, valBodyOffset)
		// Write closing parenthesis "1"
		data[structBitsOffset+(*structBit>>3)] |= (uint8(1)<<(*structBit&7))
		*structBit++
	case *Arrow1:
		binary.BigEndian.PutUint64(data[*arrowHeaderOffset:], uint64(r.pageId))
		*arrowHeaderOffset += 8
		binary.BigEndian.PutUint32(data[*arrowHeaderOffset:], uint32(r.height))
		*arrowHeaderOffset += 4
		t.serialiseKey(r.max[prefixLen:], data, keyHeaderOffset, keyBodyOffset)
		// TODO: write page hash
		*arrowHeaderOffset += t.hashLength
		// Update page bit
		data[pageBitsOffset+(*nodeIndex>>3)] |= (uint8(1)<<(*nodeIndex&7))
		*nodeIndex++
		// Write closing parenthesis "1"
		data[structBitsOffset+(*structBit>>3)] |= (uint8(1)<<(*structBit&7))
		*structBit++
	}
}

func (t *Avl1) deserialiseKey(data []byte, keyHeaderOffset *uint32, prefix []byte) []byte {
	keyStart := binary.BigEndian.Uint32(data[*keyHeaderOffset:])
	keyEnd := binary.BigEndian.Uint32(data[*keyHeaderOffset+4:]) // Start of the next key (or end offset)
	*keyHeaderOffset += 4
	return append(prefix, data[keyStart:keyEnd]...)
}

func (t *Avl1) deserialiseVal(data []byte, valueHeaderOffset, valBodyOffset *uint32) (value []byte, valueId uint64, valLen uint32) {
	valLen = binary.BigEndian.Uint32(data[*valueHeaderOffset:])
	*valueHeaderOffset += 4
	if valLen > InlineValueMax {
		valueId = binary.BigEndian.Uint64(data[*valBodyOffset:])
		*valBodyOffset += 8
		// Read the hash here
		*valBodyOffset += HashLength
	} else {
		value = make([]byte, valLen)
		copy(value, data[*valBodyOffset:])
		*valBodyOffset += valLen
	}
	return
}

func (t *Avl1) deserialisePage(pageId PageID) Ref1 {
	trace := t.trace
	if root, ok := t.pageCache.Get(pageId); ok {
		return root.(Ref1)
	}
	if trace {
		fmt.Printf("Deserialising page %d\n", pageId)
	}
	var data []byte
	if t.pageFile != nil {
		data = make([]byte, PageSize)
		if _, err := t.pageFile.ReadAt(data, int64(pageId)*int64(PageSize)); err != nil {
			panic(err)
		}
	} else {
		data = t.pageMap[pageId]
	}
	if data == nil {
		return nil
	}
	offset := uint32(0)
	// read node count
	nodeCount := binary.BigEndian.Uint32(data[offset:])
	if nodeCount == 0 {
		return nil
	}
	offset += 4
	pageBitsOffset := offset
	// Calculate number of pages
	var pageCount uint32
	pageBitsLen := 4*((nodeCount+31)/32)
	for i := uint32(0); i < pageBitsLen; i += 4 {
		pageCount += uint32(bits.OnesCount32(binary.BigEndian.Uint32(data[pageBitsOffset+i:])))
	}
	offset += pageBitsLen
	keyCount := nodeCount - pageCount
	prefixOffset := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	keyHeaderOffset := offset
	prefixLen := binary.BigEndian.Uint32(data[keyHeaderOffset:]) - prefixOffset
	prefix := make([]byte, prefixLen, prefixLen) // To prevent this to be appended to
	copy(prefix, data[prefixOffset:])
	offset += 4*nodeCount
	valBodyOffset := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	arrowHeaderOffset := offset
	offset += (12+t.hashLength)*pageCount
	valueHeaderOffset := offset
	offset += 4*keyCount
	structBitsOffset := offset
	if trace {
		fmt.Printf("StructBitsOffset %d\n", structBitsOffset)
	}
	if trace {
		fmt.Printf("valBodyOffset %d\n", valBodyOffset)
	}
	if trace {
		fmt.Printf("valueHeaderOffset %d\n", valueHeaderOffset)
	}
	var forkStack []Ref1
	var forkStackTop int
	var nodeIndex uint32
	var structBit uint32
	var noLeaf bool
	for nodeIndex < nodeCount {
		isPage := (data[pageBitsOffset+(nodeIndex>>3)] & (uint8(1)<<(nodeIndex&7))) != 0
		sbit := (data[structBitsOffset+(structBit>>3)] & (uint8(1)<<(structBit&7))) != 0
		// Interpret the structural bit
		var r Ref1
		if noLeaf {
			if sbit {
				forkStackTop--
				x := forkStack[forkStackTop]
				y := forkStack[forkStackTop-1].(*Fork1)
				y.right = x
				y.height = maxu32(y.height, 1+x.getheight())
				y.max = x.getmax()
				noLeaf = true
			} else {
				x := forkStack[forkStackTop-1]
				y := &Fork1{left: x, height: 1+x.getheight()}
				forkStack[forkStackTop-1] = y
				noLeaf = false
			}
		} else if sbit {
			if isPage {
				id := PageID(binary.BigEndian.Uint64(data[arrowHeaderOffset:]))
				arrowHeaderOffset += 8
				height := binary.BigEndian.Uint32(data[arrowHeaderOffset:])
				arrowHeaderOffset += 4
				// TODO read the page hash
				arrowHeaderOffset += t.hashLength
				max := t.deserialiseKey(data, &keyHeaderOffset, prefix)
				arrow := &Arrow1{pageId: id, height: height, max: max}
				r = arrow
				if trace {
					fmt.Printf("Deserialised arrow max %s, height %d\n", arrow.max, arrow.height)
				}
			} else {
				l := &Leaf1{}
				l.key = t.deserialiseKey(data, &keyHeaderOffset, prefix)
				l.value, l.valueId, l.valueLen = t.deserialiseVal(data, &valueHeaderOffset, &valBodyOffset)
				if trace {
					fmt.Printf("Deserialised leaf key %s, value %s, valueId %d, valueLen %d\n", l.key, l.nvalue(t), l.valueId, l.valueLen)
				}
				r = l
			}
			nodeIndex++
			noLeaf = true
		} else {
			panic("Lobed leaf encoding")
		}
		if r != nil {
			// Push onto the stack
			if forkStackTop >= len(forkStack) {
				forkStack = append(forkStack, r)
			} else {
				forkStack[forkStackTop] = r
			}
			forkStackTop++
		}
		structBit++
	}
	for forkStackTop > 1 {
		forkStackTop--
		x := forkStack[forkStackTop]
		y := forkStack[forkStackTop-1].(*Fork1)
		y.right = x
		y.height = maxu32(y.height, 1+x.getheight())
		y.max = x.getmax()
	}
	t.pageCache.Add(pageId, forkStack[0])
	return forkStack[0]
}

// Checks whether WBT without pages is equivalent to one with pages
func equivalent11(t *Avl1, path string, r1 Ref1, r2 Ref1) bool {
	switch r2 := r2.(type) {
	case nil:
		if r1 != nil {
			fmt.Printf("At path %s, expected n1 nil, but it was %s\n", path, r1.nkey())
			return false
		}
		return true
	case *Leaf1:
		if l1, ok := r1.(*Leaf1); ok {
			if !bytes.Equal(l1.key, r2.key) {
				fmt.Printf("At path %s, l1.key %s, r2.key %s\n", path, l1.nkey(), r2.nkey())
				return false
			}
			if !bytes.Equal(l1.nvalue(t), r2.nvalue(t)) {
				fmt.Printf("At path %s, l1.value %s, r2.value %s\n", path, l1.nvalue(t), r2.nvalue(t))
				return false
			}
		} else {
			fmt.Printf("At path %s, expected leaf, got %T\n", path, r1)
			return false
		}
		return true
	case *Fork1:
		if t.trace {
			fmt.Printf("equivalent11 path %s, at fork %s, height %d\n", path, r2.max, r2.height)
		}
		if f1, ok := r1.(*Fork1); ok {
			if !bytes.Equal(f1.max, r2.max) {
				fmt.Printf("At path %s, f1.max %s, r2.max %s\n", path, f1.max, r2.max)
				return false
			}
			if f1.height != r2.height {
				fmt.Printf("At path %s, f1.height %d, r2.height %d\n", path, f1.height, r2.height)
				return false
			}
			eqL := equivalent11(t, path + "l", f1.left, r2.left)
			eqR := equivalent11(t, path + "r", f1.right, r2.right)
			return eqL && eqR
		}
	case *Arrow1:
		if t.trace {
			fmt.Printf("equivalent11 path %s, at arrow P.%d[%s], height %d\n", path, r2.pageId, r2.max, r2.height)
		}
		if !bytes.Equal(r1.getmax(), r2.max) {
			fmt.Printf("At path %s, r1.max %s, r2(arrow).max %s\n", path, r1.getmax(), r2.max)
			return false
		}
		if r1 != nil && r2 != nil && r1.getheight() != r2.height {
			fmt.Printf("At path %s, r1.height %d, r2(arrow).height %d\n", path, r1.getheight(), r2.height)
			return false
		}
		root := t.deserialisePage(r2.pageId)
		point := t.walkToArrowPoint(root, r2.max, r2.height)
		return equivalent11(t, path, r1, point)
	}
	return false
}

func (t *Avl1) PrintStats() {
	var totalValueLens uint64
	if t.valueFile == nil {
		for _, l := range t.valueLens {
			totalValueLens += uint64(l)
		}
	} else {
		totalValueLens = t.maxValueId
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Total pages: %d, Mb %0.3f, page space: Mb %0.3f large vals: %d, Mb %0.3f, mem alloc %0.3fMb, sys %0.3fMb\n",
		t.maxPageId,
		float64(t.maxPageId)*float64(PageSize)/1024.0/1024.0,
		float64(t.pageSpace)/1024.0/1024.0,
		len(t.valueLens),
		float64(totalValueLens)/1024.0/1024.0,
		float64(m.Alloc)/1024.0/1024.0,
		float64(m.Alloc)/1024.0/1024.0,
		)
}