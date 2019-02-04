package avl

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"encoding/binary"
	"runtime"
	"os"
	"sort"
	"github.com/petar/GoLLRB/llrb"
	lru "github.com/hashicorp/golang-lru"
)

// AVL+ organised into pages, with history

type Avl3 struct {
	currentVersion Version
	lastFlushed    Version
	trace bool
	maxPageId PageID
	pageMap map[PageID][]byte // Pages by pageId
	maxValueId uint64
	valueMap map[uint64][]byte // Large values by valueId
	valueHashes map[uint64]Hash // Value hashes
	valueLens map[uint64]uint32 // Value lengths
	freelist []PageID // Free list of pages
	prevRoot Ref3 // Root of the previous version that is getting "peeled off" from the current version
	root Ref3 // Root of the current update buffer
	versions map[Version]Version3 // root pageId for a version
	modifiedPages map[PageID]*ModPage
	repinnedPages map[PageID]struct{} // set of pages that have been repinned to prevRoot, and cannot be released
	commitedCounter uint64
	pageSpace uint64
	solidAlloc uint64
	solidUsed uint64
	solidRefs map[int]int // Number of pages with certain number of refs
	solidTreeSizes map[uint32]int // Number of trees of certain size
	solidPinned int
	solidFree int
	pageFile, valueFile, verFile *os.File
	hashLength uint32
	compare func([]byte, []byte) int
	pageCache *lru.Cache
	pageCacheHits, pageCacheMisses int
	commitMap map[PageID][]byte
}

type ModPage struct {
	roots []Ref3
	pins []Ref3
}

type Version3 struct {
	pageId PageID
	treeIndex uint32
}

func NewAvl3() *Avl3 {
	t := &Avl3{
		pageMap: make(map[PageID][]byte),
		valueMap: make(map[uint64][]byte),
		valueHashes: make(map[uint64]Hash),
		valueLens: make(map[uint64]uint32),
		versions: make(map[Version]Version3),
		modifiedPages: make(map[PageID]*ModPage),
		repinnedPages: make(map[PageID]struct{}),
		solidRefs: make(map[int]int),
		solidTreeSizes: make(map[uint32]int),
		commitMap: make(map[PageID][]byte),
	}
	t.hashLength = 32
	t.compare = bytes.Compare
	var err error
	t.pageCache, err = lru.New(500000)
	if err != nil {
		panic(err)
	}
	return t
}

func (t *Avl3) SetCompare(c func([]byte, []byte) int) {
	t.compare = c
}

func (t *Avl3) walkToArrowPoint(r Ref3, key []byte, height uint32) (point Ref3, found bool, err error) {
	if t.trace {
		fmt.Printf("walkToArrowPoint arrow: %s %d, key: %s %d\n", r.getmax(), r.getheight(), key, height)
	}
	current := r
	for {
		switch n := current.(type) {
		case nil:
			return nil, false, nil
		case *Leaf3:
			if height != 1 || t.compare(key, n.key) != 0 {
				return nil, false, nil
			}
			return n, true, nil
		case *Fork3:
			if t.trace {
				fmt.Printf("walkToArrowPoint(Fork3) %s %d, key %s %d\n", n.max, n.height, key, height)
			}
			if n.height < height {
				return nil, false, nil
			} else if n.height > height {
				switch t.compare(key, n.left.getmax()) {
				case -1, 0:
					current = n.left
				case 1:
					current = n.right
				}
			} else {
				if t.compare(key, n.max) != 0 {
					return nil, false, nil
				}
				return n, true, nil
			}
		case *Arrow3:
			return nil, false, fmt.Errorf("Arrow3 P.%d with height %d max %x, expected height %d key %x", n.pageId, n.height, n.max, height, key)
		}
	}
}

func (t *Avl3) UseFiles(pageFileName, valueFileName, verFileName string, read bool) {
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
	var verdata [12]byte
	var lastVersion Version3
	for n, _ := t.verFile.ReadAt(verdata[:], int64(t.currentVersion)*int64(12)); n >0; n, _ = t.verFile.ReadAt(verdata[:], int64(t.currentVersion)*int64(12)) {
		lastVersion.pageId = PageID(binary.BigEndian.Uint64(verdata[:]))
		lastVersion.treeIndex = binary.BigEndian.Uint32(verdata[8:])
		t.versions[t.currentVersion] = lastVersion
		t.currentVersion++
	}
	if t.currentVersion > 0 {
		t.maxPageId = lastVersion.pageId
		fmt.Printf("Deserialising page %d\n", lastVersion.pageId)
		root := t.deserialisePage(lastVersion.pageId, nil, 0, lastVersion.treeIndex, false)
		prevRootArrow := &Arrow3{height: root.getheight(), max: root.getmax()}
		t.root = &Arrow3{pageId: lastVersion.pageId, height: root.getheight(), max: root.getmax(), arrow: prevRootArrow}
		t.prevRoot = prevRootArrow
	}
}

func (t *Avl3) Close() {
	t.checkFlush(true)
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

func (t *Avl3) CurrentVersion() Version {
	return t.currentVersion
}

func (t *Avl3) SetHashLength(hashLength uint32) {
	t.hashLength = hashLength
}

// Common fields for a tree node
type Node3 struct {
	arrow *Arrow3 // Pointer back to the arrow that points to this fork
	pinnedPageId PageID // 0 if node is not pinned to a page, otherwise the page Id where this node should stay
}

type Leaf3 struct {
	Node3
	key, value []byte
	valueId uint64
	valueLen uint32
}

type Fork3 struct {
	Node3
	height uint32 // Height of the entire subtree rooted at this node, including node itself
	left, right Ref3
	max []byte // Largest key in the subtree
}

type Arrow3 struct {
	arrow *Arrow3
	pageId PageID // Connection of this page to the page on the disk. Normally pageId corresponds to offset pageId*PageSize in the database file
	height uint32 // Height of the entire subtree rooted at this page
	max []byte
	parent *Fork3 // Fork that have this arrow as either left of right branch
	parentC int   // -1 if the parent has this arrow as left branch, 1 if the parent has this arrow as right branch
}

// Reference can be either a WbtNode3, or WbtArrow3. The latter is used when the leaves of one page reference another page
type Ref3 interface {
	getheight() uint32
	nkey() []byte
	getmax() []byte
	dot(*Avl3, *graphContext, string)
	heightsCorrect(path string) (uint32, bool)
	balanceCorrect() bool
	prepare(t *Avl3, buffer *CommitBuffer, splitPages bool) Tree3
}

func (t *Avl3) nextPageId() PageID {
	if len(t.freelist) > 0 {
		nextId := t.freelist[len(t.freelist)-1]
		t.freelist = t.freelist[:len(t.freelist)-1]
		return nextId
	}
	t.maxPageId++
	return t.maxPageId
}

func (t *Avl3) freePageId(pageId PageID) {
	delete(t.pageMap, pageId)
	t.freelist = append(t.freelist, pageId)
}

func (t *Avl3) nextValueId() uint64 {
	return t.maxValueId + 1
}

func (t *Avl3) freeValueId(valueId uint64) {
	if valueId == 0 {
		return
	}
	delete(t.valueMap, valueId)
	delete(t.valueHashes, valueId)
	delete(t.valueLens, valueId)
}

func (t *Avl3) addValue(valueId uint64, value []byte) {
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

func (t *Avl3) SetTracing(tracing bool) {
	t.trace = tracing
}

func (l *Leaf3) getheight() uint32 {
	return 1
}

func (f *Fork3) getheight() uint32 {
	return f.height
}

func (a *Arrow3) getheight() uint32 {
	return a.height
}

func (l *Leaf3) nkey() []byte {
	return l.key
}

func (f *Fork3) nkey() []byte {
	return f.max
}

func (a *Arrow3) nkey() []byte {
	return a.max
}

func (l *Leaf3) getmax() []byte {
	return l.key
}

func (f *Fork3) getmax() []byte {
	return f.max
}

func (a *Arrow3) getmax() []byte {
	return a.max
}

func (l *Leaf3) nvalue(t *Avl3) []byte {
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

func (t *Avl3) Get(key []byte) ([]byte, bool) {
	trace := t.trace
	var current Ref3 = t.root
	for {
		switch n := current.(type) {
		case nil:
			return nil, false
		case *Leaf3:
			if trace {
				fmt.Printf("Get %s on leaf %s\n", key, n.key)
			}
			if t.compare(key, n.key) == 0 {
				return n.nvalue(t), true
			}
			return nil, false
		case *Fork3:
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
		case *Arrow3:
			current = t.deserialisePage(n.pageId, n.max, n.height, 0, false)
			if current == nil {
				panic(fmt.Sprintf("Arrow P.%d, %x %d\n", n.pageId, n.max, n.height))
			}
		}
	}
}

func (t *Avl3) IsLeaf(r Ref3) bool {
	current := r
	for {
		switch r := current.(type) {
		case nil:
			panic("nil")
		case *Arrow3:
			current = t.deserialisePage(r.pageId, r.max, r.height, 0, false)
			if current == nil {
				panic("")
			}
		case *Fork3:
			return false
		case *Leaf3:
			return true
		}
	}
	panic("")
}

func (t *Avl3) Peel(r Ref3, key []byte, ins int) Ref3 {
	current := r
	for {
		switch r := current.(type) {
		case nil:
			panic("nil")
		case *Arrow3:
			point := t.deserialisePage(r.pageId, r.max, r.height, 0, true)
			if point == nil {
				panic("")
			}
			if r.arrow != nil {
				if t.trace {
					fmt.Printf("Moving arrow P.%d[%s %d] over the arrow P.%d[%s %d]\n", r.arrow.pageId, r.arrow.max, r.arrow.height, r.pageId, r.max, r.height)
				}
				r.arrow.pageId = r.pageId
				switch n := point.(type) {
				case *Leaf3:
					n.arrow = r.arrow
				case *Fork3:
					n.arrow = r.arrow
				case *Arrow3:
					n.arrow = r.arrow
				}
				r.arrow = nil
			}
			current = point
		case *Fork3:
			return r
		case *Leaf3:
			return r
		}
	}
	panic("")
}

func (t *Avl3) moveArrowOverFork(a *Arrow3, f *Fork3, repinned map[PageID]struct{}) {
	if t.trace {
		fmt.Printf("Moving arrow P.%d[%s %d] over the fork %x\n", a.pageId, a.max, a.height, f.nkey())
	}
	var lArrow *Arrow3 = &Arrow3{pageId: a.pageId, parentC: -1, height: f.left.getheight(), max: f.left.getmax()}
	var rArrow *Arrow3 = &Arrow3{pageId: a.pageId, parentC: 1, height: f.right.getheight(), max: f.right.getmax()}
	if t.trace {
		fmt.Printf("Left arrow P.%d[%s %d], right arrow P.%d[%s %d]\n", lArrow.pageId, lArrow.max, lArrow.height, rArrow.pageId, rArrow.max, rArrow.height)
	}
	fork := &Fork3{max: f.max, height: f.height, left: lArrow, right: rArrow}
	lArrow.parent = fork
	rArrow.parent = fork
	f.arrow = nil
	switch n := f.left.(type) {
	case *Fork3:
		n.arrow = lArrow
	case *Leaf3:
		n.arrow = lArrow
	case *Arrow3:
		n.arrow = lArrow
		lArrow.pageId = n.pageId
	}
	switch n := f.right.(type) {
	case *Fork3:
		n.arrow = rArrow
	case *Leaf3:
		n.arrow = rArrow
	case *Arrow3:
		n.arrow = rArrow
		rArrow.pageId = n.pageId
	}
	if a.parent == nil {
		t.prevRoot = fork
	} else {
		switch a.parentC {
		case -1:
			a.parent.left = fork
		case 1:
			a.parent.right = fork
		}
	}
	// When the pinned node gets modified, the arrow pointing to it moves over the pinned node.
	// That causes the pinned node to get unpinned in the current state, but pinned in the previous
	// state.
	if f.pinnedPageId != 0 {
		if f.pinnedPageId == 167 {
			fmt.Printf("Repinned page from fork %d\n", f.pinnedPageId)
		}
		fork.pinnedPageId = f.pinnedPageId
		t.repinnedPages[f.pinnedPageId] = struct{}{}
		repinned[f.pinnedPageId] = struct{}{}
		f.pinnedPageId = 0
	}
}


func (t *Avl3) moveArrowOverLeaf(a *Arrow3, l *Leaf3, repinned map[PageID]struct{}) {
	if t.trace {
		fmt.Printf("Moving arrow P.%d[%s %d] over the leaf %s\n", a.pageId, a.max, a.height, l.nkey())
	}
	leaf := &Leaf3{key: l.key, value: l.nvalue(t)}
	leaf.valueLen = uint32(len(leaf.value))
	if a.parent == nil {
		t.prevRoot = leaf
	} else {
		switch a.parentC {
		case -1:
			a.parent.left = leaf
		case 1:
			a.parent.right = leaf
		}
	}
	l.arrow = nil
	if l.pinnedPageId != 0 {
		if l.pinnedPageId == 167 {
			fmt.Printf("Repinned page from leaf %d\n", l.pinnedPageId)
		}
		leaf.pinnedPageId = l.pinnedPageId
		t.repinnedPages[l.pinnedPageId] = struct{}{}
		repinned[l.pinnedPageId] = struct{}{}
		l.pinnedPageId = 0
	}
}

// Peels all the nodes pinned to specified page
func (t *Avl3) peelAllPinned(pageId PageID) {
	repinned := make(map[PageID]struct{})
	trace := pageId == 167
	if trace {
		fmt.Printf("peelAllPinned(%d)\n", pageId)
	}
	modPage, modPageFound := t.modifiedPages[pageId]
	if !modPageFound {
		if trace {
			fmt.Printf("No modPage found for %d\n", pageId)
		}
		return
	}
	if len(modPage.pins) == 0 {
		if trace {
			fmt.Printf("len(modPage.pins) == 0 for %d\n", pageId)
		}
		return
	}
	if trace {
		fmt.Printf("len(modPage.pins) == %d for %d\n", len(modPage.pins), pageId)
	}
	for _, pin := range modPage.pins {
		key := pin.nkey()
		height := pin.getheight()
		current := t.root
		if trace {
			fmt.Printf("==== peelAllPinned next pin for %d: %x %d, root %x %d\n", pageId, key, height, current.nkey(), current.getheight())
		}
		var prev *Fork3
		var prevC int
		loop: for {
			switch n := current.(type) {
			case nil:
				break loop
			case *Leaf3:
				if t.compare(key, n.key) == 0 && height == 1 && n.arrow != nil {
					if trace {
						fmt.Printf("peelAllPinned moveOver leaf for %d, %x\n", pageId, n.key)
					}
					t.moveArrowOverLeaf(n.arrow, n, repinned)
				}
				break loop
			case *Fork3:
				if n.arrow != nil {
					if trace {
						fmt.Printf("peelAllPinned moveOver fork for %d, %x %d\n", pageId, n.max, n.height)
					}
					t.moveArrowOverFork(n.arrow, n, repinned)
				}
				prev = n
				switch t.compare(key, n.left.getmax()) {
				case 0, -1:
					current = n.left
					if trace {
						fmt.Printf("peelAllPinned step left for %d, %x %x %d\n", pageId, n.left.getmax(), n.max, n.height)
					}
					prevC = -1
				case 1:
					current = n.right
					if trace {
						fmt.Printf("peelAllPinned step right for %d, %x %x %d\n", pageId, n.left.getmax(), n.max, n.height)
					}
					prevC = 1
				}
				if height >= n.height {
					break loop
				}
			case *Arrow3:
				if trace || n.pageId == 167 {
					fmt.Printf("peelAllPinned arrow P.%d, %x %d for %d\n", n.pageId, n.max, n.height, pageId)
				}
				point := t.deserialisePage(n.pageId, n.max, n.height, 0, true)
				if point == nil {
					panic(fmt.Sprintf("Arrow P.%d, %x %d\n", n.pageId, n.max, n.height))
				}
				if n.arrow != nil {
					n.arrow.pageId = n.pageId
					switch nn := point.(type) {
					case *Leaf3:
						nn.arrow = n.arrow
					case *Fork3:
						nn.arrow = n.arrow
					case *Arrow3:
						nn.arrow = n.arrow
					}
				}
				if prev == nil {
					t.root = point
				} else if prevC == -1 {
					prev.left = point
				} else if prevC == 1 {
					prev.right = point
				}
				current = point
			}
		}
	}
	for secondaryPageId := range repinned {
		if secondaryPageId != pageId {
			t.peelAllPinned(secondaryPageId)
		}
	}
}

// Opens (removes arrows from the root to the given reference)
func (t *Avl3) openTo(r Ref3, pageId PageID) {
	trace := pageId == 167
	key := r.nkey()
	height := r.getheight()
	current := t.root
	var prev *Fork3
	var prevC int
	loop: for {
		switch n := current.(type) {
		case nil:
			break loop
		case *Leaf3:
			if trace {
				fmt.Printf("openTo %x %d at leaf %x\n", key, height, n.key)
			}
			break loop
		case *Fork3:
			if trace {
				fmt.Printf("openTo %x %d at fork %x %d\n", key, height, n.max, n.height)
			}
			prev = n
			switch t.compare(key, n.left.getmax()) {
			case 0, -1:
				current = n.left
				prevC = -1
			case 1:
				current = n.right
				prevC = 1
			}
			if height >= n.height {
				break loop
			}
		case *Arrow3:
			point := t.deserialisePage(n.pageId, n.max, n.height, 0, true)
			if n.pageId == 167 {
				fmt.Printf("openTo: Arrow P.%d, %x %d\n", n.pageId, n.max, n.height)
			}
			if point == nil {
				panic(fmt.Sprintf("Arrow P.%d, %x %d\n", n.pageId, n.max, n.height))
			}
			if prev == nil {
				t.root = point
			} else if prevC == -1 {
				prev.left = point
			} else if prevC == 1 {
				prev.right = point
			}
			current = point
		}
	}
}

func (t *Avl3) Insert(key, value []byte) bool {
	inserted := true
	var current Ref3 = t.root
	loop: for {
		switch n := current.(type) {
		case nil:
			break loop
		case *Leaf3:
			if t.compare(key, n.key) == 0 {
				if bytes.Equal(value, n.nvalue(t)) {
					return false
				} else {
					inserted = false
				}
			}
			break loop
		case *Fork3:
			switch t.compare(key, n.left.getmax()) {
			case 0, -1:
				current = n.left
			case 1:
				current = n.right
			}
		case *Arrow3:
			current = t.deserialisePage(n.pageId, n.max, n.height, 0, false)
			if current == nil {
				panic(fmt.Sprintf("Arrow P.%d, %x %d\n", n.pageId, n.max, n.height))
			}
		}
	}
	repinned := make(map[PageID]struct{})
	t.root = t.insert(t.root, key, value, repinned)
	for pageId := range repinned {
		t.peelAllPinned(pageId)
	}
	return inserted
}

func (t *Avl3) insert(current Ref3, key, value []byte, repinned map[PageID]struct{}) Ref3 {
	trace := t.trace
	switch n := current.(type) {
	case nil:
		if trace {
			fmt.Printf("Inserting %s, on nil\n", key)
		}
		return &Leaf3{key: key, value: value, valueLen: uint32(len(value))}
	case *Arrow3:
		return t.insert(t.Peel(n, key, 1), key, value, repinned)
	case *Leaf3:
		if trace {
			fmt.Printf("Inserting %s, on Leaf %s\n", key, n.key)
		}
		var newnode *Fork3
		switch t.compare(key, n.key) {
		case 0:
			if n.arrow != nil {
				t.moveArrowOverLeaf(n.arrow, n, repinned)
			}
			n.value = value
			t.freeValueId(n.valueId)
			n.valueId = 0
			n.valueLen = uint32(len(value))
			return n
		case -1:
			newnode = &Fork3{max: n.key, height: 2, left: &Leaf3{key: key, value: value, valueLen: uint32(len(value))}, right: n}
		case 1:
			newnode = &Fork3{max: key, height: 2, left: n, right: &Leaf3{key: key, value: value, valueLen: uint32(len(value))}}
		}
		return newnode
	case *Fork3:
		c := t.compare(key, n.left.getmax())
		if trace {
			fmt.Printf("Inserting %s, on node %s, height %d\n", key, n.max, n.height)
		}
		if n.arrow != nil {
			t.moveArrowOverFork(n.arrow, n, repinned)
		}
		// Prepare it for the next iteraion
		switch c {
		case 0, -1:
			n.left = t.insert(n.left, key, value, repinned)
		case 1:
			n.right = t.insert(n.right, key, value, repinned)
			n.max = n.right.getmax()
		}
		lHeight := n.left.getheight()
		rHeight := n.right.getheight()
		n.height = 1 + maxu32(lHeight, rHeight)
		if rHeight > lHeight {
			if rHeight - lHeight > 1 {
				// nr is a Fork, because its height is at least 3
				nr := t.Peel(n.right, key, 2).(*Fork3)
				if nr.arrow != nil {
					t.moveArrowOverFork(nr.arrow, nr, repinned)
				}
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
					nrl := t.Peel(nr.left, key, 3).(*Fork3)
					if nrl.arrow != nil {
						t.moveArrowOverFork(nrl.arrow, nrl, repinned)
					}
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
			nl := t.Peel(n.left, key, 4).(*Fork3)
			if nl.arrow != nil {
				t.moveArrowOverFork(nl.arrow, nl, repinned)
			}
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
				nlr := t.Peel(nl.right, key, 5).(*Fork3)
				if nlr.arrow != nil {
					t.moveArrowOverFork(nlr.arrow, nlr, repinned)
				}
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

func (t *Avl3) Delete(key []byte) bool {
	var current Ref3 = t.root
	loop: for {
		switch n := current.(type) {
		case nil:
			return false
		case *Leaf3:
			if t.compare(key, n.key) == 0 {
				break loop
			} else {
				return false
			}
		case *Fork3:
			switch t.compare(key, n.left.getmax()) {
			case 0, -1:
				current = n.left
			case 1:
				current = n.right
			}
		case *Arrow3:
			current = t.deserialisePage(n.pageId, n.max, n.height, 0, false)
			if current == nil {
				panic("")
			}
			if current.getheight() != n.height {
				panic(fmt.Sprintf("deseailised size %d, arrow height %d", current.getheight(), n.height))
			}
		}
	}
	repinned := make(map[PageID]struct{})
	t.root = t.delete(t.root, key, repinned)
	for pageId := range repinned {
		t.peelAllPinned(pageId)
	}
	return true
}

func (t *Avl3) delete(current Ref3, key []byte, repinned map[PageID]struct{}) Ref3 {
	trace := t.trace
	switch n := current.(type) {
	case nil:
		panic("nil")
	case *Arrow3:
		return t.delete(t.Peel(n, key, 6), key, repinned)
	case *Leaf3:
		// Assuming that key is equal to n.key
		if trace {
			fmt.Printf("Deleting on leaf %s\n", n.nkey())
		}
		if n.arrow == nil {
			// Don't release the value because it is still used by the previous version
			t.freeValueId(n.valueId)
		}
		return nil
	case *Fork3:
		c := t.compare(key, n.left.getmax())
		if n.arrow != nil {
			t.moveArrowOverFork(n.arrow, n, repinned)
		}
		// Special cases when both right and left are leaves
		if t.IsLeaf(n.left) && t.IsLeaf(n.right) {
			nl := t.Peel(n.left, key, 7).(*Leaf3)
			nr := t.Peel(n.right, key, 8).(*Leaf3)
			if trace {
				fmt.Printf("Special case: Leaf, Leaf\n")
			}
			switch c {
			case 0, -1:
				if nl.arrow == nil {
					t.freeValueId(nl.valueId)
				}
				return nr
			case 1:
				if nr.arrow == nil {
					t.freeValueId(nr.valueId)
				}
				return nl
			}
			panic("")
		}
		if trace {
			fmt.Printf("Deleting %s, on node %s, height %d\n", key, n.max, n.height)
		}
		switch c {
		case 0, -1:
			n.left = t.delete(n.left, key, repinned)
			if n.left == nil {
				return n.right
			}
		case 1:
			n.right = t.delete(n.right, key, repinned)
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
				nr := t.Peel(n.right, key, 9).(*Fork3)
				if nr.arrow != nil {
					t.moveArrowOverFork(nr.arrow, nr, repinned)
				}
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
					nrl := t.Peel(nr.left, key, 10).(*Fork3)
					if nrl.arrow != nil {
						t.moveArrowOverFork(nrl.arrow, nrl, repinned)
					}
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
			nl := t.Peel(n.left, key, 11).(*Fork3)
			if nl.arrow != nil {
				t.moveArrowOverFork(nl.arrow, nl, repinned)
			}
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
				nlr := t.Peel(nl.right, key, 12).(*Fork3)
				if nlr.arrow != nil {
					t.moveArrowOverFork(nlr.arrow, nlr, repinned)
				}
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

func (t *Avl3) Dot() *graphContext {
	fmt.Printf("Dotting the root\n")
	ctx := &graphContext{}
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: "root0",
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel("root", 16, "sans-serif")
	ctx.Nodes = append(ctx.Nodes, gn)
	t.root.dot(t, ctx, "root")
	ctx.Edges = append(ctx.Edges, &graphEdge{
		From: "root0",
		To: "root",
	})
	if t.prevRoot != nil {
		fmt.Printf("Dotting the prevRoot\n")
		gn := &graphNode{
			Attrs: map[string]string{},
			Path: "prevRoot0",
		}
		for k, v := range defaultGraphNodeAttrs {
			gn.Attrs[k] = v
		}
		gn.Label = mkLabel("prev", 16, "sans-serif")
		ctx.Nodes = append(ctx.Nodes, gn)
		t.prevRoot.dot(t, ctx, "prevRoot")
		ctx.Edges = append(ctx.Edges, &graphEdge{
			From: "prevRoot0",
			To: "prevRoot",
		})
	}
	return ctx
}

func (a *Arrow3) dot(t *Avl3, ctx *graphContext, path string) {
	fmt.Printf("Dotting the arrow P.%d max %s, height %d\n", a.pageId, a.max, a.height)
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

	if a.pageId != PageID(0) {
		point := t.deserialisePage(a.pageId, a.max, a.height, 0, false)
		if point == nil {
			panic("")
		} else {
			pointPath := fmt.Sprintf("%p", point)
			if point != nil {
				point.dot(t, ctx, pointPath)
			}
			ctx.Edges = append(ctx.Edges, &graphEdge{
				From: path,
				To: pointPath,
			})
		}
	}
}

func (l *Leaf3) dot(t *Avl3, ctx *graphContext, path string) {
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: path,
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel(string(l.nkey()), 16, "sans-serif")
	gn.Label += mkLabel(string(l.nvalue(t)), 10, "sans-serif")
	if l.pinnedPageId != PageID(0) {
		gn.Label += mkLabel(fmt.Sprintf("* %d", l.pinnedPageId), 10, "sans-serif")
	} else if l.arrow != nil {
		gn.Label += mkLabel("*", 10, "sans-serif")
	}
	ctx.Nodes = append(ctx.Nodes, gn)
}

func (f *Fork3) dot(t *Avl3, ctx *graphContext, path string) {
	gn := &graphNode{
		Attrs: map[string]string{},
		Path: path,
	}
	for k, v := range defaultGraphNodeAttrs {
		gn.Attrs[k] = v
	}
	gn.Label = mkLabel(string(f.max), 16, "sans-serif")
	gn.Label += mkLabel(fmt.Sprintf("%d", f.height), 10, "sans-serif")
	if f.pinnedPageId != PageID(0) {
		gn.Label += mkLabel(fmt.Sprintf("* %d", f.pinnedPageId), 10, "sans-serif")
	} else if f.arrow != nil {
		gn.Label += mkLabel("*", 10, "sans-serif")
	}
	ctx.Nodes = append(ctx.Nodes, gn)
	leftPath := fmt.Sprintf("%p", f.left)
	f.left.dot(t, ctx, leftPath)
	ctx.Edges = append(ctx.Edges, &graphEdge{
		From: path,
		To: leftPath,
	})
	rightPath := fmt.Sprintf("%p", f.right)
	f.right.dot(t, ctx, rightPath)
	ctx.Edges = append(ctx.Edges, &graphEdge{
		From: path,
		To: rightPath,
	})
}

func (l *Leaf3) heightsCorrect(path string) (uint32, bool) {
	return 1, true
}

func (f *Fork3) heightsCorrect(path string) (uint32, bool) {
	leftHeight, leftCorrect := f.left.heightsCorrect(path + "l")
	rightHeight, rightCorrect := f.right.heightsCorrect(path + "r")
	height, correct := 1+maxu32(leftHeight, rightHeight), leftCorrect&&rightCorrect&&(1+maxu32(leftHeight, rightHeight) == f.height)
	if !correct {
		fmt.Printf("At path %s, key %s, expected %d, got %d\n", path, f.max, height, f.height)
	}
	return height, correct
}

func (l *Leaf3) balanceCorrect() bool {
	return true
}

func (f *Fork3) balanceCorrect() bool {
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

func (a *Arrow3) heightsCorrect(path string) (uint32, bool) {
	return a.height, true
}

func (a *Arrow3) balanceCorrect() bool {
	return true
}

func (t *Avl3) pageSize3(leafCount, arrowCount, keyBodySize, valBodySize, structBits uint32, prefix []byte) (uint32, uint32) {
	nodeCount := leafCount + arrowCount
	prefixLen := uint32(len(prefix))

	base := 4 /* extensions */ + 
		4 /* nodeCount */ +
		4*((nodeCount+31)/32) /* pageBits */ +
		4 /* prefixOffset */ +
		4*nodeCount + 4 /* key header */ +
		(12+t.hashLength)*arrowCount /* arrow header */ +
		4*leafCount /* value header */ +
		4*((structBits+31)/32) /* structBits */ +
		prefixLen +
		keyBodySize - nodeCount*prefixLen /* Discount bodySize using prefixLen */ +
		valBodySize

	// Number of extensions required to store the page
	var prevExt uint32 = 0
	size := base
	extensions := (base + PageSize - 1)/PageSize - 1
	for extensions > prevExt {
		size = base + 8 * extensions
		prevExt = extensions
		extensions = (size + PageSize - 1)/PageSize - 1
	}
	return size, extensions
}

type Forest3 struct {
	refs []Ref3
	arrows []*Arrow3
	prefix []byte
	leafCount uint32
	arrowCount uint32
	keyBodySize, valBodySize, structBits uint32
}

type Tree3 struct {
	ref Ref3
	arrow *Arrow3
	prefix []byte
	leafCount uint32
	arrowCount uint32
	keyBodySize, valBodySize, structBits uint32
	pageId PageID
	pinned bool
}

func (tree Tree3) ToForest() Forest3 {
	var arrows []*Arrow3
	if tree.arrow != nil {
		arrows = append(arrows, tree.arrow)
	}
	return Forest3 {
		refs: []Ref3{tree.ref},
		arrows: arrows,
		prefix: tree.prefix,
		leafCount: tree.leafCount,
		arrowCount: tree.arrowCount,
		keyBodySize: tree.keyBodySize,
		valBodySize: tree.valBodySize,
		structBits: tree.structBits,
	}
}

// Structure holding pages to be committed. These can be rearranged further
type CommitBuffer struct {
	pinned map[PageID]Forest3 // Forests pinned to specific page number
	pinnedSizes map[PageID]uint32
	free []Tree3 // Unrestricted trees
}

type PageContainer struct {
	f          Forest3
	tieBreaker int
	pageId     PageID
	oldPin     bool
	size       uint32 // Size of stuff in the container
	extensions uint32 // Number of extensions required to store the page
}

func (f Forest3) Len() int {
	return len(f.refs)
}

func (f Forest3) Less(i, j int) bool {
	return bytes.Compare(f.refs[i].getmax(), f.refs[i].getmax()) < 0
}

func (f Forest3) Swap(i, j int) {
	f.refs[i], f.refs[j] = f.refs[j], f.refs[i]
}

func (a *PageContainer) Less(b llrb.Item) bool {
	bi := b.(*PageContainer)
	if a.size == bi.size {
		return a.tieBreaker < bi.tieBreaker
	}
	return a.size > bi.size
}

func detectMergable(pageId PageID, r Ref3) {
	switch n := r.(type) {
	case *Fork3:
		detectMergable(pageId, n.left)
		detectMergable(pageId, n.right)
	case *Arrow3:
		if n.pageId == pageId {
			panic(fmt.Sprintf("Detected mergeable arrow in page %d: %x %d\n", n.pageId, n.max, n.height))
		}
	}
}

func merge(prev *Fork3, prevC int, r Ref3, arrowMap map[string]map[uint32]Ref3) {
	switch n := r.(type) {
	case *Fork3:
		merge(n, -1, n.left, arrowMap)
		merge(n, 1, n.right, arrowMap)
	case *Arrow3:
		if a, ok1 := arrowMap[string(n.max)]; ok1 {
			if b, ok2 := a[n.height]; ok2 {
				if prev == nil {
					panic("")
				} else if prevC == -1 {
					prev.left = b
				} else if prevC == 1 {
					prev.right = b
				}
				delete(a, n.height)
			}
			if len(a) == 0 {
				delete(arrowMap, string(n.max))
			}
		}
	}
}

func (c *PageContainer) DetectMergable() {
	for _, r := range c.f.refs {
		detectMergable(c.pageId, r)
	}
}

type TreeItem struct {
	tree Tree3
	size uint32 // Size of the item
}

func (a *TreeItem) Less(b llrb.Item) bool {
	bi := b.(*TreeItem)
	return a.size < bi.size
}

func (buffer *CommitBuffer) AddPinnedTree(t *Avl3, pageId PageID, tree Tree3) {
	oldForest, ok := buffer.pinned[pageId]
	if ok {
		newForest := tree.ToForest()
		forest := Forest3 {
			refs: append(oldForest.refs, newForest.refs...),
			arrows: append(oldForest.arrows, newForest.arrows...),
			prefix: commonPrefix(oldForest.prefix, newForest.prefix),
			leafCount: oldForest.leafCount + newForest.leafCount,
			arrowCount: oldForest.arrowCount + newForest.arrowCount,
			keyBodySize: oldForest.keyBodySize + newForest.keyBodySize,
			valBodySize: oldForest.valBodySize + newForest.valBodySize,
			structBits: oldForest.structBits + newForest.structBits + 1,
		}
		forest, size, _ := t.mergeForest(forest, pageId)
		buffer.pinned[pageId] = forest
		//size, _ := t.pageSize3(forest.leafCount, forest.arrowCount, forest.keyBodySize, forest.valBodySize, forest.structBits, forest.prefix)
		buffer.pinnedSizes[pageId] = size
	} else {
		newForest := tree.ToForest()
		buffer.pinned[pageId] = newForest
		size, _ := t.pageSize3(newForest.leafCount, newForest.arrowCount, newForest.keyBodySize, newForest.valBodySize, newForest.structBits, newForest.prefix)
		buffer.pinnedSizes[pageId] = size
	}
}

func (buffer *CommitBuffer) Pack(t *Avl3, mutating bool) []*PageContainer {
	pageContainers := llrb.New()
	nextTieBreaker := 0
	// Create containers for pinned forests
	pageIds := make(PageIDs, len(buffer.pinned))
	idx := 0
	for id := range buffer.pinned {
		pageIds[idx] = id
		idx++
	}
	sort.Sort(pageIds)
	for _, id := range pageIds {
		forest := buffer.pinned[id]
		container := &PageContainer{
			f: forest,
			tieBreaker: nextTieBreaker,
			pageId: id,
			oldPin: true,
		}
		container.size, container.extensions = t.pageSize3(
			forest.leafCount,
			forest.arrowCount,
			forest.keyBodySize,
			forest.valBodySize,
			forest.structBits,
			forest.prefix,
		)
		if container.pageId == 167 {
			fmt.Printf("Container %d size is %d\n", container.pageId, container.size)
		}
		nextTieBreaker++
		pageContainers.InsertNoReplace(container)
	}
	items := llrb.New()
	for _, tree := range buffer.free {
		item := &TreeItem{
			tree: tree,
		}
		item.size, _ = t.pageSize3(tree.leafCount, tree.arrowCount, tree.keyBodySize, tree.valBodySize, tree.structBits, tree.prefix)
		items.InsertNoReplace(item)
	}
	// Best Fit Desceasing bin packing algorithm
	items.DescendLessOrEqual(items.Max(), func(x llrb.Item) bool {
		i := x.(*TreeItem)
		// Choose the smallest container that would fit this item
		var oldContainer *PageContainer
		var newContainer *PageContainer
		pageContainers.AscendGreaterOrEqual(&PageContainer{size: PageSize - (i.size - 12)}, func(y llrb.Item) bool {
			c := y.(*PageContainer)
			cLeafCount := i.tree.leafCount + c.f.leafCount
			cArrowCount := i.tree.arrowCount + c.f.arrowCount
			cKeyBodySize := i.tree.keyBodySize + c.f.keyBodySize
			cValBodySize := i.tree.valBodySize + c.f.valBodySize
			cStructBits := i.tree.structBits + c.f.structBits + 1 /* extra structBit for separator */
			cPrefix := commonPrefix(i.tree.prefix, c.f.prefix)
			cSize, cExt := t.pageSize3(cLeafCount, cArrowCount, cKeyBodySize, cValBodySize, cStructBits, cPrefix)
			if cSize <= PageSize {
				if c.pageId == 167 {
					fmt.Printf("Container %d size is %d after packing\n", c.pageId, cSize)
				}
				oldContainer = c
				arrows := c.f.arrows
				if i.tree.arrow != nil {
					arrows = append(arrows, i.tree.arrow)
				}
				newContainer = &PageContainer{
					f: Forest3 {
						refs: append(c.f.refs, i.tree.ref),
						arrows: arrows,
						prefix: cPrefix,
						leafCount: cLeafCount,
						arrowCount: cArrowCount,
						keyBodySize: cKeyBodySize,
						valBodySize: cValBodySize,
						structBits: cStructBits,
					},
					tieBreaker: nextTieBreaker,
					pageId: c.pageId,
					oldPin: c.oldPin,
					size: cSize,
					extensions: cExt,
				}
				nextTieBreaker++
				return false
			}
			return true
		})
		if oldContainer != nil {
			pageContainers.Delete(oldContainer)
		}
		if newContainer == nil {
			// No appropriate container found, create a new one
			newContainer = &PageContainer{
				f: i.tree.ToForest(),
				tieBreaker: nextTieBreaker,
			}
			newContainer.size, newContainer.extensions = t.pageSize3(
				i.tree.leafCount,
				i.tree.arrowCount,
				i.tree.keyBodySize,
				i.tree.valBodySize,
				i.tree.structBits,
				i.tree.prefix,
			)
			nextTieBreaker++
		}
		pageContainers.InsertNoReplace(newContainer)
		return true
	})
	// Sort forests in the containers
	containers := make([]*PageContainer, pageContainers.Len())
	i := 0
	pageContainers.DescendLessOrEqual(pageContainers.Max(), func(y llrb.Item) bool {
		c := y.(*PageContainer)
		c.f, c.size, c.extensions = t.mergeForest(c.f, c.pageId)
		sort.Sort(c.f)
		containers[i] = c
		i++
		return true
	})
	return containers
}

type PageIDs []PageID

func (p PageIDs) Len() int {
	return len(p)
}
func (p PageIDs) Less(i, j int) bool {
	return p[i] < p[j]
}
func (p PageIDs) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (t *Avl3) mergeForest(p Forest3, pageId PageID) (Forest3, uint32, uint32) {
	arrowMap := make(map[string]map[uint32]Ref3)
	for _, r := range p.refs {
		//if arrow, ok := r.(*Arrow3); ok && arrow.pageId == pageId {
		//	fmt.Printf("Remove useless arrow from page %d\n", pageId)
		//	p.refs[i] = nil
		//} else {
			if a, ok1 := arrowMap[string(r.getmax())]; ok1 {
				a[r.getheight()] = r
			} else {
				b := make(map[uint32]Ref3)
				b[r.getheight()] = r
				arrowMap[string(r.getmax())] = b
			}
		//}
	}
	for _, r := range p.refs {
		if r == nil {
			continue
		}
		merge(nil, 0, r, arrowMap)
	}
	// Recreate list of references that weren't merged
	p.refs = []Ref3{}
	var cLeafCount, cArrowCount, cKeyBodySize, cValBodySize, cStructBits uint32
	var cPrefix []byte
	idx := 0
	for _, a := range arrowMap {
		for _, b := range a {
			tree := b.prepare(t, nil, false)
			cLeafCount += tree.leafCount
			cArrowCount += tree.arrowCount
			cKeyBodySize += tree.keyBodySize
			cValBodySize += tree.valBodySize
			if idx == 0 {
				cStructBits = tree.structBits
			} else {
				cStructBits += tree.structBits + 1 // extra structBit for separator
			}
			cPrefix = commonPrefix(cPrefix, tree.prefix)
			p.refs = append(p.refs, b)
			idx++
		}
	}
	if pageId == 167 {
		if p.leafCount != cLeafCount {
			fmt.Printf("For page %d, p.leafCount (%d) != cLeafCount (%d)\n", pageId, p.leafCount, cLeafCount)
		}
		if p.arrowCount != cArrowCount {
			fmt.Printf("For page %d, p.arrowCount (%d) != cArrowCount (%d)\n", pageId, p.arrowCount, cArrowCount)
		}
		if p.keyBodySize != cKeyBodySize {
			fmt.Printf("For page %d, p.keyBodySize (%d) != cKeyBodySize (%d)\n", pageId, p.keyBodySize, cKeyBodySize)
		}
		if p.valBodySize != cValBodySize {
			fmt.Printf("For page %d, p.valBodySize (%d) != cValBodySize (%d)\n", pageId, p.valBodySize, cValBodySize)
		}
		if p.structBits != cStructBits {
			fmt.Printf("For page %d, p.structBits (%d) != cStructBits (%d)\n", pageId, p.structBits, cStructBits)
		}
		if !bytes.Equal(p.prefix, cPrefix) {
			fmt.Printf("For page %d, p.prefix (%x) != cPrefix (%x)\n", pageId, p.prefix, cPrefix)
		}
	}
	p.leafCount = cLeafCount
	p.arrowCount = cArrowCount
	p.keyBodySize = cKeyBodySize
	p.valBodySize = cValBodySize
	p.structBits = cStructBits
	p.prefix = cPrefix
	size, extensions := t.pageSize3(cLeafCount, cArrowCount, cKeyBodySize, cValBodySize, cStructBits, cPrefix)
	if pageId == 167 {
		fmt.Printf("Pinned refs on page %d after merging: %d\n", pageId, len(p.refs))
	}
	if pageId == 167 {
		fmt.Printf("Size of page %d after merging: %d\n", pageId, size)
	}
	return p, size, extensions
}

// Split current buffer into pages and commit them
func (t *Avl3) Commit() uint64 {
	if t.root == nil {
		return 0
	}
	startCounter := t.commitedCounter
	var buffer CommitBuffer
	buffer.pinned = make(map[PageID]Forest3)
	buffer.pinnedSizes = make(map[PageID]uint32)
	allDone := false
	for !allDone {
		allDone = true
		for pageId, modPage := range t.modifiedPages {
			if pageId == 167 {
				fmt.Printf("To open %d refs for page %d\n", len(modPage.roots), pageId)
			}
			for _, r := range modPage.roots {
				t.openTo(r, pageId)
				allDone = false
			}
		}
	}
	tree := t.root.prepare(t, &buffer, true)
	tree.ref = t.root
	if tree.pinned {
		buffer.AddPinnedTree(t, tree.pageId, tree)
	} else {
		buffer.free = append(buffer.free, tree)
	}
	if tree.pageId == 167 {
		fmt.Printf("tree.pageId %d\n", tree.pageId)
	}
	for pageId, _ := range t.modifiedPages {
		_, pinned := buffer.pinned[pageId]
		if pinned && pageId == 167 {
			fmt.Printf("Commit page %d, still pinned: %d\n", pageId, len(buffer.pinned[pageId].refs))
			/*
			for _, r := range buffer.pinned[pageId].refs {
				detectMergable(pageId, r)
			}
			*/
		}
		_, repinned := t.repinnedPages[pageId]
		if !pinned && !repinned {
			// Only release the page it if has not been repinned
			if pageId == 167 {
				fmt.Printf("Commit released page %d\n", pageId)
			}
			t.freePageId(pageId)
		} else {
			//fmt.Printf("Commit could not release page %d, pinned: %t, repinned: %t\n", pageId, pinned, repinned)
		}
	}
	t.modifiedPages = make(map[PageID]*ModPage)
	t.repinnedPages = make(map[PageID]struct{})
	containers := buffer.Pack(t, true /*mutating*/)
	var rootPageId PageID
	var rootTree uint32
	// Assign page ids to to the containers
	for _, c := range containers {
		if !c.oldPin {
			c.pageId = t.nextPageId()
		}
		for _, a := range c.f.arrows {
			a.pageId = c.pageId
		}
		if rootPageId == PageID(0) {
			for idx, r := range c.f.refs {
				if t.root == r {
					rootPageId = c.pageId
					rootTree = uint32(idx)
					break
				}
			}
		}
	}
	// Commit
	//fmt.Printf("Committing root\n")
	for _, c := range containers {
		//c.DetectMergable()
		t.commitPage(c, false)
	}

	if t.prevRoot != nil {
		var buffer CommitBuffer
		buffer.pinned = make(map[PageID]Forest3)
		buffer.pinnedSizes = make(map[PageID]uint32)
		tree := t.prevRoot.prepare(t, &buffer, true)
		tree.ref = t.prevRoot
		if tree.pinned {
			buffer.AddPinnedTree(t, tree.pageId, tree)
		} else {
			buffer.free = append(buffer.free, tree)
		}
		t.solidPinned += len(buffer.pinned)
		t.solidFree += len(buffer.free)
		containers := buffer.Pack(t, false /*mutating*/)
		var prevRootPageId PageID
		var prevRootTree uint32
		// Assign page ids to to the containers
		for _, c := range containers {
			if !c.oldPin {
				c.pageId = t.nextPageId()
			}
			for _, a := range c.f.arrows {
				a.pageId = c.pageId
			}
			if prevRootPageId == PageID(0) {
				for idx, r := range c.f.refs {
					if t.prevRoot == r {
						prevRootPageId = c.pageId
						prevRootTree = uint32(idx)
						break
					}
				}
			}
		}
		// Commit
		//fmt.Printf("Committing prevRoot\n")
		for _, c := range containers {
			t.commitPage(c, true)
		}
		t.versions[t.currentVersion] = Version3{pageId: prevRootPageId, treeIndex: prevRootTree}
		if t.verFile != nil {
			var verdata [12]byte
			binary.BigEndian.PutUint64(verdata[:], uint64(prevRootPageId))
			binary.BigEndian.PutUint32(verdata[8:], prevRootTree)
			t.verFile.WriteAt(verdata[:], int64(t.currentVersion)*int64(12))
		}
	}
	t.currentVersion++
	t.versions[t.currentVersion] = Version3{pageId: rootPageId, treeIndex: rootTree}
	if t.verFile != nil {
		var verdata [12]byte
		binary.BigEndian.PutUint64(verdata[:], uint64(rootPageId))
		binary.BigEndian.PutUint32(verdata[8:], rootTree)
		t.verFile.WriteAt(verdata[:], int64(t.currentVersion)*int64(12))
	}
	prevRootArrow := &Arrow3{pageId: rootPageId, height: t.root.getheight(), max: t.root.getmax()}
	t.root = &Arrow3{pageId: rootPageId, height: t.root.getheight(), max: t.root.getmax(), arrow: prevRootArrow}
	t.prevRoot = prevRootArrow
	t.checkFlush(false)
	return t.commitedCounter - startCounter
}

func (t *Avl3) commitPage(c *PageContainer, solid bool) {
	if c.size > PageSize {
		//fmt.Printf("Page %d overflow: %d, maxPageId: %d\n", c.pageId, c.size, t.maxPageId)
	}
	//fmt.Printf("Committing page %d\n", c.pageId)
	trace := t.trace
	data := make([]byte, c.size)
	offset := uint32(0)
	binary.BigEndian.PutUint32(data[offset:], c.extensions)
	offset += 4
	// Prepare extension pages
	pageIds := make([]PageID, 1 + c.extensions)
	pageIds[0] = c.pageId
	for i := 1; i < len(pageIds); i++ {
		pageIds[i] = t.nextPageId()
		binary.BigEndian.PutUint64(data[offset:], uint64(pageIds[i]))
		offset += 8
	}
	nodeCount := c.f.leafCount + c.f.arrowCount
	binary.BigEndian.PutUint32(data[offset:], nodeCount)
	if c.pageId == 167 {
		fmt.Printf("commiting page %d with nodeCount = %d\n", c.pageId, nodeCount)
	}
	offset += 4
	pageBitsOffset := offset
	offset += 4*((nodeCount+31)/32)
	prefixOffsetOffset := offset
	offset += 4
	keyHeaderOffset := offset
	offset += 4*nodeCount /* key offset per node */ + 4 /* end key offset */
	arrowHeaderOffset := offset
	offset += (12+t.hashLength)*c.f.arrowCount /* (pageId, size - minSize, hash) per arrow */
	valueHeaderOffset := offset
	offset += 4*c.f.leafCount /* value length per leaf */
	structBitsOffset := offset
	if trace {
		//fmt.Printf("StructBitsOffset %d\n", structBitsOffset)
	}
	offset += 4*((c.f.structBits+31)/32) // Structure encoding
	// key prefix begins here
	binary.BigEndian.PutUint32(data[prefixOffsetOffset:], uint32(offset))
	copy(data[offset:], c.f.prefix)
	offset += uint32(len(c.f.prefix))
	keyBodyOffset := offset
	offset += c.f.keyBodySize - nodeCount*uint32(len(c.f.prefix))
	valBodyOffset := offset

	var nodeIndex uint32
	var structBit uint32
	if trace {
		//fmt.Printf("valueHeaderOffset %d\n", valueHeaderOffset)
	}
	for i, r := range c.f.refs {
		if i > 0 {
			structBit++ // Insert separator bit "0"
		}
		t.serialisePass2(r, c.pageId, data, len(c.f.prefix), false, pageBitsOffset, structBitsOffset,
			&nodeIndex, &structBit, &keyHeaderOffset, &arrowHeaderOffset, &valueHeaderOffset, &keyBodyOffset, &valBodyOffset)
	}
	if c.pageId == 167 {
		fmt.Printf("Commited page %d with %d refs\n", c.pageId, len(c.f.refs))
		for i, r := range c.f.refs {
			fmt.Printf("Ref %d: %x %d\n", i, r.getmax(), r.getheight())
		}
	}
	// end key offset
	if trace {
		fmt.Printf("valBodyOffset %d\n", keyBodyOffset)
	}
	binary.BigEndian.PutUint32(data[keyHeaderOffset:], keyBodyOffset)
	if valBodyOffset != c.size {
		panic(fmt.Sprintf("valBodyOffset %d (%d) != size %d, valBodySize %d, nodeCount %d, len(prefix): %d",
			valBodyOffset, offset, c.size, c.f.valBodySize, nodeCount, len(c.f.prefix)))
	}
	if nodeIndex != nodeCount {
		panic("n != nodeCount")
	}
	if structBit != c.f.structBits {
		panic("sb != structBits")
	}
	if t.trace {
		fmt.Printf("Committed page %d, nodeCount %d, prefix %s\n", c.pageId, nodeCount, c.f.prefix)
	}
	var remaining int64 = int64(c.size)
	var chunkOffset int64 = 0
	idx := 0
	for remaining > 0 {
		var chunk int64
		if remaining > int64(PageSize) {
			chunk = int64(PageSize)
		} else {
			chunk = remaining
		}
		dc := make([]byte, chunk)
		copy(dc[:], data[chunkOffset:chunkOffset+chunk])
		if t.pageFile != nil {
			if !solid {
				t.pageFile.WriteAt(dc, int64(pageIds[idx])*int64(PageSize))
				t.commitedCounter++
				t.pageCache.Add(pageIds[idx], dc)
			} else {
				t.commitMap[pageIds[idx]] = dc
				t.pageCache.Remove(pageIds[idx])
			}
		} else {
			t.pageMap[pageIds[idx]] = dc
			t.commitedCounter++
		}
		remaining -= chunk
		chunkOffset += chunk
		idx++
	}
	if solid {
		t.solidUsed += uint64(c.size)
		t.solidAlloc += uint64((1+c.extensions)*PageSize)
		t.solidRefs[len(c.f.refs)]++
		t.solidTreeSizes[256*(c.size/256)]++
	}
}

func (t *Avl3) checkFlush(force bool) {
	if !force && t.currentVersion - t.lastFlushed < 8*1024 && len(t.commitMap) < 512*1024 {
		return
	}
	for pageId, data := range t.commitMap {
		t.pageFile.WriteAt(data, int64(pageId)*int64(PageSize))
		t.pageCache.Add(pageId, data)
		t.commitedCounter++
	}
	t.commitMap = make(map[PageID][]byte)
	t.lastFlushed = t.currentVersion
}
 
// Computes all the dynamic parameters that allow calculation of the page length and pre-allocation of all buffers
func (l *Leaf3) prepare(t *Avl3, buffer *CommitBuffer, splitPages bool) (tree Tree3) {
	tree.leafCount = 1
	tree.keyBodySize = uint32(len(l.key))
	if l.valueLen > InlineValueMax {
		tree.valBodySize = 8 + HashLength // Size of value id + valueHash
	} else {
		tree.valBodySize = l.valueLen
	}
	tree.structBits = 1 // one bit per leaf
	tree.prefix = l.key
	if l.pinnedPageId != 0 {
		// Old pin
		tree.pageId = l.pinnedPageId
		tree.pinned = true
	}
	return
}

func (f *Fork3) prepare(t *Avl3, buffer *CommitBuffer, splitPages bool) (tree Tree3) {
	if f.pinnedPageId != 0 {
		// Old pin
		tree.pageId = f.pinnedPageId
		tree.pinned = true
	}

	treeL := f.left.prepare(t, buffer, splitPages)
	treeR := f.right.prepare(t, buffer, splitPages)
	// Fork and both children fit in the page
	var mergable3 bool
	var mergePageId PageID
	var mergePinned bool
	if splitPages {
		if !treeL.pinned && !treeR.pinned {
			mergable3 = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		} else if !treeL.pinned && !tree.pinned {
			mergable3 = true
			mergePageId = treeR.pageId
			mergePinned = treeR.pinned
		} else if !treeR.pinned && !tree.pinned {
			mergable3 = true
			mergePageId = treeL.pageId
			mergePinned = treeL.pinned
		} else if treeL.pageId == treeR.pageId && treeL.pinned == treeR.pinned && treeR.pageId == tree.pageId && treeR.pinned == tree.pinned {
			mergable3 = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		}
		if !tree.pinned && treeL.pinned {
			if pinnedSize, ok := buffer.pinnedSizes[treeL.pageId]; ok {
				if pinnedSize >= PageSize {
					mergable3 = false
				}
			}
		}
		if !tree.pinned && treeR.pinned {
			if pinnedSize, ok := buffer.pinnedSizes[treeR.pageId]; ok {
				if pinnedSize >= PageSize {
					mergable3 = false
				}
			}
		}
	}
	if !splitPages || mergable3 {
		tree.leafCount = treeL.leafCount+treeR.leafCount
		tree.arrowCount = treeL.arrowCount+treeR.arrowCount
		tree.keyBodySize = treeL.keyBodySize+treeR.keyBodySize
		tree.valBodySize = treeL.valBodySize+treeR.valBodySize
		tree.structBits = treeL.structBits+treeR.structBits+2 // 2 bits for the fork
		tree.prefix = commonPrefix(treeL.prefix, treeR.prefix)
		sizeLFR, _ := t.pageSize3(tree.leafCount, tree.arrowCount, tree.keyBodySize, tree.valBodySize, tree.structBits, tree.prefix)
		if !splitPages || sizeLFR < PageSize {
			tree.pageId = mergePageId
			tree.pinned = mergePinned
			return
		}
	}
	// Choose the biggest child and make a page out of it
	sizeL, _ := t.pageSize3(treeL.leafCount, treeL.arrowCount, treeL.keyBodySize, treeL.valBodySize, treeL.structBits, treeL.prefix)
	sizeR, _ := t.pageSize3(treeR.leafCount, treeR.arrowCount, treeR.keyBodySize, treeR.valBodySize, treeR.structBits, treeR.prefix)
	if sizeL > sizeR {
		var mergableR bool
		if !treeR.pinned {
			mergableR = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		} else if !tree.pinned {
			mergableR = true
			mergePageId = treeR.pageId
			mergePinned = treeR.pinned
		} else if treeR.pageId == tree.pageId && treeR.pinned == tree.pinned {
			mergableR = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		}
		if !tree.pinned && treeR.pinned {
			if pinnedSize, ok := buffer.pinnedSizes[treeR.pageId]; ok {
				if pinnedSize >= PageSize {
					mergableR = false
				}
			}
		}
		var lArrow *Arrow3
		if la, ok := f.left.(*Arrow3); ok {
			lArrow = la
		} else {
			lArrow = &Arrow3{height: f.left.getheight(), max: f.left.getmax()}
			treeL.ref = f.left
			if treeL.pinned {
				lArrow.pageId = treeL.pageId
				buffer.AddPinnedTree(t, treeL.pageId, treeL)
			} else {
				treeL.arrow = lArrow
				buffer.free = append(buffer.free, treeL)
			}
			f.left = lArrow
		}
		// Check if the fork and the right child still fit into a page
		tree.leafCount = treeR.leafCount
		tree.arrowCount = treeR.arrowCount+1 // 1 for the left arrow
		tree.keyBodySize = treeR.keyBodySize+uint32(len(lArrow.max))
		tree.valBodySize = treeR.valBodySize
		tree.structBits = treeR.structBits+3 // 2 for the fork and 1 for the left arrow
		tree.prefix = commonPrefix(treeR.prefix, lArrow.max)
		sizeFR, _ := t.pageSize3(tree.leafCount, tree.arrowCount, tree.keyBodySize, tree.valBodySize, tree.structBits, tree.prefix)
		if mergableR && sizeFR < PageSize {
			tree.pageId = mergePageId
			tree.pinned = mergePinned
			return
		} else {
			// Have to commit right child too
			var rArrow *Arrow3
			if ra, ok := f.right.(*Arrow3); ok {
				rArrow = ra
			} else {
				rArrow = &Arrow3{height: f.right.getheight(), max: f.right.getmax()}
				treeR.ref = f.right
				if treeR.pinned {
					rArrow.pageId = treeR.pageId
					buffer.AddPinnedTree(t, treeR.pageId, treeR)
				} else {
					treeR.arrow = rArrow
					buffer.free = append(buffer.free, treeR)
				}
				f.right = rArrow
			}
			tree.prefix = commonPrefix(rArrow.max, lArrow.max)
			tree.leafCount = 0
			tree.arrowCount = 2
			tree.keyBodySize = uint32(len(lArrow.max))+uint32(len(rArrow.max))
			tree.valBodySize = 0
			tree.structBits = 4 /* 2 bits for arrows, 2 for the fork */
			return
		}
	} else {
		var mergableL bool
		if !treeL.pinned {
			mergableL = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		} else if !tree.pinned {
			mergableL = true
			mergePageId = treeL.pageId
			mergePinned = treeL.pinned
		} else if treeL.pageId == tree.pageId && treeL.pinned == tree.pinned {
			mergableL = true
			mergePageId = tree.pageId
			mergePinned = tree.pinned
		}
		if !tree.pinned && treeL.pinned {
			if pinnedSize, ok := buffer.pinnedSizes[treeL.pageId]; ok {
				if pinnedSize >= PageSize {
					mergableL = false
				}
			}
		}
		var rArrow *Arrow3
		if ra, ok := f.right.(*Arrow3); ok {
			rArrow = ra
		} else {
			rArrow = &Arrow3{height: f.right.getheight(), max: f.right.getmax()}
			treeR.ref = f.right
			if treeR.pinned {
				rArrow.pageId = treeR.pageId
				buffer.AddPinnedTree(t, treeR.pageId, treeR)
			} else {
				treeR.arrow = rArrow
				buffer.free = append(buffer.free, treeR)
			}
			f.right = rArrow
		}
		// Check if the fork and the let child still fit into a page
		tree.leafCount = treeL.leafCount
		tree.arrowCount = treeL.arrowCount+1 // 1 for the left arrow
		tree.keyBodySize = treeL.keyBodySize+uint32(len(rArrow.max))
		tree.valBodySize = treeL.valBodySize
		tree.structBits = treeL.structBits+3 // 2 for the fork and 1 for the left arrow
		tree.prefix = commonPrefix(treeL.prefix, rArrow.max)
		sizeFL, _ := t.pageSize3(tree.leafCount, tree.arrowCount, tree.keyBodySize, tree.valBodySize, tree.structBits, tree.prefix)
		if mergableL && sizeFL < PageSize {
			tree.pageId = mergePageId
			tree.pinned = mergePinned
			return
		} else {
			// Have to commit left child too
			var lArrow *Arrow3
			if la, ok := f.left.(*Arrow3); ok {
				lArrow = la
			} else {
				lArrow = &Arrow3{height: f.left.getheight(), max: f.left.getmax()}
				treeL.ref = f.left
				if treeL.pinned {
					lArrow.pageId = treeL.pageId
					buffer.AddPinnedTree(t, treeL.pageId, treeL)
				} else {
					treeL.arrow = lArrow
					buffer.free = append(buffer.free, treeL)
				}
				f.left = lArrow
			}
			tree.prefix = commonPrefix(rArrow.max, lArrow.max)
			tree.leafCount = 0
			tree.arrowCount = 2
			tree.keyBodySize = uint32(len(lArrow.max))+uint32(len(rArrow.max))
			tree.valBodySize = 0
			tree.structBits = 4 /* 2 bits for arrows, 2 for the fork */
			return
		}
	}
}

func (a *Arrow3) prepare(t *Avl3, buffer *CommitBuffer, splitPages bool) (tree Tree3) {
	tree.arrowCount = 1
	tree.structBits = 1 // one bit for page reference
	tree.keyBodySize = uint32(len(a.max))
	tree.prefix = a.max
	return
}

func (t *Avl3) serialiseKey(key, data []byte, keyHeaderOffset, keyBodyOffset *uint32) {
	binary.BigEndian.PutUint32(data[*keyHeaderOffset:], *keyBodyOffset)
	*keyHeaderOffset += 4
	copy(data[*keyBodyOffset:], key)
	*keyBodyOffset += uint32(len(key))
}

func (t *Avl3) serialiseVal(value []byte, valueId uint64, valueLen uint32, data []byte, valueHeaderOffset, valBodyOffset *uint32) uint64 {
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
		if int(*valBodyOffset) >= len(data) {
			fmt.Printf("*valBodyOffset==%d, len(data)=%d\n", *valBodyOffset, len(data))
		}
		copy(data[*valBodyOffset:], value)
		*valBodyOffset += valueLen
	}
	return valueId
}

func (t *Avl3) serialisePass2(r Ref3, pageId PageID, data []byte, prefixLen int, subtreePinned bool, pageBitsOffset, structBitsOffset uint32,
	nodeIndex, structBit, keyHeaderOffset, arrowHeaderOffset, valueHeaderOffset, keyBodyOffset, valBodyOffset *uint32) {
	switch r := r.(type) {
	case *Leaf3:
		t.serialiseKey(r.key[prefixLen:], data, keyHeaderOffset, keyBodyOffset)
		r.valueId = t.serialiseVal(r.value, r.valueId, r.valueLen, data, valueHeaderOffset, valBodyOffset)
		// Update page bits
		*nodeIndex++
		// Update struct bits
		pinned := subtreePinned || r.pinnedPageId == pageId || r.arrow != nil
		if !pinned {
			// If the subtree containing this leaf of the leaf itself is pinned, we write "0" structural bit
			data[structBitsOffset+(*structBit>>3)] |= (uint8(1)<<(*structBit&7))
		} else {
			if t.trace {
				//fmt.Printf("Pinned leaf %s in page %d\n", r.key, pageId)
			}
		}
		*structBit++
		if r.arrow != nil {
			r.arrow.pageId = pageId
		}
	case *Fork3:
		pinned := subtreePinned || r.pinnedPageId == pageId || r.arrow != nil
		// Write opening parenthesis "0" (noop)
		t.serialisePass2(r.left, pageId, data, prefixLen, pinned, pageBitsOffset, structBitsOffset,
			nodeIndex, structBit, keyHeaderOffset, arrowHeaderOffset, valueHeaderOffset, keyBodyOffset, valBodyOffset)
		// Update struct bit
		*structBit++
		if r.arrow != nil {
			r.arrow.pageId = pageId
		}
		t.serialisePass2(r.right, pageId, data, prefixLen, pinned, pageBitsOffset, structBitsOffset,
			nodeIndex, structBit, keyHeaderOffset, arrowHeaderOffset, valueHeaderOffset, keyBodyOffset, valBodyOffset)
		// Write closing parenthesis "1"
		data[structBitsOffset+(*structBit>>3)] |= (uint8(1)<<(*structBit&7))
		*structBit++
	case *Arrow3:
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
		if !subtreePinned {
			data[structBitsOffset+(*structBit>>3)] |= (uint8(1)<<(*structBit&7))
		} else {
			if t.trace {
				//fmt.Printf("Pinned arrow %s in page %d\n", r.max, pageId)
			}
		}
		*structBit++
	}
}

func (t *Avl3) deserialiseKey(data []byte, keyHeaderOffset *uint32, prefix []byte) []byte {
	keyStart := binary.BigEndian.Uint32(data[*keyHeaderOffset:])
	keyEnd := binary.BigEndian.Uint32(data[*keyHeaderOffset+4:]) // Start of the next key (or end offset)
	*keyHeaderOffset += 4
	return append(prefix, data[keyStart:keyEnd]...)
}

func (t *Avl3) deserialiseVal(data []byte, valueHeaderOffset, valBodyOffset *uint32) (value []byte, valueId uint64, valLen uint32) {
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

func (t *Avl3) returnModPage(pageId PageID, modRefs []Ref3, key []byte, height uint32, releaseRef bool) Ref3 {
	for i, r := range modRefs {
		if point, found, err := t.walkToArrowPoint(r, key, height); found {
			// Release reference if requested
			if releaseRef {
				modRefs = append(modRefs[:i], modRefs[i+1:]...)
				t.modifiedPages[pageId].roots = modRefs
				if pageId == 167 {
					fmt.Printf("returnModPage, released ref on page %d, left refs: %d\n", pageId, len(modRefs))
				}
			}
			return point
		} else if err != nil {
			//fmt.Printf("pageId: %d\n", pageId)
			//panic(err)
		}
	}
	return nil
}

func (t *Avl3) deserialisePage(pageId PageID, key []byte, height uint32, treeIndex uint32, releaseRef bool) Ref3 {
	trace := t.trace
	if trace {
		fmt.Printf("Deserialising page %d %s %d\n", pageId, key, height)
	}
	if pageId == 167 {
		fmt.Printf("deserialisePage on page %d, key %x, height %d, releaseRef %t\n", pageId, key, height, releaseRef)
	}
	modPage, modPageFound := t.modifiedPages[pageId]
	if modPageFound {
		if pageId == 167 {
			fmt.Printf("deserialisePage on page %d, key %x, height %d, releaseRef %t, found modePage\n", pageId, key, height, releaseRef)
		}
		return t.returnModPage(pageId, modPage.roots, key, height, releaseRef)
	}
	var data []byte
	if t.pageFile != nil {
		if cached, ok := t.pageCache.Get(pageId); ok {
			data = cached.([]byte)
			t.pageCacheHits++
		} else {
			if d, ok1 := t.commitMap[pageId]; ok1 {
				data = d
				t.pageCacheHits++
			} else {
				data = make([]byte, PageSize)
				if _, err := t.pageFile.ReadAt(data, int64(pageId)*int64(PageSize)); err != nil && err != io.EOF {
					panic(err)
				}
				t.pageCacheMisses++
				if data != nil {
					t.pageCache.Add(pageId, data)
				}
			}
		}
	} else {
		data = t.pageMap[pageId]
	}
	if data == nil {
		fmt.Printf("nil data\n")
		return nil
	}
	offset := uint32(0)
	extensions := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if extensions > 0 {
		d := make([]byte, (1+extensions)*PageSize)
		copy(d, data)
		data = d
	}
	for i := 0; i < int(extensions); i++ {
		extId := PageID(binary.BigEndian.Uint64(data[offset:]))
		var extData []byte
		if t.pageFile != nil {
			if cached, ok := t.pageCache.Get(extId); ok {
				extData = cached.([]byte)
				t.pageCacheHits++
			} else {
				if d, ok1 := t.commitMap[extId]; ok1 {
					extData = d
					t.pageCacheHits++
				} else {
					extData = make([]byte, PageSize)
					if _, err := t.pageFile.ReadAt(extData, int64(extId)*int64(PageSize)); err != nil && err != io.EOF {
						panic(err)
					}
					t.pageCacheMisses++
					if extData != nil {
						t.pageCache.Add(extId, extData)
					}
				}
			}
		} else {
			extData = t.pageMap[extId]
		}
		copy(data[(i+1)*int(PageSize):], extData)
		if releaseRef {
			if extId == 167 {
				fmt.Printf("Recycle extId %d\n", pageId)
			}
			t.modifiedPages[extId] = &ModPage{} // To make sure this page Id gets recycled
		}
		offset += 8
	}
	// read node count
	nodeCount := binary.BigEndian.Uint32(data[offset:])
	if nodeCount == 0 {
		fmt.Printf("nodeCount == 0\n")
		return nil
	}
	offset += 4
	arrowBitsOffset := offset
	// Calculate number of arrows
	var arrowCount uint32
	arrowBitsLen := 4*((nodeCount+31)/32)
	for i := uint32(0); i < arrowBitsLen; i += 4 {
		arrowCount += uint32(bits.OnesCount32(binary.BigEndian.Uint32(data[arrowBitsOffset+i:])))
	}
	offset += arrowBitsLen
	leafCount := nodeCount - arrowCount
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
	offset += (12+t.hashLength)*arrowCount
	valueHeaderOffset := offset
	offset += 4*leafCount
	structBitsOffset := offset
	if trace {
		//fmt.Printf("StructBitsOffset %d\n", structBitsOffset)
	}
	if trace {
		//fmt.Printf("valBodyOffset %d\n", valBodyOffset)
	}
	if trace {
		//fmt.Printf("valueHeaderOffset %d\n", valueHeaderOffset)
	}
	var point Ref3
	var pins []Ref3
	var forkStack []Ref3
	var pinnedStack []bool
	var freeStack []bool
	var stackTop int
	var nodeIndex uint32
	var structBit uint32
	var noLeaf bool
	var maxStructBits uint32 = (prefixOffset - structBitsOffset) << 3
	for nodeIndex < nodeCount || structBit < maxStructBits  {
		sbit := (data[structBitsOffset+(structBit>>3)] & (uint8(1)<<(structBit&7))) != 0
		if !sbit && nodeIndex == nodeCount {
			// We got to the padding "0" bits of the structure
			break
		}
		// Interpret the structural bit
		var r Ref3
		if noLeaf {
			if sbit {
				stackTop--
				right := forkStack[stackTop]
				rPinned := pinnedStack[stackTop]
				rFree := freeStack[stackTop]
				left := forkStack[stackTop-1]
				lPinned := pinnedStack[stackTop-1]
				lFree := freeStack[stackTop-1]
				y := &Fork3{left: left, right: right, height: 1+maxu32(left.getheight(), right.getheight()), max: right.getmax()}
				if y.height == height && t.compare(y.max, key) == 0 {
					point = y
				}
				if rPinned && !lPinned {
					pins = append(pins, right)
					switch rt := right.(type) {
					case *Leaf3:
						rt.pinnedPageId = pageId
					case *Fork3:
						rt.pinnedPageId = pageId
					}
				} else if lPinned && !rPinned {
					pins = append(pins, left)
					switch lt := left.(type) {
					case *Leaf3:
						lt.pinnedPageId = pageId
					case *Fork3:
						lt.pinnedPageId = pageId
					}
				}
				forkStack[stackTop-1] = y
				freeStack[stackTop-1] = lFree && rFree
				pinnedStack[stackTop-1] = lPinned && rPinned
				noLeaf = true
			} else {
				noLeaf = false
			}
		} else {
			isPage := (data[arrowBitsOffset+(nodeIndex>>3)] & (uint8(1)<<(nodeIndex&7))) != 0
			if isPage {
				id := PageID(binary.BigEndian.Uint64(data[arrowHeaderOffset:]))
				arrowHeaderOffset += 8
				height := binary.BigEndian.Uint32(data[arrowHeaderOffset:])
				arrowHeaderOffset += 4
				// TODO read the page hash
				arrowHeaderOffset += t.hashLength
				max := t.deserialiseKey(data, &keyHeaderOffset, prefix)
				arrow := &Arrow3{pageId: id, height: height, max: max}
				r = arrow
			} else {
				l := &Leaf3{}
				l.key = t.deserialiseKey(data, &keyHeaderOffset, prefix)
				l.value, l.valueId, l.valueLen = t.deserialiseVal(data, &valueHeaderOffset, &valBodyOffset)
				if height == 1 && t.compare(l.key, key) == 0 {
					point = l
				}
				r = l
			}
			nodeIndex++
			noLeaf = true
		}
		if r != nil {
			// Push onto the stack
			if stackTop >= len(forkStack) {
				forkStack = append(forkStack, r)
				pinnedStack = append(pinnedStack, !sbit)
				freeStack = append(freeStack, sbit)
			} else {
				forkStack[stackTop] = r
				pinnedStack[stackTop] = !sbit
				freeStack[stackTop] = sbit
			}
			stackTop++
		}
		structBit++
	}
	if pageId == 167 {
		fmt.Printf("deserialisePage on page %d, nodeCount %d, roots %d, releaseRef %t\n", pageId, nodeCount, stackTop, releaseRef)
	}
	for i := 0; i < stackTop; i++ {
		if pinnedStack[i] {
			pins = append(pins, forkStack[i])
			switch rt := forkStack[i].(type) {
				case *Leaf3:
					rt.pinnedPageId = pageId
				case *Fork3:
					rt.pinnedPageId = pageId
			}
		}
	}
	if releaseRef {
		var modRefs []Ref3
		for i := 0; i < stackTop; i++ {
			modRefs = append(modRefs, forkStack[i])
		}
		if len(modRefs) > 0 {
			t.modifiedPages[pageId] = &ModPage{roots: modRefs, pins: pins}
		}
		return t.returnModPage(pageId, modRefs, key, height, releaseRef)
	}
	if key == nil && height == 0 && point == nil {
		point = forkStack[treeIndex]
	}
	return point
}

// Checks whether WBT without pages is equivalent to one with pages
func equivalent33(t *Avl3, path string, r1 Ref3, r2 Ref3) bool {
	switch r2 := r2.(type) {
	case nil:
		if r1 != nil {
			fmt.Printf("At path %s, expected n1 nil, but it was %s\n", path, r1.nkey())
			return false
		}
		return true
	case *Leaf3:
		if l1, ok := r1.(*Leaf3); ok {
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
	case *Fork3:
		if t.trace {
			fmt.Printf("equivalent33 path %s, at fork %s, height %d\n", path, r2.max, r2.height)
		}
		if f1, ok := r1.(*Fork3); ok {
			if !bytes.Equal(f1.max, r2.max) {
				fmt.Printf("At path %s, f1.max %s, r2.max %s\n", path, f1.max, r2.max)
				return false
			}
			if f1.height != r2.height {
				fmt.Printf("At path %s, f1.height %d, r2.height %d\n", path, f1.height, r2.height)
				return false
			}
			eqL := equivalent33(t, path + "l", f1.left, r2.left)
			eqR := equivalent33(t, path + "r", f1.right, r2.right)
			return eqL && eqR
		}
	case *Arrow3:
		if t.trace {
			fmt.Printf("equivalent33 path %s, at arrow P.%d[%s], height %d\n", path, r2.pageId, r2.max, r2.height)
		}
		if !bytes.Equal(r1.getmax(), r2.max) {
			fmt.Printf("At path %s, r1.max %s, r2(arrow).max %s\n", path, r1.getmax(), r2.max)
			return false
		}
		if r1 != nil && r2 != nil && r1.getheight() != r2.height {
			fmt.Printf("At path %s, r1.height %d, r2(arrow).height %d\n", path, r1.getheight(), r2.height)
			return false
		}
		point := t.deserialisePage(r2.pageId, r2.max, r2.height, 0, false)
		if point == nil {
			panic("")
		}
		return equivalent33(t, path, r1, point)
	}
	return false
}

func (t *Avl3) PrintStats() {
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
	fmt.Printf("Total pages: %d, Mb %0.3f, solid used: Mb %0.3f, solid allocs: Mb %0.3f, mem alloc %0.3fMb, sys %0.3fMb\n",
		t.maxPageId,
		float64(t.maxPageId)*float64(PageSize)/1024.0/1024.0,
		float64(t.solidUsed)/1024.0/1024.0,
		float64(t.solidAlloc)/1024.0/1024.0,
		float64(m.Alloc)/1024.0/1024.0,
		float64(m.Alloc)/1024.0/1024.0,
		)
	fmt.Printf("Size of pageCache: %d, hit ratio: %0.3f\n", t.pageCache.Len(), float64(t.pageCacheHits)/float64(t.pageCacheHits+t.pageCacheMisses+1))
	fmt.Printf("Solid refs: %v, solid tree sizes: %v, pinned: %d, free: %d\n", t.solidRefs, t.solidTreeSizes, t.solidPinned, t.solidFree)
}

func (t *Avl3) scan(r Ref3, m map[PageID]struct{}) (int, int) {
	switch r := r.(type) {
	case *Leaf3:
		return 0, 1
	case *Fork3:
		lDepth, lLeaves := t.scan(r.left, m)
		rDepth, rLeaves := t.scan(r.right, m)
		if lDepth > rDepth {
			return lDepth, lLeaves + rLeaves
		} else {
			return rDepth, lLeaves + rLeaves
		}
	case *Arrow3:
		point := t.deserialisePage(r.pageId, r.max, r.height, 0, false)
		if _, ok := m[r.pageId]; !ok {
			m[r.pageId] = struct{}{}
		}
		depth, leaves := t.scan(point, m)
		return 1 + depth, leaves
	}
	return 0, 0
}

func (t *Avl3) Analyse() {
	fmt.Printf("Number of versions: %d, current version: %d\n", len(t.versions), t.currentVersion)
	var v Version
	for v = 0; v < t.currentVersion; v++ {
		version := t.versions[v]
		m := make(map[PageID]struct{})
		root := t.deserialisePage(version.pageId, nil, 0, version.treeIndex, false)
		depth, leaves := t.scan(root, m)
		if v < 1000000 {
			fmt.Printf("Version: %d, max depth: %d, pages: %d, leaves: %d\n", v, depth, len(m), leaves)
		}
	}
	/*
	var verdata [12]byte
	var lastVersion Version3
	for n, _ := t.verFile.ReadAt(verdata[:], int64(t.currentVersion)*int64(12)); n >0; n, _ = t.verFile.ReadAt(verdata[:], int64(t.currentVersion)*int64(12)) {
		lastVersion.pageId = PageID(binary.BigEndian.Uint64(verdata[:]))
		lastVersion.treeIndex = binary.BigEndian.Uint32(verdata[8:])
		t.versions[t.currentVersion] = lastVersion
		t.currentVersion++
	}
	if t.currentVersion > 0 {
		t.maxPageId = lastVersion.pageId
		fmt.Printf("Deserialising page %d\n", lastVersion.pageId)
		root := t.deserialisePage(lastVersion.pageId, nil, 0, lastVersion.treeIndex, false)
		prevRootArrow := &Arrow3{height: root.getheight(), max: root.getmax()}
		t.root = &Arrow3{pageId: lastVersion.pageId, height: root.getheight(), max: root.getmax(), arrow: prevRootArrow}
		t.prevRoot = prevRootArrow
	}
	*/
}