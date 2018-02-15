package avl

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"os"
	"os/exec"
)

const (
	strChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" // 62 characters
)

func randChars(r *rand.Rand, length int) []byte {
	chars := []byte{}
MAIN_LOOP:
	for {
		val := r.Int63()
		for i := 0; i < 10; i++ {
			v := int(val & 0x3f) // rightmost 6 bits
			if v >= 62 {         // only 62 characters in strChars
				val >>= 6
				continue
			} else {
				chars = append(chars, strChars[v])
				if len(chars) == length {
					break MAIN_LOOP
				}
				val >>= 6
			}
		}
	}

	return chars
}

func snapGraph(tr Graphable, name string) {
	f, err := os.Create(name + ".dot")
	check(err)
	check(DotGraph(name, tr, f))
	check(f.Close())
	cmd := exec.Command("dot", "-Tpng", "-o", name + ".png", name + ".dot")
	check(cmd.Run())
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestInsertRandom1Commit(t *testing.T) {
	source := rand.NewSource(67585940305)
	r := rand.New(source)
	tr := NewAvl1()
	var keys [][]byte
	var values [][]byte
	for i := 0; i < 100000; i++ {
		key := randChars(r, 16)
		value := randChars(r, 32)
		tr.Insert(key, value)
		keys = append(keys, key)
		values = append(values, value)
		// Redundant inserts
		j := r.Int63n(int64(len(keys)))
		tr.Insert(keys[j], values[j])
		// Replaces
		k := r.Int63n(int64(len(keys)))
		value = randChars(r, 32)
		tr.Insert(keys[k], value)
		values[k] = value
		if i < 10000 {
			if _, ok := tr.buffer.heightsCorrect(""); !ok {
				t.Errorf("height fields are incorrect after step %d", i)
				break
			}
			if !tr.buffer.balanceCorrect() {
				t.Errorf("tree is not balanced after step %d", i)
				break
			}
		}
	}
	if _, ok := tr.buffer.heightsCorrect(""); !ok {
		t.Errorf("height fields are incorrect")
	}
	if !tr.buffer.balanceCorrect() {
		t.Errorf("tree is not balanced")
	}
	tr.Commit()
	for i, key := range keys {
		value := values[i]
		v, found := tr.Get(key)
		if !found {
			t.Errorf("Did not find inserted value")
		} else if !bytes.Equal(v, value) {
			t.Errorf("Found wrong value %s, expected %s", v, value)
		}
	}
}

func TestInsertDeleteRandom1(t *testing.T) {
	source := rand.NewSource(586937474955600644)
	r := rand.New(source)
	tr := NewAvl1()
	var keys [][]byte
	var values [][]byte
	deleted := make(map[string]struct{})
	for i := 0; i < 100000; i++ {
		key := randChars(r, 16)
		value := randChars(r, 32)
		tr.Insert(key, value)
		keys = append(keys, key)
		values = append(values, value)
		// Deletes
		if i % 10 == 0 {
			k := r.Int63n(int64(len(keys)))
			tr.Delete(keys[k])
			deleted[string(keys[k])] = struct{}{}
		}
		if tr.buffer != nil && i < 10000 {
			if _, ok := tr.buffer.heightsCorrect(""); !ok {
				t.Errorf("height fields are incorrect after step %d", i)
				break
			}
			if !tr.buffer.balanceCorrect() {
				t.Errorf("tree is not balanced after step %d", i)
				break
			}
		}
	}
	if _, ok := tr.buffer.heightsCorrect(""); !ok {
		t.Errorf("height fields are incorrect")
	}
	if !tr.buffer.balanceCorrect() {
		t.Errorf("tree is not balanced")
	}
	for i, key := range keys {
		value := values[i]
		v, found := tr.Get(key)
		_, del := deleted[string(key)]
		if del {
			continue
		}
		if !found {
			if !del {
				t.Errorf("Did not find inserted value %s", key)
				break
			}
		} else if !bytes.Equal(v, value) {
			t.Errorf("Found wrong value")
			break
		}
	}
}

func TestInsertDeleteStage1(t *testing.T) {
	source := rand.NewSource(586937474955600644)
	r := rand.New(source)
	tr1 := NewAvl1()
	tr2 := NewAvl1()
	var keys [][]byte
	var values [][]byte
	deleted := make(map[string]struct{})
	for i := 0; i < 100000; i++ {
		key := randChars(r, 16)
		valLen := int(r.Int63n(128)+1)
		value := randChars(r, valLen)
		tr1.Insert(key, value)
		tr2.Insert(key, value)
		keys = append(keys, key)
		values = append(values, value)
		// Replaces
		j := r.Int63n(int64(len(keys)))
		// Fresh value
		valLen = int(r.Int63n(128)+1)
		value = randChars(r, valLen)
		tr1.Insert(keys[j], value)
		tr2.Insert(keys[j], value)
		values[j] = value
		if _, ok := deleted[string(keys[j])]; ok {
			delete(deleted, string(keys[j]))
		}
		// Deletes
		if i % 10 == 0 {
			k := r.Int63n(int64(len(keys)))
			tr1.Delete(keys[k])
			tr2.Delete(keys[k])
			deleted[string(keys[k])] = struct{}{}
		}
		if i % 1573 == 0 {
			tr2.Commit()
		}
		if i < 10000 {
			if tr1.buffer != nil {
				if _, ok := tr1.buffer.heightsCorrect(""); !ok {
					t.Errorf("height fields are incorrect after step %d", i)
					break
				}
				if !tr1.buffer.balanceCorrect() {
					t.Errorf("tree is not balanced after step %d", i)
					break
				}
			}
			if !equivalent11(tr2, "", tr1.buffer, tr2.buffer) {
				t.Errorf("Not equivalent(11) after step %d", i)
				break
			}
		}
	}
	if !equivalent11(tr2, "", tr1.buffer, tr2.buffer) {
		t.Errorf("Not equivalent (11)")
	}
	tr2.Commit()
}

func TestInsertRandom2Commit(t *testing.T) {
	source := rand.NewSource(67585947470305)
	r := rand.New(source)
	tr := NewAvl2()
	var keys [][]byte
	var values [][]byte
	for i := 0; i < 100000; i++ {
		key := randChars(r, 16)
		value := randChars(r, 32)
		tr.Insert(key, value)
		keys = append(keys, key)
		values = append(values, value)
		// Redundant inserts
		j := r.Int63n(int64(len(keys)))
		tr.Insert(keys[j], values[j])
		// Replaces
		k := r.Int63n(int64(len(keys)))
		value = randChars(r, 32)
		tr.Insert(keys[k], value)
		values[k] = value
		if i < 10000 {
			if _, ok := tr.root.heightsCorrect(""); !ok {
				t.Errorf("height fields are incorrect after step %d", i)
				break
			}
			if !tr.root.balanceCorrect() {
				t.Errorf("tree is not balanced after step %d", i)
				break
			}
		}
	}
	if _, ok := tr.root.heightsCorrect(""); !ok {
		t.Errorf("height fields are incorrect")
	}
	if !tr.root.balanceCorrect() {
		t.Errorf("tree is not balanced")
	}
	tr.Commit()
	for i, key := range keys {
		value := values[i]
		v, found := tr.Get(key)
		if !found {
			t.Errorf("Did not find inserted value")
		} else if !bytes.Equal(v, value) {
			t.Errorf("Found wrong value %s, expected %s", v, value)
		}
	}
}

func TestInsertRandom2Commits(t *testing.T) {
	source := rand.NewSource(67585947470305)
	r := rand.New(source)
	tr := NewAvl2()
	var keys [][]byte
	var values [][]byte
	for i := 0; i < 100000; i++ {
		key := randChars(r, 16)
		value := randChars(r, 32)
		tr.Insert(key, value)
		keys = append(keys, key)
		values = append(values, value)
		// Redundant inserts
		j := r.Int63n(int64(len(keys)))
		tr.Insert(keys[j], values[j])
		// Replaces
		k := r.Int63n(int64(len(keys)))
		value = randChars(r, 32)
		tr.Insert(keys[k], value)
		values[k] = value
		if i % 103 == 0 {
			tr.Commit()
		}
		if i < 10000 {
			if _, ok := tr.root.heightsCorrect(""); !ok {
				t.Errorf("height fields are incorrect after step %d", i)
				break
			}
			if !tr.root.balanceCorrect() {
				t.Errorf("tree is not balanced after step %d", i)
				break
			}
		}
	}
	if _, ok := tr.root.heightsCorrect(""); !ok {
		t.Errorf("height fields are incorrect")
	}
	if !tr.root.balanceCorrect() {
		t.Errorf("tree is not balanced")
	}
	tr.Commit()
	for i, key := range keys {
		value := values[i]
		v, found := tr.Get(key)
		if !found {
			t.Errorf("Did not find inserted value")
		} else if !bytes.Equal(v, value) {
			t.Errorf("Found wrong value %s, expected %s", v, value)
		}
	}
}

func TestInsertDeleteRandom2(t *testing.T) {
	source := rand.NewSource(5869374758495600644)
	r := rand.New(source)
	tr := NewAvl2()
	var keys [][]byte
	var values [][]byte
	deleted := make(map[string]struct{})
	for i := 0; i < 100000; i++ {
		key := randChars(r, 16)
		value := randChars(r, 32)
		tr.Insert(key, value)
		keys = append(keys, key)
		values = append(values, value)
		// Deletes
		if i % 10 == 0 {
			k := r.Int63n(int64(len(keys)))
			tr.Delete(keys[k])
			deleted[string(keys[k])] = struct{}{}
		}
		if tr.root != nil && i < 10000 {
			if _, ok := tr.root.heightsCorrect(""); !ok {
				t.Errorf("height fields are incorrect after step %d", i)
				break
			}
			if !tr.root.balanceCorrect() {
				t.Errorf("tree is not balanced after step %d", i)
				break
			}
		}
	}
	if _, ok := tr.root.heightsCorrect(""); !ok {
		t.Errorf("height fields are incorrect")
	}
	if !tr.root.balanceCorrect() {
		t.Errorf("tree is not balanced")
	}
	for i, key := range keys {
		value := values[i]
		v, found := tr.Get(key)
		_, del := deleted[string(key)]
		if del {
			continue
		}
		if !found {
			if !del {
				t.Errorf("Did not find inserted value %s", key)
				break
			}
		} else if !bytes.Equal(v, value) {
			t.Errorf("Found wrong value")
			break
		}
	}
}

func TestInsertDeleteRandom2Commits(t *testing.T) {
	source := rand.NewSource(5869374758495600644)
	r := rand.New(source)
	tr := NewAvl2()
	var keys [][]byte
	var values [][]byte
	deleted := make(map[string]struct{})
	for i := 0; i < 100000; i++ {
		key := randChars(r, 16)
		value := randChars(r, 32)
		tr.Insert(key, value)
		keys = append(keys, key)
		values = append(values, value)
		// Deletes
		if i % 10 == 0 {
			k := r.Int63n(int64(len(keys)))
			tr.Delete(keys[k])
			deleted[string(keys[k])] = struct{}{}
		}
		if i % 1013 == 0 {
			tr.Commit()
		}
		if tr.root != nil && i < 10000 {
			if _, ok := tr.root.heightsCorrect(""); !ok {
				t.Errorf("height fields are incorrect after step %d", i)
				break
			}
			if !tr.root.balanceCorrect() {
				t.Errorf("tree is not balanced after step %d", i)
				break
			}
		}
		fmt.Printf("Completed step %d\n", i)
	}
	if _, ok := tr.root.heightsCorrect(""); !ok {
		t.Errorf("height fields are incorrect")
	}
	if !tr.root.balanceCorrect() {
		t.Errorf("tree is not balanced")
	}
	for i, key := range keys {
		value := values[i]
		v, found := tr.Get(key)
		_, del := deleted[string(key)]
		if del {
			continue
		}
		if !found {
			if !del {
				t.Errorf("Did not find inserted value %s", key)
				break
			}
		} else if !bytes.Equal(v, value) {
			t.Errorf("Found wrong value")
			break
		}
	}
}

func TestInsertDeleteRandom3Commits(t *testing.T) {
	source := rand.NewSource(5693747584943356006)
	r := rand.New(source)
	tr := NewAvl3()
	var keys [][]byte
	var values [][]byte
	deleted := make(map[string]struct{})
	for i := 0; i < 1000000; i++ {
		if i == 2040 {
			//tr.SetTracing(true)
		}
		key := randChars(r, 16)
		value := randChars(r, 32)
		tr.Insert(key, value)
		keys = append(keys, key)
		values = append(values, value)
		// Deletes
		if i % 10 == 0 {
			k := r.Int63n(int64(len(keys)))
			tr.Delete(keys[k])
			deleted[string(keys[k])] = struct{}{}
		}
		if i % 1013 == 0 {
			tr.Commit()
		}
		if i == 2040 {
			//tr.SetTracing(false)
		}
		if tr.root != nil && i < 10000 {
			if _, ok := tr.root.heightsCorrect(""); !ok {
				t.Errorf("height fields are incorrect after step %d", i)
				break
			}
			if !tr.root.balanceCorrect() {
				t.Errorf("tree is not balanced after step %d", i)
				break
			}
		}
		if i % 1000 == 0 {
			fmt.Printf("Completed step %d\n", i)
		}
	}
	if _, ok := tr.root.heightsCorrect(""); !ok {
		t.Errorf("height fields are incorrect")
	}
	if !tr.root.balanceCorrect() {
		t.Errorf("tree is not balanced")
	}
	for i, key := range keys {
		value := values[i]
		v, found := tr.Get(key)
		_, del := deleted[string(key)]
		if del {
			continue
		}
		if !found {
			if !del {
				t.Errorf("Did not find inserted value %s", key)
				break
			}
		} else if !bytes.Equal(v, value) {
			t.Errorf("Found wrong value")
			break
		}
	}
}