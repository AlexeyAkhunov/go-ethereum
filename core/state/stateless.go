// Copyright 2019 The go-ethereum Authors
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

package state

import (
	"bytes"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

/* Proof Of Concept for verification of Stateless client proofs */
type Stateless struct {
	stateRoot common.Hash
	contracts []common.Address
	cMasks []uint32
	cHashes []common.Hash
	cShortKeys [][]byte
	cValues [][]byte
	masks []uint32
	hashes []common.Hash
	shortKeys [][]byte
	values [][]byte
	blockNr uint64
	t *trie.Trie
	storageTries map[common.Address]*trie.Trie
}

func NewStateless(stateRoot common.Hash,
	contracts []common.Address,
	cMasks []uint32,
	cHashes []common.Hash,
	cShortKeys [][]byte,
	cValues [][]byte,
	masks []uint32,
	hashes []common.Hash,
	shortKeys [][]byte,
	values [][]byte,
	blockNr uint64,
	trace bool,
) (*Stateless, error) {
	t, _, _, _, _ := trie.NewFromProofs(AccountsBucket, nil, false, masks, shortKeys, values, hashes, trace)
	if stateRoot != t.Hash() {
		filename := fmt.Sprintf("root_%d.txt", blockNr)
		f, err := os.Create(filename)
		if err == nil {
			defer f.Close()
			t.Print(f)
		}
		return nil, fmt.Errorf("Expected root: %x, Constructed root: %x", stateRoot, t.Hash())
	}
	storageTries := make(map[common.Address]*trie.Trie)
	var maskIdx, hashIdx, shortIdx, valueIdx int
	for _, contract := range contracts {
		st, mIdx, hIdx, sIdx, vIdx := trie.NewFromProofs(StorageBucket, nil, true, cMasks[maskIdx:], cShortKeys[shortIdx:], cValues[valueIdx:], cHashes[hashIdx:], trace)
		storageTries[contract] = st
		maskIdx += mIdx
		shortIdx += sIdx
		hashIdx += hIdx
		valueIdx += vIdx
	}
	return &Stateless {
		stateRoot: stateRoot,
		masks: masks,
		hashes: hashes,
		shortKeys: shortKeys,
		values: values,
		blockNr: blockNr,
		t: t,
		storageTries: storageTries,
	}, nil
}

func (s *Stateless) SetBlockNr(blockNr uint64) {
	s.blockNr = blockNr
}

func (s *Stateless) ReadAccountData(address common.Address) (*Account, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := s.t.TryGet(nil, buf[:], s.blockNr)
	if err != nil {
		return nil, err
	}
	return encodingToAccount(enc)
}

func (s *Stateless) getStorageTrie(address common.Address, create bool) (*trie.Trie, error) {
	t, ok := s.storageTries[address]
	if !ok && create {
		account, err := s.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if account == nil {
			t = trie.New(common.Hash{}, StorageBucket, address[:], true)
		} else {
			t = trie.New(account.Root, StorageBucket, address[:], true)
		}
		s.storageTries[address] = t
	}
	return t, nil
}

func (s *Stateless) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	fmt.Printf("ReadAccountStorage\n")
	t, err := s.getStorageTrie(address, false)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return []byte{}, nil
	}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write((*key)[:])
	var secKey common.Hash
	h.sha.Read(secKey[:]) 
	enc, err := t.TryGet(nil, secKey[:], s.blockNr)
	if err != nil {
		return nil, err
	}
	return enc, nil
}

func (s *Stateless) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	fmt.Printf("ReadAccountCode\n")
	return nil, nil
}

func (s *Stateless) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	fmt.Printf("ReadAccountCodeSize\n")
	return 0, nil
}

func (s *Stateless) UpdateAccountData(address common.Address, original, account *Account) error {
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
	}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	if account != nil {
		storageTrie, err := s.getStorageTrie(address, false)
		if err != nil {
			return err
		}
		if storageTrie != nil {
			account.Root = storageTrie.Hash()
		}
		data, err := rlp.EncodeToBytes(account)
		if err != nil {
			return err
		}
		err = s.t.TryUpdate(nil, addrHash[:], data, s.blockNr)
		if err != nil {
			return err
		}
	} else {
		err := s.t.TryDelete(nil, addrHash[:], s.blockNr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Stateless) CheckRoot(expected common.Hash) error {
	myRoot := s.t.Hash()
	if myRoot != expected {
		filename := fmt.Sprintf("root_%d.txt", s.blockNr+1)
		f, err := os.Create(filename)
		if err == nil {
			defer f.Close()
			s.t.Print(f)
		}
		return fmt.Errorf("Final root: %x, expected: %x", myRoot, expected)
	}
	return nil
}

func (s *Stateless) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	fmt.Printf("UpdateAccountCode\n")
	return nil
}

func (s *Stateless) DeleteAccount(address common.Address, original *Account) error {
	return nil
}

func (s *Stateless) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	fmt.Printf("WriteAccountStorage\n")
	if *original == *value {
		return nil
	}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write((*key)[:])
	var secKey common.Hash
	h.sha.Read(secKey[:])
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	t, err := s.getStorageTrie(address, true)
	if err != nil {
		return err
	}
	if len(v) == 0 {
		err = t.TryDelete(nil, secKey[:], s.blockNr)
	} else {
		err = t.TryUpdate(nil, secKey[:], vv, s.blockNr)
	}
	return err
}