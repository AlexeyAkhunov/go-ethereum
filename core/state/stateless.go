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
	storageTries map[common.Hash]*trie.Trie
	codeMap map[common.Hash][]byte
	trace bool
	storageUpdates map[common.Address]map[common.Hash][]byte
	accountUpdates map[common.Hash]*Account
	deleted map[common.Hash]struct{}
}

func NewStateless(stateRoot common.Hash,
	contracts []common.Address,
	cMasks []uint32,
	cHashes []common.Hash,
	cShortKeys [][]byte,
	cValues [][]byte,
	codes [][]byte,
	masks []uint32,
	hashes []common.Hash,
	shortKeys [][]byte,
	values [][]byte,
	blockNr uint64,
	trace bool,
) (*Stateless, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	if trace {
		fmt.Printf("ACCOUNT TRIE ==============================================\n")
	}
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
	storageTries := make(map[common.Hash]*trie.Trie)
	var maskIdx, hashIdx, shortIdx, valueIdx int
	for _, contract := range contracts {
		if trace {
			fmt.Printf("TRIE %x ==============================================\n", contract)
		}
		st, mIdx, hIdx, sIdx, vIdx := trie.NewFromProofs(StorageBucket, nil, true, cMasks[maskIdx:], cShortKeys[shortIdx:], cValues[valueIdx:], cHashes[hashIdx:], trace)
		h.sha.Reset()
		h.sha.Write(contract[:])
		var addrHash common.Hash
		h.sha.Read(addrHash[:])
		storageTries[addrHash] = st
		enc, err := t.TryGet(nil,  addrHash[:], blockNr)
		if err != nil {
			return nil, err
		}
		account, err := encodingToAccount(enc)
		if err != nil {
			return nil, err
		}
		if account.Root != st.Hash() {
			filename := fmt.Sprintf("root_%d.txt", blockNr)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close()
				st.Print(f)
			}
			return nil, fmt.Errorf("Expected storage root: %x, constructed root: %x", account.Root, st.Hash())
		}
		maskIdx += mIdx
		shortIdx += sIdx
		hashIdx += hIdx
		valueIdx += vIdx
	}
	codeMap := make(map[common.Hash][]byte)
	codeMap[common.BytesToHash(emptyCodeHash)] = []byte{}
	var codeHash common.Hash
	for _, code := range codes {
		h.sha.Reset()
		h.sha.Write(code)
		h.sha.Read(codeHash[:])
		codeMap[codeHash] = code
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
		codeMap: codeMap,
		trace: trace,
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Hash]*Account),
		deleted: make(map[common.Hash]struct{}),
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
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	enc, err := s.t.TryGet(nil,  addrHash[:], s.blockNr)
	if err != nil {
		return nil, err
	}
	return encodingToAccount(enc)
}

func (s *Stateless) getStorageTrie(address common.Address, addrHash common.Hash, create bool) (*trie.Trie, error) {
	t, ok := s.storageTries[addrHash]
	if !ok && create {
		t = trie.New(common.Hash{}, StorageBucket, address[:], true)
		s.storageTries[addrHash] = t
	}
	return t, nil
}

func (s *Stateless) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	//fmt.Printf("ReadAccountStorage\n")
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	t, err := s.getStorageTrie(common.Address{}, addrHash, false)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, nil
	}
	h.sha.Reset()
	h.sha.Write((*key)[:])
	var secKey common.Hash
	h.sha.Read(secKey[:])
	enc, err := t.TryGet(nil, secKey[:], s.blockNr)
	if err != nil {
		return nil, err
	}
	return common.CopyBytes(enc), nil
}

func (s *Stateless) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	//fmt.Printf("ReadAccountCode\n")
	return s.codeMap[codeHash], nil
}

func (s *Stateless) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	//fmt.Printf("ReadAccountCodeSize\n")
	return len(s.codeMap[codeHash]), nil
}

func (s *Stateless) UpdateAccountData(address common.Address, original, account *Account) error {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	s.accountUpdates[addrHash] = account
	if s.trace {
		fmt.Printf("UpdateeAccount %x, hash %x\n", address, addrHash)
	}
	return nil
}

func (s *Stateless) CheckRoot(expected common.Hash) error {
	h := newHasher()
	defer returnHasherToPool(h)
	// Process updates first, deletes next
	for address, m := range s.storageUpdates {
		h.sha.Reset()
		h.sha.Write(address[:])
		var addrHash common.Hash
		h.sha.Read(addrHash[:])
		t, err := s.getStorageTrie(address, addrHash, true)
		if err != nil {
			return err
		}
		for keyHash, v := range m {
			if len(v) != 0 {
				if err := t.TryUpdate(nil, keyHash[:], v, s.blockNr); err != nil {
					return err
				}
			}
		}
	}
	for address, m := range s.storageUpdates {
		h.sha.Reset()
		h.sha.Write(address[:])
		var addrHash common.Hash
		h.sha.Read(addrHash[:])
		t, err := s.getStorageTrie(address, addrHash, true)
		if err != nil {
			return err
		}
		for keyHash, v := range m {
			if len(v) == 0 {
				if err := t.TryDelete(nil, keyHash[:], s.blockNr); err != nil {
					return err
				}
			}
		}
	}
	// First process updates, then deletes
	for addrHash, account := range s.accountUpdates {
		if account != nil {
			storageTrie, err := s.getStorageTrie(common.Address{}, addrHash, false)
			if err != nil {
				return err
			}
			if _, ok := s.deleted[addrHash]; ok {
				account.Root = emptyRoot
			} else if storageTrie != nil {
				//fmt.Printf("Updating account.Root of %x with %x, emptyRoot: %x\n", address, storageTrie.Hash(), emptyRoot)
				account.Root = storageTrie.Hash()
			}
			data, err := rlp.EncodeToBytes(account)
			if err != nil {
				return err
			}
			if err := s.t.TryUpdate(nil, addrHash[:], data, s.blockNr); err != nil {
				return err
			}
		}
	}
	for addrHash, account := range s.accountUpdates {
		if account == nil {
			if err := s.t.TryDelete(nil, addrHash[:], s.blockNr); err != nil {
				return err
			}
		}
	}
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
	//fmt.Printf("UpdateAccountCode\n")
	s.codeMap[codeHash] = common.CopyBytes(code)
	return nil
}

func (s *Stateless) DeleteAccount(address common.Address, original *Account) error {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	s.accountUpdates[addrHash] = nil
	s.deleted[addrHash] = struct{}{}
	if s.trace {
		fmt.Printf("DeleteAccount %x, hash %x\n", address, addrHash)
	}
	return nil
}

func (s *Stateless) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	m, ok := s.storageUpdates[address]
	if !ok {
		m = make(map[common.Hash][]byte)
		s.storageUpdates[address] = m
	}
	//fmt.Printf("WriteAccountStorage\n")
	//if *original == *value {
	//	return nil
	//}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write((*key)[:])
	var secKey common.Hash
	h.sha.Read(secKey[:])
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	m[secKey] = vv
	if s.trace {
		fmt.Printf("WriteAccountStorage addr %x, keyHash %x, value %x\n", address, secKey, vv)
	}
	return nil
}