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
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

type BlockProof struct {
	Contracts []common.Address
	CMasks []uint16
	CHashes []common.Hash
	CShortKeys [][]byte
	CValues [][]byte
	Codes [][]byte
	Masks []uint16
	Hashes []common.Hash
	ShortKeys [][]byte
	Values [][]byte	
}

/* Proof Of Concept for verification of Stateless client proofs */
type Stateless struct {
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
	blockProof BlockProof,
	blockNr uint64,
	trace bool,
) (*Stateless, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	if trace {
		fmt.Printf("ACCOUNT TRIE ==============================================\n")
	}
	t, _, _, _, _ := trie.NewFromProofs(AccountsBucket, nil, false, blockProof.Masks, blockProof.ShortKeys, blockProof.Values, blockProof.Hashes, trace)
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
	for _, contract := range blockProof.Contracts {
		if trace {
			fmt.Printf("TRIE %x ==============================================\n", contract)
		}
		st, mIdx, hIdx, sIdx, vIdx := trie.NewFromProofs(StorageBucket, nil, true,
			blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:], trace)
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
			filename := fmt.Sprintf("root_%d.txt", blockNr-1)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close()
				st.Print(f)
			}
			return nil, fmt.Errorf("Expected storage root for %x: %x, constructed root: %x", contract, account.Root, st.Hash())
		}
		maskIdx += mIdx
		shortIdx += sIdx
		hashIdx += hIdx
		valueIdx += vIdx
	}
	codeMap := make(map[common.Hash][]byte)
	codeMap[common.BytesToHash(emptyCodeHash)] = []byte{}
	var codeHash common.Hash
	for _, code := range blockProof.Codes {
		h.sha.Reset()
		h.sha.Write(code)
		h.sha.Read(codeHash[:])
		codeMap[codeHash] = code
	}
	return &Stateless {
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

func (s *Stateless) ThinProof(blockProof BlockProof, cuttime uint64, trace bool) BlockProof {
	h := newHasher()
	defer returnHasherToPool(h)
	if trace {
		fmt.Printf("THIN\n")
	}
	var aMasks, acMasks []uint16
	var aShortKeys, acShortKeys [][]byte
	var aValues,acValues [][]byte
	var aHashes, acHashes []common.Hash
	_, _, _, _, aMasks, aShortKeys, aValues, aHashes = s.t.AmmendProofs(cuttime, blockProof.Masks, blockProof.ShortKeys, blockProof.Values, blockProof.Hashes,
		aMasks, aShortKeys, aValues, aHashes, trace)
	var maskIdx, hashIdx, shortIdx, valueIdx int
	aContracts := []common.Address{}
	for _, contract := range blockProof.Contracts {
		if trace {
			fmt.Printf("THIN TRIE %x ==============================================\n", contract)
		}
		h.sha.Reset()
		h.sha.Write(contract[:])
		var addrHash common.Hash
		h.sha.Read(addrHash[:])
		var st *trie.Trie
		var ok bool
		var mIdx, hIdx, sIdx, vIdx int
		if st, ok = s.storageTries[addrHash]; !ok {
			_, mIdx, hIdx, sIdx, vIdx = trie.NewFromProofs(StorageBucket, nil, true,
				blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:], trace)
			if mIdx > 0 {
				acMasks = append(acMasks, blockProof.CMasks[maskIdx:maskIdx+mIdx]...)
			}
			if sIdx > 0 {
				acShortKeys = append(acShortKeys, blockProof.CShortKeys[shortIdx:shortIdx+sIdx]...)
			}
			if vIdx > 0 {
				acValues = append(acValues, blockProof.CValues[valueIdx:valueIdx+vIdx]...)
			}
			if hIdx > 0 {
				acHashes = append(acHashes, blockProof.CHashes[hashIdx:hashIdx+hIdx]...)
			}
			aContracts = append(aContracts, contract)
		} else {
			mIdx, hIdx, sIdx, vIdx, acMasks, acShortKeys, acValues, acHashes = st.AmmendProofs(cuttime,
				blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:],
				acMasks, acShortKeys, acValues, acHashes,
				trace)
			aContracts = append(aContracts, contract)
		}
		maskIdx += mIdx
		shortIdx += sIdx
		hashIdx += hIdx
		valueIdx += vIdx
	}
	aCodes := [][]byte{}
	var codeHash common.Hash
	for _, code := range blockProof.Codes {
		h.sha.Reset()
		h.sha.Write(code)
		h.sha.Read(codeHash[:])
		if _, ok := s.codeMap[codeHash]; !ok {
			aCodes = append(aCodes, code)
		}
	}
	return BlockProof{aContracts, acMasks, acHashes, acShortKeys, acValues, aCodes, aMasks, aHashes, aShortKeys, aValues}
}

func (s *Stateless) ApplyProof(stateRoot common.Hash, blockProof BlockProof,
	blockNr uint64,
	trace bool,
) error {
	h := newHasher()
	defer returnHasherToPool(h)
	if len(blockProof.Masks) > 0 {
		s.t.ApplyProof(blockProof.Masks, blockProof.ShortKeys, blockProof.Values, blockProof.Hashes, trace)
		if stateRoot != s.t.Hash() {
			filename := fmt.Sprintf("root_%d.txt", blockNr)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close()
				s.t.Print(f)
			}
			return fmt.Errorf("[THIN] Expected root: %x, Constructed root: %x", stateRoot, s.t.Hash())
		}
	}
	var maskIdx, hashIdx, shortIdx, valueIdx int
	for _, contract := range blockProof.Contracts {
		if trace {
			fmt.Printf("TRIE %x ==============================================\n", contract)
		}
		h.sha.Reset()
		h.sha.Write(contract[:])
		var addrHash common.Hash
		h.sha.Read(addrHash[:])
		var st *trie.Trie
		var ok bool
		var mIdx, hIdx, sIdx, vIdx int
		if st, ok = s.storageTries[addrHash]; !ok {
			st, mIdx, hIdx, sIdx, vIdx = trie.NewFromProofs(StorageBucket, nil, true,
				blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:], trace)
			s.storageTries[addrHash] = st
		} else {
			mIdx, hIdx, sIdx, vIdx = st.ApplyProof(blockProof.CMasks[maskIdx:], blockProof.CShortKeys[shortIdx:], blockProof.CValues[valueIdx:], blockProof.CHashes[hashIdx:], trace)
		}
		enc, err := s.t.TryGet(nil,  addrHash[:], blockNr)
		if err != nil {
			return err
		}
		account, err := encodingToAccount(enc)
		if err != nil {
			return err
		}
		if account.Root != st.Hash() {
			filename := fmt.Sprintf("root_%d.txt", blockNr-1)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close()
				st.Print(f)
			}
			return fmt.Errorf("Expected storage root for %x: %x, constructed root: %x", contract, account.Root, st.Hash())
		}
		maskIdx += mIdx
		shortIdx += sIdx
		hashIdx += hIdx
		valueIdx += vIdx
	}
	var codeHash common.Hash
	for _, code := range blockProof.Codes {
		h.sha.Reset()
		h.sha.Write(code)
		h.sha.Read(codeHash[:])
		s.codeMap[codeHash] = code
	}
	return nil
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
	if code, ok := s.codeMap[codeHash]; ok {
		if s.trace {
			fmt.Printf("ReadAccountCode %x: %d\n", codeHash, len(code))
		}
		return code, nil
	} else {
		if s.trace {
			fmt.Printf("ReadAccountCode %x: nil\n", codeHash)
		}
		return nil, fmt.Errorf("Could not find code for codeHash %x\n", codeHash)
	}
}

func (s *Stateless) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	if code, ok := s.codeMap[codeHash]; ok {
		return len(code), nil
	} else {
		return 0, fmt.Errorf("Could not find code for codeHash %x\n", codeHash)
	}
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
		fmt.Printf("UpdateAccount %x, hash %x\n", address, addrHash)
	}
	return nil
}

func (s *Stateless) CheckRoot(expected common.Hash, check bool) error {
	h := newHasher()
	defer returnHasherToPool(h)
	// Process updates first, deletes next
	for address, m := range s.storageUpdates {
		h.sha.Reset()
		h.sha.Write(address[:])
		var addrHash common.Hash
		h.sha.Read(addrHash[:])
		if _, ok := s.deleted[addrHash]; ok {
			continue
		}
		t, err := s.getStorageTrie(address, addrHash, true)
		if err != nil {
			return err
		}
		hashes := make(Hashes, len(m))
		i := 0
		for keyHash, _ := range m {
			hashes[i] = keyHash
			i++
		}
		sort.Sort(hashes)
		for _, keyHash := range hashes {
			v := m[keyHash]
			var c *trie.TrieContinuation
			if len(v) != 0 {
				c = t.UpdateAction(keyHash[:], v)
			} else {
				c = t.DeleteAction(keyHash[:])
			}
			if !c.RunWithDb(nil, s.blockNr-1) {
				return fmt.Errorf("Unexpected resolution")
			}
		}
	}
	addrs := make(Hashes, len(s.accountUpdates))
	i := 0
	for addrHash, _ := range s.accountUpdates {
		addrs[i] = addrHash
		i++
	}
	sort.Sort(addrs)
	for _, addrHash := range addrs {
		account := s.accountUpdates[addrHash]
		deleteStorageTrie := false
		var c *trie.TrieContinuation
		if account != nil {
			storageTrie, err := s.getStorageTrie(common.Address{}, addrHash, false)
			if err != nil {
				return err
			}
			if _, ok := s.deleted[addrHash]; ok {
				account.Root = emptyRoot
				deleteStorageTrie = true
			} else if storageTrie != nil {
				//fmt.Printf("Updating account.Root of %x with %x, emptyRoot: %x\n", address, storageTrie.Hash(), emptyRoot)
				account.Root = storageTrie.Hash()
			}
			data, err := rlp.EncodeToBytes(account)
			if err != nil {
				return err
			}
			c = s.t.UpdateAction(addrHash[:], data)
		} else {
			deleteStorageTrie = true
			c = s.t.DeleteAction(addrHash[:])
		}
		if !c.RunWithDb(nil, s.blockNr-1) {
			return fmt.Errorf("Unexpected resolution")
		}
		if deleteStorageTrie {
			delete(s.storageTries, addrHash)
		}
	}
	if check {
		myRoot := s.t.Hash()
		if myRoot != expected {
			filename := fmt.Sprintf("root_%d.txt", s.blockNr)
			f, err := os.Create(filename)
			if err == nil {
				defer f.Close()
				s.t.Print(f)
			}
			return fmt.Errorf("Final root: %x, expected: %x", myRoot, expected)
		}
	}
	s.storageUpdates = make(map[common.Address]map[common.Hash][]byte)
	s.accountUpdates = make(map[common.Hash]*Account)
	s.deleted = make(map[common.Hash]struct{})
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