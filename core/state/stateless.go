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
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

/* Proof Of Concept for verification of Stateless client proofs */
type Stateless struct {
	stateRoot common.Hash
	masks []uint32
	hashes []common.Hash
	shortLens []int
	values [][]byte
	blockNr uint64
	valueIdx int // Current value index
}

func NewStateless(stateRoot common.Hash, masks []uint32, hashes []common.Hash, shortLens []int, values [][]byte, blockNr uint64) *Stateless {
	return &Stateless {
		stateRoot: stateRoot,
		masks: masks,
		hashes: hashes,
		shortLens: shortLens,
		values: values,
		blockNr: blockNr,
	}
}

func (s *Stateless) SetBlockNr(blockNr uint64) {
	s.blockNr = blockNr
}

func (s *Stateless) ReadAccountData(address common.Address) (*Account, error) {
	fmt.Printf("ReadAccountData, s.valueIdx %d\n", s.valueIdx)
	vi := s.valueIdx
	s.valueIdx++
	return encodingToAccount(s.values[vi])
}

func (s *Stateless) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	return nil, nil
}

func (s *Stateless) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	return nil, nil
}

func (s *Stateless) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	return 0, nil
}

func (s *Stateless) UpdateAccountData(address common.Address, original, account *Account) error {
	return nil
}

func (s *Stateless) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (s *Stateless) DeleteAccount(address common.Address, original *Account) error {
	return nil
}

func (s *Stateless) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	return nil
}