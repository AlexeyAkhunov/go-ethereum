package state

import (

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
	return nil, nil
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