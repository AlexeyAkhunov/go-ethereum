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

package ethapi

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
)

type PrivateTestAPI struct {
	b Backend
}

func NewPrivateTestAPI(b Backend) *PrivateTestAPI {
	return &PrivateTestAPI{b}
}

type GenConfParams struct {
	HomesteadForkBlock	math.HexOrDecimal64		`json:"homesteadForkBlock"`
}

type GenConfAccount struct {
}

type GenConfGenesis struct {
	Author 		common.Address			`json:"author"`
	Difficulty	*math.HexOrDecimal256	`json:"difficulty"`
	GasLimit    math.HexOrDecimal64     `json:"gasLimit"`
	Nonce		math.HexOrDecimal64		`json:"nonce"`
	ExtraData   hexutil.Bytes			`json:"extraData"`
	Timestamp	math.HexOrDecimal64		`json:"timestamp"`
	MixHash 	common.Hash 			`json:"mixHash"`
}

type GenesisConfig struct {
	SealEngine		string				`json:"sealEngine"`
	Params          *GenConfParams  	`json:"params"`
	Genesis         *GenConfGenesis 	`json:"genesis"`
	Accounts 		[]GenConfAccount 	`json:"accounts"`
}

func (s *PrivateTestAPI) SetChainParams(genesisConfig *GenesisConfig) {

}