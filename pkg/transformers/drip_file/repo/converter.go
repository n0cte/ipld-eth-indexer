// Copyright 2018 Vulcanize
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package repo

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
)

type Converter interface {
	ToModel(ethLog types.Log) (DripFileRepoModel, error)
}

type DripFileRepoConverter struct{}

func (DripFileRepoConverter) ToModel(ethLog types.Log) (DripFileRepoModel, error) {
	err := verifyLog(ethLog)
	if err != nil {
		return DripFileRepoModel{}, err
	}
	what := string(bytes.Trim(ethLog.Topics[2].Bytes(), "\x00"))
	data := big.NewInt(0).SetBytes(ethLog.Topics[3].Bytes()).String()
	raw, err := json.Marshal(ethLog)
	return DripFileRepoModel{
		What:             what,
		Data:             data,
		TransactionIndex: ethLog.TxIndex,
		Raw:              raw,
	}, err
}

func verifyLog(log types.Log) error {
	if len(log.Topics) < 4 {
		return errors.New("log missing topics")
	}
	return nil
}