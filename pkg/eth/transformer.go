// VulcanizeDB
// Copyright © 2019 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"fmt"
	"math/big"

	"github.com/sirupsen/logrus"

	"github.com/ethereum/go-ethereum/core/state"
	node "github.com/ipfs/go-ipld-format"
	"github.com/jmoiron/sqlx"
	"github.com/multiformats/go-multihash"
	"github.com/vulcanize/ipld-eth-indexer/pkg/ipfs/ipld"
	"github.com/vulcanize/ipld-eth-indexer/pkg/postgres"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff"

	"github.com/vulcanize/ipld-eth-indexer/pkg/shared"
)

// Transformer interface to allow substitution of mocks for testing
type Transformer interface {
	Transform(workerID int, payload statediff.Payload) (int64, error)
}

// StateDiffTransformer satisfies the Transformer interface for ethereum statediff objects
type StateDiffTransformer struct {
	chainConfig *params.ChainConfig
	indexer     *CIDIndexer
}

// NewStateDiffTransformer creates a pointer to a new PayloadConverter which satisfies the PayloadConverter interface
func NewStateDiffTransformer(chainConfig *params.ChainConfig, db *postgres.DB) *StateDiffTransformer {
	return &StateDiffTransformer{
		chainConfig: chainConfig,
		indexer:     NewCIDIndexer(db),
	}
}

// Transform method is used to process statediff.Payload objects
// It performs the necessary data conversions and database persistence
func (sdt *StateDiffTransformer) Transform(workerID int, payload statediff.Payload) (int64, error) {
	// Unpack block rlp to access fields
	block := new(types.Block)
	if err := rlp.DecodeBytes(payload.BlockRlp, block); err != nil {
		return 0, err
	}
	blockHash := block.Hash()
	logrus.Infof("worker %d transforming state diff payload for blocknumber %d with hash %s", workerID, block.Number().Int64(), blockHash)
	transactions := block.Transactions()
	// Block processing
	// Decode receipts for this block
	receipts := make(types.Receipts, 0)
	if err := rlp.DecodeBytes(payload.ReceiptsRlp, &receipts); err != nil {
		return 0, err
	}
	// Derive any missing fields
	if err := receipts.DeriveFields(sdt.chainConfig, blockHash, block.NumberU64(), transactions); err != nil {
		return 0, err
	}
	// Generate the block iplds
	headerNode, uncleNodes, txNodes, txTrieNodes, rctNodes, rctTrieNodes, err := ipld.FromBlockAndReceipts(block, receipts)
	if err != nil {
		return 0, err
	}
	if len(txNodes) != len(txTrieNodes) && len(rctNodes) != len(rctTrieNodes) && len(txNodes) != len(rctNodes) {
		return 0, fmt.Errorf("expected number of transactions (%d), transaction trie nodes (%d), receipts (%d), and receipt trie nodes (%d)to be equal", len(txNodes), len(txTrieNodes), len(rctNodes), len(rctTrieNodes))
	}
	// Begin new db tx for everything
	tx, err := sdt.indexer.db.Beginx()
	if err != nil {
		return 0, err
	}
	defer func() {
		if p := recover(); p != nil {
			shared.Rollback(tx)
			panic(p)
		} else if err != nil {
			shared.Rollback(tx)
		} else {
			err = tx.Commit()
		}
	}()

	// Publish and index header, collect headerID
	reward := CalcEthBlockReward(block.Header(), block.Uncles(), block.Transactions(), receipts)
	headerID, err := sdt.processHeader(tx, block.Header(), headerNode, reward, payload.TotalDifficulty)
	if err != nil {
		return 0, err
	}
	// Publish and index uncles
	if err := sdt.processUncles(tx, headerID, block.Number().Int64(), uncleNodes); err != nil {
		return 0, err
	}
	// Publish and index receipts and txs
	if err := sdt.processReceiptsAndTxs(tx, processArgs{
		headerID:     headerID,
		blockNumber:  block.Number(),
		receipts:     receipts,
		txs:          transactions,
		rctNodes:     rctNodes,
		rctTrieNodes: rctTrieNodes,
		txNodes:      txNodes,
		txTrieNodes:  txTrieNodes,
	}); err != nil {
		return 0, err
	}

	// Unpack state diff rlp to access fields
	stateDiff := new(statediff.StateObject)
	if err := rlp.DecodeBytes(payload.StateObjectRlp, stateDiff); err != nil {
		return 0, err
	}
	// Publish and index state and storage nodes
	if err := sdt.processStateAndStorage(tx, headerID, stateDiff); err != nil {
		return 0, err
	}

	return block.Number().Int64(), err // return error explicity so that the defer() assigns to it
}

// processHeader publishes and indexes a header IPLD in Postgres
// it returns the headerID
func (sdt *StateDiffTransformer) processHeader(tx *sqlx.Tx, header *types.Header, headerNode node.Node, reward, td *big.Int) (int64, error) {
	// publish header
	if err := shared.PublishIPLD(tx, headerNode); err != nil {
		return 0, err
	}
	// index header
	return sdt.indexer.indexHeaderCID(tx, HeaderModel{
		CID:             headerNode.Cid().String(),
		MhKey:           shared.MultihashKeyFromCID(headerNode.Cid()),
		ParentHash:      header.ParentHash.String(),
		BlockNumber:     header.Number.String(),
		BlockHash:       header.Hash().String(),
		TotalDifficulty: td.String(),
		Reward:          reward.String(),
		Bloom:           header.Bloom.Bytes(),
		StateRoot:       header.Root.String(),
		RctRoot:         header.ReceiptHash.String(),
		TxRoot:          header.TxHash.String(),
		UncleRoot:       header.UncleHash.String(),
		Timestamp:       header.Time,
	})
}

func (sdt *StateDiffTransformer) processUncles(tx *sqlx.Tx, headerID, blockNumber int64, uncleNodes []*ipld.EthHeader) error {
	// publish and index uncles
	for _, uncleNode := range uncleNodes {
		if err := shared.PublishIPLD(tx, uncleNode); err != nil {
			return err
		}
		uncleReward := CalcUncleMinerReward(blockNumber, uncleNode.Number.Int64())
		uncle := UncleModel{
			CID:        uncleNode.Cid().String(),
			MhKey:      shared.MultihashKeyFromCID(uncleNode.Cid()),
			ParentHash: uncleNode.ParentHash.String(),
			BlockHash:  uncleNode.Hash().String(),
			Reward:     uncleReward.String(),
		}
		if err := sdt.indexer.indexUncleCID(tx, uncle, headerID); err != nil {
			return err
		}
	}
	return nil
}

// processArgs bundles arugments to processReceiptsAndTxs
type processArgs struct {
	headerID     int64
	blockNumber  *big.Int
	receipts     types.Receipts
	txs          types.Transactions
	rctNodes     []*ipld.EthReceipt
	rctTrieNodes []*ipld.EthRctTrie
	txNodes      []*ipld.EthTx
	txTrieNodes  []*ipld.EthTxTrie
}

// processReceiptsAndTxs publishes and indexes receipt and transaction IPLDs in Postgres
func (sdt *StateDiffTransformer) processReceiptsAndTxs(tx *sqlx.Tx, args processArgs) error {
	// Process receipts and txs
	signer := types.MakeSigner(sdt.chainConfig, args.blockNumber)
	for i, receipt := range args.receipts {
		// tx that corresponds with this receipt
		trx := args.txs[i]
		from, err := types.Sender(signer, trx)
		if err != nil {
			return err
		}

		// Publishing
		// publish trie nodes, these aren't indexed directly
		if err := shared.PublishIPLD(tx, args.txTrieNodes[i]); err != nil {
			return err
		}
		if err := shared.PublishIPLD(tx, args.rctTrieNodes[i]); err != nil {
			return err
		}
		// publish the txs and receipts
		txNode, rctNode := args.txNodes[i], args.rctNodes[i]
		if err := shared.PublishIPLD(tx, txNode); err != nil {
			return err
		}
		if err := shared.PublishIPLD(tx, rctNode); err != nil {
			return err
		}

		// Indexing
		// extract topic and contract data from the receipt for indexing
		topicSets := make([][]string, 4)
		mappedContracts := make(map[string]bool) // use map to avoid duplicate addresses
		for _, log := range receipt.Logs {
			for i, topic := range log.Topics {
				topicSets[i] = append(topicSets[i], topic.Hex())
			}
			mappedContracts[log.Address.String()] = true
		}
		// these are the contracts seen in the logs
		logContracts := make([]string, 0, len(mappedContracts))
		for addr := range mappedContracts {
			logContracts = append(logContracts, addr)
		}
		// this is the contract address if this receipt is for a contract creation tx
		contract := shared.HandleZeroAddr(receipt.ContractAddress)
		var contractHash string
		deployment := false
		if contract != "" {
			deployment = true
			contractHash = crypto.Keccak256Hash(common.HexToAddress(contract).Bytes()).String()
			// if tx is a contract deployment, publish the data (code)
			// codec doesn't matter in this case sine we are not interested in the cid and the db key is multihash-derived
			// TODO: THE DATA IS NOT DIRECTLY THE CONTRACT CODE; THERE IS A MISSING PROCESSING STEP HERE
			// the contractHash => contract code is not currently correct
			if _, err := shared.PublishRaw(tx, ipld.MEthStorageTrie, multihash.KECCAK_256, trx.Data()); err != nil {
				return err
			}
		}
		// index tx first so that the receipt can reference it by FK
		txModel := TxModel{
			Dst:        shared.HandleZeroAddrPointer(trx.To()),
			Src:        shared.HandleZeroAddr(from),
			TxHash:     trx.Hash().String(),
			Index:      int64(i),
			Data:       trx.Data(),
			Deployment: deployment,
			CID:        txNode.Cid().String(),
			MhKey:      shared.MultihashKeyFromCID(txNode.Cid()),
		}
		txID, err := sdt.indexer.indexTransactionCID(tx, txModel, args.headerID)
		if err != nil {
			return err
		}
		// index the receipt
		rctModel := ReceiptModel{
			Topic0s:      topicSets[0],
			Topic1s:      topicSets[1],
			Topic2s:      topicSets[2],
			Topic3s:      topicSets[3],
			Contract:     contract,
			ContractHash: contractHash,
			LogContracts: logContracts,
			CID:          rctNode.Cid().String(),
			MhKey:        shared.MultihashKeyFromCID(rctNode.Cid()),
		}
		if err := sdt.indexer.indexReceiptCID(tx, rctModel, txID); err != nil {
			return err
		}
	}
	return nil
}

// processStateAndStorage publishes and indexes state and storage nodes in Postgres
func (sdt *StateDiffTransformer) processStateAndStorage(tx *sqlx.Tx, headerID int64, stateDiff *statediff.StateObject) error {
	for _, stateNode := range stateDiff.Nodes {
		// publish the state node
		stateCIDStr, err := shared.PublishRaw(tx, ipld.MEthStateTrie, multihash.KECCAK_256, stateNode.NodeValue)
		if err != nil {
			return err
		}
		mhKey, _ := shared.MultihashKeyFromCIDString(stateCIDStr)
		stateModel := StateNodeModel{
			Path:     stateNode.Path,
			StateKey: common.BytesToHash(stateNode.LeafKey).String(),
			CID:      stateCIDStr,
			MhKey:    mhKey,
			NodeType: ResolveFromNodeType(stateNode.NodeType),
		}
		// index the state node, collect the stateID to reference by FK
		stateID, err := sdt.indexer.indexStateCID(tx, stateModel, headerID)
		if err != nil {
			return err
		}
		// if we have a leaf, decode and index the account data
		if stateNode.NodeType == statediff.Leaf {
			var i []interface{}
			if err := rlp.DecodeBytes(stateNode.NodeValue, &i); err != nil {
				return err
			}
			if len(i) != 2 {
				return fmt.Errorf("eth IPLDPublisher expected state leaf node rlp to decode into two elements")
			}
			var account state.Account
			if err := rlp.DecodeBytes(i[1].([]byte), &account); err != nil {
				return err
			}
			accountModel := StateAccountModel{
				Balance:     account.Balance.String(),
				Nonce:       account.Nonce,
				CodeHash:    account.CodeHash,
				StorageRoot: account.Root.String(),
			}
			if err := sdt.indexer.indexStateAccount(tx, accountModel, stateID); err != nil {
				return err
			}
		}
		// if there are any storage nodes associated with this node, publish and index them
		for _, storageNode := range stateNode.StorageNodes {
			storageCIDStr, err := shared.PublishRaw(tx, ipld.MEthStorageTrie, multihash.KECCAK_256, storageNode.NodeValue)
			if err != nil {
				return err
			}
			mhKey, _ := shared.MultihashKeyFromCIDString(storageCIDStr)
			storageModel := StorageNodeModel{
				Path:       storageNode.Path,
				StorageKey: common.BytesToHash(storageNode.LeafKey).String(),
				CID:        storageCIDStr,
				MhKey:      mhKey,
				NodeType:   ResolveFromNodeType(storageNode.NodeType),
			}
			if err := sdt.indexer.indexStorageCID(tx, storageModel, stateID); err != nil {
				return err
			}
		}
	}
	return nil
}
