package indexer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// GenesisParentHash is the parent hash of the genesis block (height 0).
const GenesisParentHash = "GENESIS"

// Tx is a single transfer inside a block.
type Tx struct {
	Hash   string `json:"hash"`
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int64  `json:"amount"`
}

// Block is the unit of ingestion delivered by the (simulated) chain source.
type Block struct {
	Height    uint64 `json:"height"`
	Hash      string `json:"hash"`
	PrevHash  string `json:"prev_hash"`
	Timestamp int64  `json:"timestamp"`
	Txs       []Tx   `json:"txs"`
}

// ComputeHash derives the canonical hash of a block from its content.
func ComputeHash(height uint64, prevHash string, timestamp int64, txs []Tx) string {
	payload, _ := json.Marshal(struct {
		Height    uint64 `json:"height"`
		PrevHash  string `json:"prev_hash"`
		Timestamp int64  `json:"timestamp"`
		Txs       []Tx   `json:"txs"`
	}{height, prevHash, timestamp, txs})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// Validate checks structural integrity of a block. Any violation means the
// input is corrupted and must be rejected without touching the index.
func (b Block) Validate() error {
	if b.Height == 0 {
		if b.PrevHash != GenesisParentHash {
			return errors.New("genesis block must reference GenesisParentHash")
		}
	} else if b.PrevHash == "" || b.PrevHash == GenesisParentHash {
		return errors.New("non-genesis block has invalid parent reference")
	}
	if b.Hash == "" {
		return errors.New("missing block hash")
	}
	if want := ComputeHash(b.Height, b.PrevHash, b.Timestamp, b.Txs); want != b.Hash {
		return fmt.Errorf("hash mismatch: declared %s, computed %s", b.Hash, want)
	}
	seen := map[string]bool{}
	for _, tx := range b.Txs {
		if tx.Hash == "" || tx.From == "" || tx.To == "" {
			return errors.New("transaction with empty hash/from/to")
		}
		if tx.Amount <= 0 {
			return fmt.Errorf("transaction %s has non-positive amount", tx.Hash)
		}
		if seen[tx.Hash] {
			return fmt.Errorf("duplicate transaction %s inside block", tx.Hash)
		}
		seen[tx.Hash] = true
	}
	return nil
}

// MakeBlock is a test/demo helper that builds a valid, self-consistent block.
func MakeBlock(height uint64, prevHash string, timestamp int64, txs []Tx) Block {
	return Block{
		Height:    height,
		PrevHash:  prevHash,
		Timestamp: timestamp,
		Txs:       txs,
		Hash:      ComputeHash(height, prevHash, timestamp, txs),
	}
}
