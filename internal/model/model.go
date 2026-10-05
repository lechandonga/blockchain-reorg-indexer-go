package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Tx is a minimal on-chain transaction.
type Tx struct {
	Hash  string `json:"hash"`
	From  string `json:"from"`
	To    string `json:"to"`
	Value uint64 `json:"value"`
}

// Block is a minimal block produced by the local chain simulator.
// Hash must equal ComputeHash(Number, ParentHash, Transactions).
type Block struct {
	Number       uint64 `json:"number"`
	Hash         string `json:"hash"`
	ParentHash   string `json:"parent_hash"`
	Transactions []Tx   `json:"transactions"`
}

// GenesisHash is the parent hash of block 0.
const GenesisHash = "0xGENESIS"

// ComputeHash is the deterministic, tamper-evident hash of a block's content.
// Any mutation of number/parent/transactions changes the hash.
func ComputeHash(number uint64, parentHash string, txs []Tx) string {
	h := sha256.New()
	fmt.Fprintf(h, "n:%d|p:%s|t:%d", number, parentHash, len(txs))
	for i, tx := range txs {
		fmt.Fprintf(h, "|%d:%s>%s:%d:%s", i, tx.From, tx.To, tx.Value, tx.Hash)
	}
	return "0x" + hex.EncodeToString(h.Sum(nil))[:16]
}

// ValidHash reports whether the block's declared hash matches its content.
func (b *Block) ValidHash() bool {
	return b.Hash == ComputeHash(b.Number, b.ParentHash, b.Transactions)
}

// Encode serializes a block for immutable on-disk storage.
func (b *Block) Encode() ([]byte, error) {
	return json.Marshal(b)
}

// DecodeBlock parses a block and returns an error for malformed payloads.
func DecodeBlock(data []byte) (*Block, error) {
	var b Block
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("malformed block payload: %w", err)
	}
	return &b, nil
}
