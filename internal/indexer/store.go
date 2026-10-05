package indexer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// snapshot is the durable representation of the whole index. It is written
// atomically (tmp file + rename) after every accepted mutation, so a restart
// always resumes from the last fully committed state.
type snapshot struct {
	Chain   []Block             `json:"chain"`   // canonical blocks, genesis first
	Txs     map[string]TxRef    `json:"txs"`     // tx hash -> indexed tx
	ByAddr  map[string][]string `json:"by_addr"` // address -> tx hashes (canonical order)
	Balance map[string]int64    `json:"balance"` // address -> confirmed balance
}

// TxRef is an indexed transaction plus its canonical position.
type TxRef struct {
	Tx          `json:"tx"`
	BlockHash   string `json:"block_hash"`
	BlockHeight uint64 `json:"block_height"`
}

func loadSnapshot(dir string) (*snapshot, error) {
	path := filepath.Join(dir, "snapshot.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &snapshot{
				Txs:     map[string]TxRef{},
				ByAddr:  map[string][]string{},
				Balance: map[string]int64{},
			}, nil
		}
		return nil, err
	}
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("corrupt snapshot %s: %w", path, err)
	}
	if s.Txs == nil {
		s.Txs = map[string]TxRef{}
	}
	if s.ByAddr == nil {
		s.ByAddr = map[string][]string{}
	}
	if s.Balance == nil {
		s.Balance = map[string]int64{}
	}
	return &s, nil
}

func saveSnapshot(dir string, s *snapshot) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "snapshot.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "snapshot.json"))
}
