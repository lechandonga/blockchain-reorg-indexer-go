package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lechandonga/blockchain-reorg-indexer-go/internal/model"
)

// Link records which block occupies a height on the canonical chain.
type Link struct {
	Number     uint64 `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parent_hash"`
}

// TxRecord is an indexed transaction tied to its canonical block.
type TxRecord struct {
	TxHash      string `json:"tx_hash"`
	From        string `json:"from"`
	To          string `json:"to"`
	Value       uint64 `json:"value"`
	BlockNumber uint64 `json:"block_number"`
	BlockHash   string `json:"block_hash"`
}

// State is the full durable index state, replaced atomically on each commit.
type State struct {
	Tip     uint64     `json:"tip"`
	Chain   []Link     `json:"chain"`
	Records []TxRecord `json:"records"`
}

// Store persists the immutable block library and the canonical-chain state.
type Store struct {
	dir string

	mu    sync.RWMutex
	state State

	byHash    map[string]TxRecord
	byAddress map[string][]TxRecord

blocks map[string]*model.Block

logf func(format string, args ...any)
}

// New opens (or initializes) a store rooted at dir and replays persisted state.
func New(dir string, logf func(string, ...any)) (*Store, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if err := os.MkdirAll(filepath.Join(dir, "blocks"), 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	s := &Store{
		dir:       dir,
		byHash:    map[string]TxRecord{},
		byAddress: map[string][]TxRecord{},
		blocks:    map[string]*model.Block{},
		logf:      logf,
	}
	if err := s.loadBlocks(); err != nil {
		return nil, err
	}
	if err := s.loadState(); err != nil {
		return nil, err
	}
	s.rebuildIndexes()
	return s, nil
}

// SaveBlock stores a block immutably. A block hash has exactly one content.
func (s *Store) SaveBlock(b *model.Block) (bool, error) {
	path := filepath.Join(s.dir, "blocks", b.Hash+".json")
	if existing, err := os.ReadFile(path); err == nil {
		saved, derr := model.DecodeBlock(existing)
		if derr != nil || saved.Hash != b.Hash {
			return false, fmt.Errorf("corrupted block file for hash %s", b.Hash)
		}
		s.mu.Lock()
		s.blocks[b.Hash] = saved
		s.mu.Unlock()
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	data, err := b.Encode()
	if err != nil {
		return false, err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return false, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return false, err
	}
	s.mu.Lock()
	s.blocks[b.Hash] = b
	s.mu.Unlock()
	return true, nil
}

// Block returns a persisted block by hash.
func (s *Store) Block(hash string) (*model.Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.blocks[hash]
	return b, ok
}

// AllBlocks returns every persisted block (used for startup recovery).
func (s *Store) AllBlocks() []*model.Block {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Block, 0, len(s.blocks))
	for _, b := range s.blocks {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Number != out[j].Number {
			return out[i].Number < out[j].Number
		}
		return out[i].Hash < out[j].Hash
	})
	return out
}

// Tip returns the current canonical-chain tip height.
func (s *Store) Tip() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.Tip
}

// CanonicalHash returns the block hash occupying a canonical height.
func (s *Store) CanonicalHash(height uint64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if height >= uint64(len(s.state.Chain)) {
		return "", false
	}
	return s.state.Chain[height].Hash, true
}

// TxByHash looks up a transaction by hash on the canonical chain.
func (s *Store) TxByHash(hash string) (TxRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.byHash[hash]
	return r, ok
}

// TxByAddress returns transactions sent from or received by address,
// ordered by block height and transaction position.
func (s *Store) TxByAddress(addr string) []TxRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.byAddress[addr]
	out := make([]TxRecord, len(src))
	copy(out, src)
	return out
}

// Snapshot returns a deep copy of the current state (used by the reorg planner).
func (s *Store) Snapshot() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneState(s.state)
}

// ReplaceState installs a new state atomically: callers never observe a state
// that is half-rolled-back or half-filled. The switch is crash-safe via
// temp-file + atomic rename.
func (s *Store) ReplaceState(next *State) error {
	if err := validateChain(next); err != nil {
		return fmt.Errorf("refusing to persist invalid state: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.persist(next); err != nil {
		return err
	}
	s.state = cloneState(*next)
	s.rebuildIndexesLocked()
	return nil
}

func validateChain(st *State) error {
	if len(st.Chain) == 0 {
		if len(st.Records) != 0 {
			return errors.New("empty chain with tx records")
		}
		return nil
	}
	if int(st.Tip) != len(st.Chain)-1 {
		return fmt.Errorf("tip %d does not match chain length %d", st.Tip, len(st.Chain))
	}
	for i, link := range st.Chain {
		if link.Number != uint64(i) {
			return fmt.Errorf("chain gap at index %d (height %d)", i, link.Number)
		}
		if i == 0 {
			if link.ParentHash != model.GenesisHash {
				return fmt.Errorf("block 0 parent is %q, want %q", link.ParentHash, model.GenesisHash)
			}
			continue
		}
		if link.ParentHash != st.Chain[i-1].Hash {
			return fmt.Errorf("chain break at height %d", i)
		}
	}
	return nil
}

func cloneState(in State) State {
	out := State{Tip: in.Tip}
	if in.Chain != nil {
		out.Chain = append([]Link(nil), in.Chain...)
	}
	if in.Records != nil {
		out.Records = append([]TxRecord(nil), in.Records...)
	}
	return out
}

func (s *Store) rebuildIndexes() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebuildIndexesLocked()
}

func (s *Store) rebuildIndexesLocked() {
	s.byHash = map[string]TxRecord{}
	s.byAddress = map[string][]TxRecord{}
	for _, r := range s.state.Records {
		if _, dup := s.byHash[r.TxHash]; dup {
			// Records are built by the indexer from canonical blocks; a duplicate
			// here means an on-disk state nobody should have written.
			panic(fmt.Sprintf("duplicate tx record %s in persisted state", r.TxHash))
		}
		s.byHash[r.TxHash] = r
		s.byAddress[r.From] = append(s.byAddress[r.From], r)
		if r.To != r.From {
			s.byAddress[r.To] = append(s.byAddress[r.To], r)
		}
	}
}

func (s *Store) persist(st *State) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, "state.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Store) loadState() error {
	data, err := os.ReadFile(filepath.Join(s.dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("corrupted state.json: %w", err)
	}
	if err := validateChain(&st); err != nil {
	return fmt.Errorf("invalid persisted state: %w", err)
	}
	s.state = st
	return nil
}

func (s *Store) loadBlocks() error {
	dir := filepath.Join(s.dir, "blocks")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return err
		}
		b, err := model.DecodeBlock(data)
		if err != nil {
			return fmt.Errorf("block file %s: %w", e.Name(), err)
	}
		if !b.ValidHash() {
			return fmt.Errorf("block file %s: content hash mismatch", e.Name())
		}
		s.blocks[b.Hash] = b
	}
	return nil
}
