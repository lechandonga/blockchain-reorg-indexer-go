package indexer

import (
	"fmt"
	"log"
	"sync"
)

// Status classifies the outcome of ingesting one block so callers can tell
// apart duplicates, missing dependencies, corruption and reorg rejections.
type Status string

const (
	StatusApplied        Status = "applied"         // appended to the canonical tip
	StatusDuplicate      Status = "duplicate"       // already indexed, ignored
	StatusPending        Status = "pending"         // parent missing, buffered for retry
	StatusCorrupted      Status = "corrupted"       // failed integrity validation
	StatusReorgApplied   Status = "reorg_applied"   // replaced part of the canonical chain
	StatusReorgTooDeep   Status = "reorg_too_deep"  // fork point beyond MaxReorgDepth, rejected
	StatusHeightMismatch Status = "height_mismatch" // height inconsistent with parent
)

// Result is the per-block ingestion outcome.
type Result struct {
	Status Status
	Detail string
}

func (r Result) String() string {
	if r.Detail == "" {
		return string(r.Status)
	}
	return fmt.Sprintf("%s(%s)", r.Status, r.Detail)
}

// Config holds the tunables of an Indexer.
type Config struct {
	// DataDir is where the durable snapshot lives. Empty means in-memory only.
	DataDir string
	// MaxReorgDepth is the maximum number of canonical blocks a reorg may
	// roll back. Forks deeper than this are rejected and the index is left
	// untouched.
	MaxReorgDepth uint64
	// Logger receives human-readable pipeline events. Nil disables logging.
	Logger *log.Logger
}

// Indexer ingests blocks, maintains the canonical-chain index and serves
// consistent queries. A single RWMutex guards everything: a reorg (rollback
// + re-apply) completes inside one write lock, so concurrent readers never
// observe a half-rolled-back state.
type Indexer struct {
	cfg Config

	mu      sync.RWMutex
	chain   []Block          // canonical chain, genesis first
	byHash  map[string]int   // canonical block hash -> height index
	txs     map[string]TxRef // tx hash -> indexed tx
	byAddr  map[string][]string
	balance map[string]int64
	pending map[string]Block // blocks waiting for their parent, keyed by hash
}

// New opens (or creates) an indexer. Persisted progress is loaded from
// cfg.DataDir so already-processed blocks are never re-applied after restart.
func New(cfg Config) (*Indexer, error) {
	if cfg.MaxReorgDepth == 0 {
		cfg.MaxReorgDepth = 64
	}
	ix := &Indexer{
		cfg:     cfg,
		byHash:  map[string]int{},
		txs:     map[string]TxRef{},
		byAddr:  map[string][]string{},
		balance: map[string]int64{},
		pending: map[string]Block{},
	}
	if cfg.DataDir != "" {
		s, err := loadSnapshot(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		for i, b := range s.Chain {
			ix.byHash[b.Hash] = i
		}
		ix.chain = s.Chain
		ix.txs = s.Txs
		ix.byAddr = s.ByAddr
		ix.balance = s.Balance
		if len(ix.chain) > 0 {
			ix.logf("resumed from %s: height=%d tip=%s", cfg.DataDir, ix.Height(), ix.TipHash())
		}
	}
	return ix, nil
}

func (ix *Indexer) logf(format string, args ...any) {
	if ix.cfg.Logger != nil {
		ix.cfg.Logger.Printf(format, args...)
	}
}

// Height returns the current canonical tip height, or 0 when empty.
func (ix *Indexer) Height() uint64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if len(ix.chain) == 0 {
		return 0
	}
	return ix.chain[len(ix.chain)-1].Height
}

// TipHash returns the canonical tip hash, or "" when empty.
func (ix *Indexer) TipHash() string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if len(ix.chain) == 0 {
		return ""
	}
	return ix.chain[len(ix.chain)-1].Hash
}

// GetTx looks up an indexed transaction by hash.
func (ix *Indexer) GetTx(hash string) (TxRef, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	ref, ok := ix.txs[hash]
	return ref, ok
}

// TxsByAddress returns hashes of canonical transactions involving the address
// (as sender or recipient), in canonical order.
func (ix *Indexer) TxsByAddress(addr string) []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := make([]string, len(ix.byAddr[addr]))
	copy(out, ix.byAddr[addr])
	return out
}

// Balance returns the confirmed balance of an address.
func (ix *Indexer) Balance(addr string) int64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.balance[addr]
}

// CanonicalBlock returns the canonical block at a height.
func (ix *Indexer) CanonicalBlock(height uint64) (Block, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if int(height) >= len(ix.chain) {
		return Block{}, false
	}
	return ix.chain[height], true
}

// PendingCount reports how many blocks are buffered waiting for parents.
func (ix *Indexer) PendingCount() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.pending)
}

// Ingest processes one delivered block and returns a distinguishable result.
// The pipeline never aborts on bad input: corrupted or orphaned blocks are
// reported and the indexer keeps accepting further deliveries.
func (ix *Indexer) Ingest(b Block) Result {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	if err := b.Validate(); err != nil {
		ix.logf("reject corrupted block height=%d hash=%s: %v", b.Height, b.Hash, err)
		return Result{StatusCorrupted, err.Error()}
	}
	if _, ok := ix.byHash[b.Hash]; ok {
		return Result{StatusDuplicate, "block already indexed"}
	}
	if _, ok := ix.pending[b.Hash]; ok {
		return Result{StatusDuplicate, "block already pending"}
	}

	// Genesis.
	if b.Height == 0 {
		if len(ix.chain) != 0 {
			return Result{StatusHeightMismatch, "genesis delivered but chain is not empty"}
		}
		ix.applyBlock(b)
		return ix.commit(Result{StatusApplied, "genesis"})
	}

	parentIdx, parentKnown := ix.byHash[b.PrevHash]
	if !parentKnown {
		// Parent missing (out-of-order delivery or unknown fork): buffer the
		// block and keep the pipeline running; it is retried once the parent
		// gets applied.
		ix.pending[b.Hash] = b
		ix.logf("pending block height=%d hash=%s: parent %s not indexed yet", b.Height, b.Hash, b.PrevHash)
		return Result{StatusPending, "parent not indexed"}
	}

	parent := ix.chain[parentIdx]
	if b.Height != parent.Height+1 {
		return Result{StatusHeightMismatch,
			fmt.Sprintf("block height %d does not follow parent height %d", b.Height, parent.Height)}
	}

	if parentIdx == len(ix.chain)-1 {
		// Happy path: extends the tip.
		ix.applyBlock(b)
		return ix.commit(Result{StatusApplied, ""})
	}

	// Fork: the parent is canonical but not the tip. Roll back everything
	// above the parent and apply the new branch, atomically.
	depth := uint64(len(ix.chain) - 1 - parentIdx)
	if depth > ix.cfg.MaxReorgDepth {
		ix.logf("reject reorg at height=%d: depth %d exceeds max %d", b.Height, depth, ix.cfg.MaxReorgDepth)
		return Result{StatusReorgTooDeep,
			fmt.Sprintf("fork depth %d exceeds max reorg depth %d", depth, ix.cfg.MaxReorgDepth)}
	}
	rolledBack := make([]Block, 0, depth)
	for len(ix.chain)-1 > parentIdx {
		rolledBack = append(rolledBack, ix.rollbackTip())
	}
	ix.applyBlock(b)
	ix.logf("reorg: rolled back %d block(s), new tip height=%d hash=%s", len(rolledBack), b.Height, b.Hash)
	return ix.commit(Result{StatusReorgApplied,
		fmt.Sprintf("rolled back %d block(s)", len(rolledBack))})
}

// applyBlock appends b to the canonical chain and indexes its transactions.
// Callers must hold the write lock. Afterwards, pending blocks whose parent
// just became available are drained recursively.
func (ix *Indexer) applyBlock(b Block) {
	ix.byHash[b.Hash] = len(ix.chain)
	ix.chain = append(ix.chain, b)
	ix.indexTxs(b)
	ix.logf("applied block height=%d hash=%s txs=%d", b.Height, b.Hash, len(b.Txs))
	ix.drainPending()
}

// indexTxs records a block's transactions in the tx/address/balance indexes.
// A self-transfer (from == to) is indexed once for the address.
func (ix *Indexer) indexTxs(b Block) {
	for _, tx := range b.Txs {
		ix.txs[tx.Hash] = TxRef{Tx: tx, BlockHash: b.Hash, BlockHeight: b.Height}
		ix.byAddr[tx.From] = append(ix.byAddr[tx.From], tx.Hash)
		if tx.To != tx.From {
			ix.byAddr[tx.To] = append(ix.byAddr[tx.To], tx.Hash)
		}
		ix.balance[tx.From] -= tx.Amount
		ix.balance[tx.To] += tx.Amount
	}
}

// unindexTxs reverses indexTxs for a rolled-back block.
func (ix *Indexer) unindexTxs(b Block) {
	for _, tx := range b.Txs {
		delete(ix.txs, tx.Hash)
		ix.byAddr[tx.From] = removeHash(ix.byAddr[tx.From], tx.Hash)
		if tx.To != tx.From {
			ix.byAddr[tx.To] = removeHash(ix.byAddr[tx.To], tx.Hash)
		}
		ix.balance[tx.From] += tx.Amount
		ix.balance[tx.To] -= tx.Amount
	}
}

// drainPending repeatedly applies buffered blocks whose parent is now the
// canonical tip. Called with the write lock held.
func (ix *Indexer) drainPending() {
	for {
		tip := ix.chain[len(ix.chain)-1]
		child, ok := ix.findPendingChild(tip)
		if !ok {
			return
		}
		delete(ix.pending, child.Hash)
		ix.byHash[child.Hash] = len(ix.chain)
		ix.chain = append(ix.chain, child)
		ix.indexTxs(child)
		ix.logf("applied pending block height=%d hash=%s txs=%d", child.Height, child.Hash, len(child.Txs))
	}
}

func (ix *Indexer) findPendingChild(tip Block) (Block, bool) {
	for _, b := range ix.pending {
		if b.PrevHash == tip.Hash && b.Height == tip.Height+1 {
			return b, true
		}
	}
	return Block{}, false
}

// rollbackTip removes the canonical tip and reverses its transactions.
func (ix *Indexer) rollbackTip() Block {
	tip := ix.chain[len(ix.chain)-1]
	ix.chain = ix.chain[:len(ix.chain)-1]
	delete(ix.byHash, tip.Hash)
	ix.unindexTxs(tip)
	ix.logf("rolled back block height=%d hash=%s txs=%d", tip.Height, tip.Hash, len(tip.Txs))
	return tip
}

func removeHash(hashes []string, h string) []string {
	for i, x := range hashes {
		if x == h {
			return append(hashes[:i], hashes[i+1:]...)
		}
	}
	return hashes
}

// commit persists the index after a successful mutation.
func (ix *Indexer) commit(r Result) Result {
	if ix.cfg.DataDir == "" {
		return r
	}
	s := &snapshot{
		Chain:   ix.chain,
		Txs:     ix.txs,
		ByAddr:  ix.byAddr,
		Balance: ix.balance,
	}
	if err := saveSnapshot(ix.cfg.DataDir, s); err != nil {
		ix.logf("persist snapshot failed: %v", err)
		return Result{Status: r.Status, Detail: r.Detail + "; persist error: " + err.Error()}
	}
	return r
}
