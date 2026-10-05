package indexer

import (
	"fmt"

	"github.com/lechandonga/blockchain-reorg-indexer-go/internal/model"
	"github.com/lechandonga/blockchain-reorg-indexer-go/internal/storage"
)

// IngestStatus is the distinguishable outcome of delivering one block.
type IngestStatus string

const (
	// StatusApplied: the block extended or reorganized the canonical chain.
	StatusApplied IngestStatus = "APPLIED"
	// StatusDuplicate: identical block already anchored at the same height.
	StatusDuplicate IngestStatus = "DUPLICATE"
	// StatusPendingGap: stored and parked; an ancestor is missing or a fork
	// branch is incomplete. It will be retried after dependencies arrive.
	StatusPendingGap IngestStatus = "PENDING_MISSING_PARENT"
	// StatusPendingShorterFork: competing fork is not strictly longer yet.
	StatusPendingShorterFork IngestStatus = "PENDING_SHORTER_FORK"
	// StatusRejectedCorrupt: hash mismatch or otherwise invalid content;
	// it is neither stored nor applied, and the index is untouched.
	StatusRejectedCorrupt IngestStatus = "REJECTED_CORRUPT"
	// StatusRejectedDeepReorg: reorg depth exceeds the configured limit;
	// the existing index stays fully in place and queryable.
	StatusRejectedDeepReorg IngestStatus = "REJECTED_DEEP_REORG"
)

// IngestResult describes what happened to one delivered block.
type IngestResult struct {
	Number uint64
	Hash   string
	Status IngestStatus
	Reason string
}

// Config holds pipeline parameters.
type Config struct {
	// MaxReorgDepth limits how many confirmed blocks a reorg may replace.
	MaxReorgDepth uint64
}

// DefaultConfig returns MaxReorgDepth=16.
func DefaultConfig() Config { return Config{MaxReorgDepth: 16} }

// Indexer serializes ingestion on top of a durable Store.
type Indexer struct {
	store *storage.Store
	cfg   Config
	logf  func(string, ...any)

	ingestMu mutex
}

// New builds an indexer and resumes from whatever the store already contains:
// persisted blocks that were parked before a restart get another anchoring pass.
func New(store *storage.Store, cfg Config, logf func(string, ...any)) (*Indexer, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if cfg.MaxReorgDepth == 0 {
		cfg = DefaultConfig()
	}
	ix := &Indexer{store: store, cfg: cfg, logf: logf}
	if err := ix.resume(); err != nil {
		return nil, err
	}
	return ix, nil
}

// Store exposes the underlying store for queries.
func (ix *Indexer) Store() *storage.Store { return ix.store }

// Ingest validates, persists and (when possible) anchors one block.
// It never blocks the pipeline on a bad or premature block: such inputs get a
// distinct status and processing of later blocks continues.
func (ix *Indexer) Ingest(b *model.Block) IngestResult {
	ix.ingestMu.Lock()
	defer ix.ingestMu.Unlock()

	res := ix.ingestOne(b)
	if res.Status == StatusApplied {
		ix.pumpPending()
	}
	return res
}

func (ix *Indexer) resume() error {
	ix.ingestMu.Lock()
	defer ix.ingestMu.Unlock()
	ix.logf("resume: canonical tip=%d, replaying persisted blocks", ix.store.Tip())
	ix.pumpPending()
	return nil
}

func (ix *Indexer) ingestOne(b *model.Block) IngestResult {
	if b == nil {
		return IngestResult{Status: StatusRejectedCorrupt, Reason: "nil block"}
	}
	if !b.ValidHash() {
		ix.logf("reject corrupt block height=%d declared=%s", b.Number, b.Hash)
		return IngestResult{Number: b.Number, Hash: b.Hash,
			Status: StatusRejectedCorrupt, Reason: "content hash mismatch"}
	}
	if b.Number == 0 && b.ParentHash != model.GenesisHash {
		return IngestResult{Number: b.Number, Hash: b.Hash,
			Status: StatusRejectedCorrupt, Reason: "block 0 must anchor at genesis"}
	}

	saved, err := ix.store.SaveBlock(b)
	if err != nil {
		return IngestResult{Number: b.Number, Hash: b.Hash,
			Status: StatusRejectedCorrupt, Reason: err.Error()}
	}
	if !saved {
		ix.logf("dedupe block height=%d hash=%s (already stored)", b.Number, b.Hash)
	}

	st := ix.store.Snapshot()
	return ix.anchor(b, &st)
}

// anchor decides whether b can be connected to the canonical chain now.
func (ix *Indexer) anchor(b *model.Block, st *storage.State) IngestResult {
	tip := ix.store.Tip()

	if len(st.Chain) == 0 {
		if b.Number != 0 {
			return pending(b, "waiting for block 0 / earlier ancestors")
		}
		return ix.extendFrom(b, st)
	}

	switch {
	case b.Number > tip+1:
		return pending(b, fmt.Sprintf("gap: tip=%d delivered=%d", tip, b.Number))
	case b.Number == tip+1:
		if b.ParentHash == st.Chain[tip].Hash {
			return ix.extendFrom(b, st)
		}
		return pending(b, "parent at tip height is on a different branch")
	default:
		if st.Chain[b.Number].Hash == b.Hash {
			return IngestResult{Number: b.Number, Hash: b.Hash, Status: StatusDuplicate,
				Reason: "already canonical"}
		}
		return ix.reorg(b, st)
	}
}

func pending(b *model.Block, reason string) IngestResult {
	return IngestResult{Number: b.Number, Hash: b.Hash,
		Status: StatusPendingGap, Reason: reason}
}

// extendFrom appends b and any already-stored descendants to the chain.
func (ix *Indexer) extendFrom(b *model.Block, st *storage.State) IngestResult {
	added, err := ix.collectForward(b, st)
	if err != nil {
		return IngestResult{Number: b.Number, Hash: b.Hash,
			Status: StatusRejectedCorrupt, Reason: err.Error()}
	}
	if err := ix.store.ReplaceState(buildState(st, added)); err != nil {
		return IngestResult{Number: b.Number, Hash: b.Hash,
			Status: StatusRejectedCorrupt, Reason: err.Error()}
	}
	ix.logf("applied block height=%d hash=%s (new tip=%d)", b.Number, b.Hash, ix.store.Tip())
	return IngestResult{Number: b.Number, Hash: b.Hash, Status: StatusApplied}
}

// reorg plans and applies a competing branch atomically or refuses cleanly.
func (ix *Indexer) reorg(b *model.Block, st *storage.State) IngestResult {
	forkPoint, missing := ix.findForkPoint(b, st)
	if missing != "" {
		return pending(b, "fork branch incomplete: "+missing)
	}

	// b walks backward to the fork point; now walk forward through stored
	// descendants to build the longest deterministic competing branch.
	branch, err := ix.collectForward(b, truncateCopy(st, forkPoint))
	if err != nil {
		return IngestResult{Number: b.Number, Hash: b.Hash,
			Status: StatusRejectedCorrupt, Reason: err.Error()}
	}
	newTip := forkPoint
	if n := len(branch); n > 0 {
		newTip = branch[n-1].Number
	}

	if newTip <= st.Tip {
		return IngestResult{Number: b.Number, Hash: b.Hash,
			Status: StatusPendingShorterFork,
			Reason: fmt.Sprintf("fork tip %d not beyond canonical tip %d", newTip, st.Tip)}
	}

	depth := b.Number - forkPoint - 1
	if depth > ix.cfg.MaxReorgDepth {
		ix.logf("REJECT deep reorg height=%d fork=%d depth=%d limit=%d",
			b.Number, forkPoint, depth, ix.cfg.MaxReorgDepth)
		return IngestResult{Number: b.Number, Hash: b.Hash,
			Status: StatusRejectedDeepReorg,
			Reason: fmt.Sprintf("reorg depth %d > limit %d", depth, ix.cfg.MaxReorgDepth)}
	}

	if err := ix.store.ReplaceState(buildState(truncateCopy(st, forkPoint), branch)); err != nil {
		return IngestResult{Number: b.Number, Hash: b.Hash,
			Status: StatusRejectedCorrupt, Reason: err.Error()}
	}
	ix.logf("REORG height=%d fork=%d depth=%d: rolled back %d blocks, applied %d, new tip=%d",
		b.Number, forkPoint, depth, len(branch), ix.store.Tip())
	return IngestResult{Number: b.Number, Hash: b.Hash, Status: StatusApplied,
		Reason: fmt.Sprintf("reorg depth %d", depth)}
}

// findForkPoint walks parents of b until it meets the canonical chain.
func (ix *Indexer) findForkPoint(b *model.Block, st *storage.State) (uint64, string) {
	cur := b
	for {
		h := cur.Number
		if h == 0 {
			if cur.ParentHash == model.GenesisHash {
				return 0, ""
			}
			return 0, "block 0 not anchored at genesis"
		}
		if h <= st.Tip && st.Chain[h].Hash == cur.Hash {
			return h, ""
		}
		parent, ok := ix.store.Block(cur.ParentHash)
		if !ok {
			return 0, fmt.Sprintf("missing parent %s at height %d", cur.ParentHash, h-1)
		}
		if parent.Number != h-1 {
			return 0, fmt.Sprintf("parent %s has height %d, expected %d",
				parent.Hash, parent.Number, h-1)
		}
		cur = parent
	}
}

// collectForward follows child pointers through the block library starting at
// start (which must fit onto the given state) until the branch ends.
func (ix *Indexer) collectForward(start *model.Block, st *storage.State) ([]*model.Block, error) {
	var branch []*model.Block
	cur := start
	for {
		expected := uint64(0)
		if len(st.Chain) > 0 {
			expected = st.Tip + 1
		}
		if len(branch) > 0 {
			expected = cur.Number + 1
		}
		if len(branch) == 0 {
			if cur.Number != expected {
				return nil, fmt.Errorf("block %d does not connect at height %d", cur.Number, expected)
			}
		}
		branch = append(branch, cur)

		next := ix.pickChild(cur.Hash, cur.Number+1)
		if next == nil {
			return branch, nil
		}
		cur = next
	}
}

// pickChild deterministically chooses a stored child block: correct height and
// parent hash, then lexicographically greatest hash on a tie (heaviest wins,
// tie broken deterministically).
func (ix *Indexer) pickChild(parentHash string, height uint64) *model.Block {
	var best *model.Block
	for _, blk := range ix.store.AllBlocks() {
		if blk.Number == height && blk.ParentHash == parentHash {
			if best == nil || blk.Hash > best.Hash {
				best = blk
			}
		}
	}
	return best
}

// pumpPending repeatedly retries stored, non-canonical blocks; one pass often
// anchors a whole queued segment, repeated passes resolve cascading gaps.
func (ix *Indexer) pumpPending() {
	for {
		progress := false
		for _, blk := range ix.store.AllBlocks() {
			st := ix.store.Snapshot()
			if blk.Number <= st.Tip && st.Chain[blk.Number].Hash == blk.Hash {
				continue
			}
			res := ix.anchor(blk, &st)
			switch res.Status {
			case StatusApplied:
				progress = true
				ix.logf("pump: anchored parked block height=%d hash=%s", blk.Number, blk.Hash)
			case StatusRejectedCorrupt, StatusRejectedDeepReorg:
				ix.logf("pump: %s height=%d: %s", res.Status, blk.Number, res.Reason)
			}
		}
		if !progress {
			return
		}
	}
}

func truncateCopy(st *storage.State, keepThrough uint64) *storage.State {
	out := storage.State{}
	if len(st.Chain) > 0 {
		out.Chain = append([]storage.Link(nil), st.Chain[:keepThrough+1]...)
	}
	out.Tip = keepThrough
	for _, r := range st.Records {
		if r.BlockNumber <= keepThrough {
			out.Records = append(out.Records, r)
		}
	}
	return &out
}

// buildState assails records for added blocks onto a truncated base state.
func buildState(base *storage.State, added []*model.Block) *storage.State {
	out := &stateBuilder
	_ = out
	next := storage.State{
		Chain:   append([]storage.Link(nil), base.Chain...),
		Records: append([]storage.TxRecord(nil), base.Records...),
	}
	for _, blk := range added {
		next.Chain = append(next.Chain, storage.Link{
			Number:     blk.Number,
			Hash:       blk.Hash,
			ParentHash: blk.ParentHash,
		})
		for _, tx := range blk.Transactions {
			next.Records = append(next.Records, storage.TxRecord{
				TxHash:      tx.Hash,
				From:        tx.From,
				To:          tx.To,
				Value:       tx.Value,
				BlockNumber: blk.Number,
				BlockHash:   blk.Hash,
			})
		}
	}
	if len(next.Chain) > 0 {
		next.Tip = next.Chain[len(next.Chain)-1].Number
	}
	return &next
}
