package indexer

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func tx(hash, from, to string, amount int64) Tx {
	return Tx{Hash: hash, From: from, To: to, Amount: amount}
}

// buildChain returns a valid chain of n blocks starting at genesis.
func buildChain(n int, tag string) []Block {
	blocks := make([]Block, 0, n)
	prev := GenesisParentHash
	for h := 0; h < n; h++ {
		txs := []Tx{tx(fmt.Sprintf("%s-tx-%d", tag, h), "alice", "bob", int64(10+h))}
		b := MakeBlock(uint64(h), prev, int64(1000+h), txs)
		blocks = append(blocks, b)
		prev = b.Hash
	}
	return blocks
}

func mustStatus(t *testing.T, got Result, want Status) {
	t.Helper()
	if got.Status != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSequentialIngestAndQuery(t *testing.T) {
	ix, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	chain := buildChain(5, "main")
	for _, b := range chain {
		mustStatus(t, ix.Ingest(b), StatusApplied)
	}
	if ix.Height() != 4 {
		t.Fatalf("height=%d, want 4", ix.Height())
	}
	ref, ok := ix.GetTx("main-tx-2")
	if !ok {
		t.Fatal("tx main-tx-2 not indexed")
	}
	if ref.BlockHeight != 2 || ref.BlockHash != chain[2].Hash {
		t.Fatalf("bad tx ref: %+v", ref)
	}
	if got := ix.TxsByAddress("alice"); len(got) != 5 {
		t.Fatalf("alice txs=%v, want 5 entries", got)
	}
	// alice sent 10+11+12+13+14=60, bob received the same.
	if got := ix.Balance("alice"); got != -60 {
		t.Fatalf("alice balance=%d, want -60", got)
	}
	if got := ix.Balance("bob"); got != 60 {
		t.Fatalf("bob balance=%d, want 60", got)
	}
}

func TestDuplicateDeliveryNotReapplied(t *testing.T) {
	ix, _ := New(Config{})
	chain := buildChain(3, "dup")
	for _, b := range chain {
		ix.Ingest(b)
	}
	for _, b := range chain {
		mustStatus(t, ix.Ingest(b), StatusDuplicate)
	}
	if ix.Height() != 2 {
		t.Fatalf("height=%d, want 2", ix.Height())
	}
	if got := ix.Balance("bob"); got != 33 {
		t.Fatalf("bob balance=%d, want 33 (no double apply)", got)
	}
	if got := ix.TxsByAddress("bob"); len(got) != 3 {
		t.Fatalf("bob txs=%v, want exactly 3", got)
	}
}

func TestOutOfOrderDelivery(t *testing.T) {
	ix, _ := New(Config{})
	chain := buildChain(5, "ooo")
	// Deliver in scrambled order: 0, 2, 4, 1, 3.
	order := []int{0, 2, 4, 1, 3}
	statuses := map[int]Status{}
	for _, i := range order {
		statuses[i] = ix.Ingest(chain[i]).Status
	}
	mustStatus(t, Result{Status: statuses[2]}, StatusPending)
	mustStatus(t, Result{Status: statuses[4]}, StatusPending)
	mustStatus(t, Result{Status: statuses[1]}, StatusApplied)
	mustStatus(t, Result{Status: statuses[3]}, StatusApplied)
	if ix.Height() != 4 {
		t.Fatalf("height=%d, want 4 (pending blocks must be drained)", ix.Height())
	}
	if ix.PendingCount() != 0 {
		t.Fatalf("pending=%d, want 0", ix.PendingCount())
	}
	if got := ix.Balance("bob"); got != 60 {
		t.Fatalf("bob balance=%d, want 60", got)
	}
}

func TestCorruptedBlockRejected(t *testing.T) {
	ix, _ := New(Config{})
	chain := buildChain(3, "corrupt")
	ix.Ingest(chain[0])
	ix.Ingest(chain[1])

	bad := chain[2]
	bad.Txs = append(bad.Txs, tx("evil", "mallory", "mallory", 9999)) // hash no longer matches
	mustStatus(t, ix.Ingest(bad), StatusCorrupted)

	badHash := chain[2]
	badHash.Hash = "deadbeef"
	mustStatus(t, ix.Ingest(badHash), StatusCorrupted)

	badTx := MakeBlock(2, chain[1].Hash, 1002, []Tx{tx("neg", "a", "b", -5)})
	badTx.Hash = ComputeHash(2, chain[1].Hash, 1002, badTx.Txs)
	mustStatus(t, ix.Ingest(badTx), StatusCorrupted)

	// Index untouched; the good block still applies afterwards.
	if ix.Height() != 1 {
		t.Fatalf("height=%d, want 1", ix.Height())
	}
	mustStatus(t, ix.Ingest(chain[2]), StatusApplied)
	if got := ix.Balance("bob"); got != 33 {
		t.Fatalf("bob balance=%d, want 33", got)
	}
}

func TestReorgRollsBackAndAppliesNewBranch(t *testing.T) {
	ix, _ := New(Config{})
	old := buildChain(5, "old")
	for _, b := range old {
		ix.Ingest(b)
	}
	// New branch forks at height 2 (shares blocks 0..1).
	fork := buildChain(6, "new")
	fork[0], fork[1] = old[0], old[1]
	prev := old[1].Hash
	for h := 2; h < 6; h++ {
		fork[h] = MakeBlock(uint64(h), prev, int64(2000+h), fork[h].Txs)
		prev = fork[h].Hash
	}
	mustStatus(t, ix.Ingest(fork[2]), StatusReorgApplied)
	for h := 3; h < 6; h++ {
		mustStatus(t, ix.Ingest(fork[h]), StatusApplied)
	}
	if ix.Height() != 5 {
		t.Fatalf("height=%d, want 5", ix.Height())
	}
	// Old-branch txs above the fork must be gone, new-branch txs present.
	for h := 2; h < 5; h++ {
		if _, ok := ix.GetTx(fmt.Sprintf("old-tx-%d", h)); ok {
			t.Fatalf("stale tx old-tx-%d still indexed after reorg", h)
		}
	}
	for h := 0; h < 2; h++ {
		if _, ok := ix.GetTx(fmt.Sprintf("old-tx-%d", h)); !ok {
			t.Fatalf("shared tx old-tx-%d missing after reorg", h)
		}
	}
	for h := 2; h < 6; h++ {
		ref, ok := ix.GetTx(fmt.Sprintf("new-tx-%d", h))
		if !ok {
			t.Fatalf("new-tx-%d missing after reorg", h)
		}
		if ref.BlockHash != fork[h].Hash {
			t.Fatalf("new-tx-%d points at old block %s", h, ref.BlockHash)
		}
	}
	// Balances reflect only the canonical chain: shared blocks 0,1 (10+11)
	// plus new branch blocks 2..5 (12+13+14+15).
	if got := ix.Balance("bob"); got != 75 {
		t.Fatalf("bob balance=%d, want 75", got)
	}
	if got := ix.TxsByAddress("bob"); len(got) != 6 {
		t.Fatalf("bob txs=%v, want 6 entries, no duplicates", got)
	}
	// Canonical blocks at replaced heights are the new branch's.
	for h := 2; h < 6; h++ {
		b, _ := ix.CanonicalBlock(uint64(h))
		if b.Hash != fork[h].Hash {
			t.Fatalf("canonical block at %d is %s, want new branch %s", h, b.Hash, fork[h].Hash)
		}
	}
}

func TestReorgTooDeepRejected(t *testing.T) {
	ix, _ := New(Config{MaxReorgDepth: 2})
	chain := buildChain(6, "deep")
	for _, b := range chain {
		ix.Ingest(b)
	}
	tipBefore := ix.TipHash()
	bobBefore := ix.Balance("bob")
	// Fork from height 1: rollback depth would be 4 > 2.
	rival := MakeBlock(2, chain[1].Hash, 3002, []Tx{tx("rival-tx", "alice", "carol", 7)})
	mustStatus(t, ix.Ingest(rival), StatusReorgTooDeep)
	// Index must be fully intact and queryable.
	if ix.TipHash() != tipBefore {
		t.Fatal("tip changed after rejected reorg")
	}
	if ix.Balance("bob") != bobBefore {
		t.Fatal("balance changed after rejected reorg")
	}
	if _, ok := ix.GetTx("rival-tx"); ok {
		t.Fatal("rejected reorg tx leaked into index")
	}
	for h := 0; h < 6; h++ {
		if _, ok := ix.GetTx(fmt.Sprintf("deep-tx-%d", h)); !ok {
			t.Fatalf("deep-tx-%d lost after rejected reorg", h)
		}
	}
}

func TestRestartResumesWithoutReapply(t *testing.T) {
	dir := t.TempDir()
	chain := buildChain(4, "persist")

	ix1, _ := New(Config{DataDir: dir})
	ix1.Ingest(chain[0])
	ix1.Ingest(chain[1])

	// Restart: resume from persisted progress.
	ix2, err := New(Config{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if ix2.Height() != 1 {
		t.Fatalf("resumed height=%d, want 1", ix2.Height())
	}
	// Re-delivering processed blocks is a no-op.
	mustStatus(t, ix2.Ingest(chain[0]), StatusDuplicate)
	mustStatus(t, ix2.Ingest(chain[1]), StatusDuplicate)
	// New blocks continue from the persisted tip.
	mustStatus(t, ix2.Ingest(chain[2]), StatusApplied)
	mustStatus(t, ix2.Ingest(chain[3]), StatusApplied)
	if got := ix2.Balance("bob"); got != 46 {
		t.Fatalf("bob balance=%d, want 46", got)
	}

	// Second restart keeps confirmed data.
	ix3, _ := New(Config{DataDir: dir})
	if ix3.Height() != 3 || ix3.Balance("bob") != 46 {
		t.Fatalf("after second restart: height=%d balance=%d", ix3.Height(), ix3.Balance("bob"))
	}
	if got := ix3.TxsByAddress("alice"); len(got) != 4 {
		t.Fatalf("alice txs after restart=%v, want 4", got)
	}
}

func TestConcurrentQueryDuringReorg(t *testing.T) {
	ix, _ := New(Config{})
	old := buildChain(8, "c-old")
	for _, b := range old {
		ix.Ingest(b)
	}
	fork := buildChain(8, "c-new")
	fork[0], fork[1] = old[0], old[1]
	prev := old[1].Hash
	for h := 2; h < 8; h++ {
		fork[h] = MakeBlock(uint64(h), prev, int64(4000+h), fork[h].Txs)
		prev = fork[h].Hash
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// A reader must never observe a partially rolled-back chain:
				// every tx it sees must point at a canonical block.
				for h := 0; h < 8; h++ {
					for _, tag := range []string{"c-old", "c-new"} {
						if ref, ok := ix.GetTx(fmt.Sprintf("%s-tx-%d", tag, h)); ok {
							b, canon := ix.CanonicalBlock(ref.BlockHeight)
							if !canon || b.Hash != ref.BlockHash {
								t.Errorf("tx %s-tx-%d points at non-canonical block", tag, h)
								return
							}
						}
					}
				}
			}
		}()
	}
	for h := 2; h < 8; h++ {
		ix.Ingest(fork[h])
	}
	close(stop)
	wg.Wait()
}

// TestIdempotentRerun feeds the same shuffled input twice (fresh indexer each
// time, plus a rerun over the same data dir) and requires identical final
// index state.
func TestIdempotentRerun(t *testing.T) {
	old := buildChain(6, "r-old")
	fork := buildChain(7, "r-new")
	fork[0], fork[1], fork[2] = old[0], old[1], old[2]
	prev := old[2].Hash
	for h := 3; h < 7; h++ {
		fork[h] = MakeBlock(uint64(h), prev, int64(5000+h), fork[h].Txs)
		prev = fork[h].Hash
	}
	// One input program: in-order prefix, out-of-order tail, duplicates, a
	// corrupted block, then a reorg branch.
	corrupt := old[4]
	corrupt.Hash = "broken"
	program := []Block{
		old[0], old[2], old[1], // out of order
		old[3], old[3], // duplicate
		corrupt, // corrupted
		old[4], old[5],
		fork[3], fork[5], fork[4], fork[6], // reorg branch, out of order
		old[4], // duplicate of rolled-back block -> pending (parent gone)
	}

	run := func(dir string) *Indexer {
		ix, err := New(Config{DataDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range program {
			ix.Ingest(b)
		}
		return ix
	}

	type indexState struct {
		height   uint64
		tip      string
		bobBal   int64
		aliceTxs []string
	}
	stateOf := func(ix *Indexer) indexState {
		return indexState{ix.Height(), ix.TipHash(), ix.Balance("bob"), ix.TxsByAddress("alice")}
	}

	a := stateOf(run(t.TempDir()))
	b := stateOf(run(t.TempDir()))
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same input, different final state:\n%v\n%v", a, b)
	}

	// Rerunning the whole program over an already-populated data dir must
	// also converge to the same state.
	dir := t.TempDir()
	run(dir)
	c := stateOf(run(dir))
	if !reflect.DeepEqual(a, c) {
		t.Fatalf("rerun over persisted state diverged:\n%v\n%v", a, c)
	}
}
