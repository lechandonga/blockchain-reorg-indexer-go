// Command indexer runs the local indexing pipeline end to end against a
// simulated, fully local block source: sequential ingest, out-of-order and
// duplicate deliveries, a corrupted block, a chain reorg and a restart.
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"

	"github.com/lechandonga/blockchain-reorg-indexer-go/internal/indexer"
)

func main() {
	dataDir := flag.String("data-dir", "./index-data", "directory for the persisted index snapshot")
	maxReorg := flag.Uint64("max-reorg-depth", 64, "maximum reorg depth accepted before rejection")
	seed := flag.Int64("seed", 42, "random seed for the simulated source")
	flag.Parse()

	logger := log.New(os.Stdout, "[indexer] ", log.LstdFlags|log.Lmsgprefix)
	rng := rand.New(rand.NewSource(*seed))

	addrs := []string{"alice", "bob", "carol", "dave"}
	txID := 0
	nextTxs := func() []indexer.Tx {
		n := 1 + rng.Intn(3)
		txs := make([]indexer.Tx, 0, n)
		for i := 0; i < n; i++ {
			txID++
			txs = append(txs, indexer.Tx{
				Hash:   fmt.Sprintf("tx-%04d", txID),
				From:   addrs[rng.Intn(len(addrs))],
				To:     addrs[rng.Intn(len(addrs))],
				Amount: int64(1 + rng.Intn(100)),
			})
		}
		return txs
	}

	// Simulated canonical chain of 12 blocks.
	main := make([]indexer.Block, 0, 12)
	prev := indexer.GenesisParentHash
	for h := 0; h < 12; h++ {
		b := indexer.MakeBlock(uint64(h), prev, int64(1700000000+h), nextTxs())
		main = append(main, b)
		prev = b.Hash
	}
	// Rival branch forking at height 7 (replaces heights 8..11).
	fork := make([]indexer.Block, 0, 5)
	prev = main[7].Hash
	for h := 8; h < 13; h++ {
		b := indexer.MakeBlock(uint64(h), prev, int64(1800000000+h), nextTxs())
		fork = append(fork, b)
		prev = b.Hash
	}

	open := func() *indexer.Indexer {
		ix, err := indexer.New(indexer.Config{DataDir: *dataDir, MaxReorgDepth: *maxReorg, Logger: logger})
		if err != nil {
			logger.Fatalf("open indexer: %v", err)
		}
		return ix
	}
	report := func(tag string, r indexer.Result) {
		logger.Printf("ingest %-22s -> %s", tag, r)
	}

	ix := open()

	logger.Printf("=== phase 1: sequential ingest of blocks 0..5 ===")
	for h := 0; h <= 5; h++ {
		report(fmt.Sprintf("main[%d]", h), ix.Ingest(main[h]))
	}

	logger.Printf("=== phase 2: out-of-order + duplicate delivery ===")
	report("main[7] (early)", ix.Ingest(main[7]))
	report("main[6]", ix.Ingest(main[6]))
	report("main[7] (retry)", ix.Ingest(main[7]))
	report("main[6] (dup)", ix.Ingest(main[6]))
	report("main[8]", ix.Ingest(main[8]))

	logger.Printf("=== phase 3: corrupted input is rejected, pipeline continues ===")
	corrupt := main[9]
	corrupt.Txs = append(corrupt.Txs, indexer.Tx{Hash: "tx-evil", From: "mallory", To: "mallory", Amount: 1000000})
	report("main[9] (corrupt)", ix.Ingest(corrupt))
	report("main[9]", ix.Ingest(main[9]))
	report("main[10]", ix.Ingest(main[10]))
	report("main[11]", ix.Ingest(main[11]))

	logger.Printf("=== phase 4: reorg - rival branch replaces heights 8..12 ===")
	for i, b := range fork {
		report(fmt.Sprintf("fork[%d]", 8+i), ix.Ingest(b))
	}

	logger.Printf("=== phase 5: restart and resume from persisted progress ===")
	ix = open()
	report("main[11] (dup after restart)", ix.Ingest(main[11]))

	logger.Printf("=== final state ===")
	logger.Printf("canonical height=%d tip=%s", ix.Height(), ix.TipHash())
	for _, a := range addrs {
		logger.Printf("addr=%s balance=%d txs=%v", a, ix.Balance(a), ix.TxsByAddress(a))
	}
}
