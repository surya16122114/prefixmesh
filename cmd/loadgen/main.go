// Loadgen v0: reproducible benchmark client (docs/BENCHMARKS.md §2).
//
// Workload shape per request: tenant system prompt (long shared prefix) +
// Zipfian document (mid-length shared prefix) + unique user suffix (always a
// miss by design). The client does what a real inference sidecar would:
// Match, treat everything past matched_depth as prefill work, write the
// computed blocks back.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/chain"
)

const costPerTokenUS = 50 // simulated prefill cost; ~real order for a 7B model

type config struct {
	gateway     string
	requests    int
	concurrency int
	tenants     int
	docs        int
	zipfS       float64
	blockSize   int
	sysBlocks   int
	docBlocks   int
	payload     int
	seed        int64
	modelID     string
	// Every expensiveEvery-th doc costs expensiveMult× to prefill (stand-in
	// for long-context / multimodal-heavy documents) — the heterogeneity
	// that cost-aware eviction exploits (docs/BENCHMARKS.md §3).
	expensiveEvery int
	expensiveMult  uint64
}

// docCostMult is the per-doc prefill cost multiplier.
func (c config) docCostMult(doc int) uint64 {
	if c.expensiveEvery > 0 && doc%c.expensiveEvery == 0 {
		return c.expensiveMult
	}
	return 1
}

type metrics struct {
	blocksRequested atomic.Uint64
	blocksHit       atomic.Uint64
	costRequestedUS atomic.Uint64
	costSavedUS     atomic.Uint64
	fullHits        atomic.Uint64

	mu        sync.Mutex
	latencies []time.Duration // Match RPC latency
}

// tenantTokens returns the deterministic system-prompt tokens for a tenant.
func tenantTokens(cfg config, tenant int) []uint32 {
	r := rand.New(rand.NewSource(cfg.seed ^ int64(tenant)<<32))
	return randTokens(r, cfg.sysBlocks*cfg.blockSize)
}

// docTokens returns the deterministic tokens for one RAG document.
func docTokens(cfg config, doc int) []uint32 {
	r := rand.New(rand.NewSource(cfg.seed ^ 0x5eed ^ int64(doc)<<16))
	return randTokens(r, cfg.docBlocks*cfg.blockSize)
}

func randTokens(r *rand.Rand, n int) []uint32 {
	ts := make([]uint32, n)
	for i := range ts {
		ts[i] = uint32(r.Intn(50_000)) // vocab-sized token ids
	}
	return ts
}

func worker(ctx context.Context, cfg config, client pmv1.GatewayServiceClient,
	m *metrics, workerID, requests int) error {

	r := rand.New(rand.NewSource(cfg.seed + int64(workerID)*1_000_003))
	// zipf-s <= 1 means uniform popularity (Go's Zipf needs s > 1, and even
	// s=1.01 is still ~200x skewed across 200 docs — not "nearly flat").
	var zipf *rand.Zipf
	if cfg.zipfS > 1 {
		zipf = rand.NewZipf(r, cfg.zipfS, 1, uint64(cfg.docs-1))
	}
	pickDoc := func() int {
		if zipf != nil {
			return int(zipf.Uint64())
		}
		return r.Intn(cfg.docs)
	}

	for i := 0; i < requests; i++ {
		tenant := r.Intn(cfg.tenants)
		doc := pickDoc()
		docMult := cfg.docCostMult(doc)
		// System-prompt and suffix blocks cost 1×; the doc's blocks carry its
		// multiplier. Segment boundaries align with block boundaries because
		// sys/doc lengths are whole blocks.
		blockMult := func(j int) uint64 {
			if j >= cfg.sysBlocks && j < cfg.sysBlocks+cfg.docBlocks {
				return docMult
			}
			return 1
		}

		tokens := append([]uint32{}, tenantTokens(cfg, tenant)...)
		tokens = append(tokens, docTokens(cfg, doc)...)
		tokens = append(tokens, randTokens(r, cfg.blockSize)...) // unique suffix

		ids := chain.Build(cfg.modelID, cfg.blockSize, tokens)
		raw := make([][]byte, len(ids))
		for j := range ids {
			raw[j] = ids[j][:]
		}

		start := time.Now()
		resp, err := client.Match(ctx, &pmv1.MatchRequest{
			ModelId: cfg.modelID,
			Chain:   raw,
		})
		if err != nil {
			return fmt.Errorf("Match: %w", err)
		}
		m.mu.Lock()
		m.latencies = append(m.latencies, time.Since(start))
		m.mu.Unlock()

		depth := int(resp.MatchedDepth)
		blocks := chain.Chunk(tokens, cfg.blockSize)
		m.blocksRequested.Add(uint64(len(ids)))
		m.blocksHit.Add(uint64(depth))
		if depth == len(ids) {
			m.fullHits.Add(1)
		}
		for j, blk := range blocks {
			cost := uint64(len(blk)) * costPerTokenUS * blockMult(j)
			m.costRequestedUS.Add(cost)
			if j < depth {
				m.costSavedUS.Add(cost)
			}
		}

		// "Prefill" the misses and write them back.
		if depth < len(ids) {
			up, err := client.PutBlocks(ctx)
			if err != nil {
				return fmt.Errorf("PutBlocks open: %w", err)
			}
			parent := chain.Root(cfg.modelID, cfg.blockSize)
			for j, blk := range blocks {
				if j >= depth {
					payload := make([]byte, cfg.payload)
					if err := up.Send(&pmv1.PutBlocksRequest{Block: &pmv1.Block{
						BlockId:    ids[j][:],
						ParentId:   parent[:],
						ModelId:    cfg.modelID,
						Payload:    payload,
						TokenCount: uint32(len(blk)),
						CostUs:     uint64(len(blk)) * costPerTokenUS * blockMult(j),
					}}); err != nil {
						return fmt.Errorf("PutBlocks send: %w", err)
					}
				}
				parent = ids[j]
			}
			if _, err := up.CloseAndRecv(); err != nil {
				return fmt.Errorf("PutBlocks close: %w", err)
			}
		}
	}
	return nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)-1))
	return sorted[i]
}

func main() {
	var cfg config
	flag.StringVar(&cfg.gateway, "gateway", "localhost:7000", "gateway address")
	flag.IntVar(&cfg.requests, "requests", 2000, "total requests")
	flag.IntVar(&cfg.concurrency, "concurrency", 8, "concurrent workers")
	flag.IntVar(&cfg.tenants, "tenants", 4, "tenants (one system prompt each)")
	flag.IntVar(&cfg.docs, "docs", 200, "RAG document pool size")
	flag.Float64Var(&cfg.zipfS, "zipf-s", 1.2, "Zipf skew for doc popularity")
	flag.IntVar(&cfg.blockSize, "block-size", 128, "tokens per block")
	flag.IntVar(&cfg.sysBlocks, "sys-blocks", 8, "system prompt length, blocks")
	flag.IntVar(&cfg.docBlocks, "doc-blocks", 4, "document length, blocks")
	flag.IntVar(&cfg.payload, "payload-bytes", 64<<10, "simulated KV bytes per block")
	flag.Int64Var(&cfg.seed, "seed", 42, "workload PRNG seed")
	flag.StringVar(&cfg.modelID, "model", "sim-7b", "model id (cache namespace)")
	flag.IntVar(&cfg.expensiveEvery, "expensive-every", 5, "every Nth doc is expensive (0 = uniform costs)")
	flag.Uint64Var(&cfg.expensiveMult, "expensive-mult", 10, "prefill cost multiplier for expensive docs")
	flag.Parse()

	conn, err := grpc.NewClient(cfg.gateway,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("dial failed", "err", err)
		os.Exit(1)
	}
	client := pmv1.NewGatewayServiceClient(conn)

	m := &metrics{}
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, cfg.concurrency)
	per := cfg.requests / cfg.concurrency
	for w := 0; w < cfg.concurrency; w++ {
		n := per
		if w == cfg.concurrency-1 {
			n = cfg.requests - per*(cfg.concurrency-1)
		}
		wg.Add(1)
		go func(w, n int) {
			defer wg.Done()
			if err := worker(context.Background(), cfg, client, m, w, n); err != nil {
				errs <- err
			}
		}(w, n)
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		slog.Error("worker failed", "err", err)
		os.Exit(1)
	}
	elapsed := time.Since(start)

	sort.Slice(m.latencies, func(a, b int) bool { return m.latencies[a] < m.latencies[b] })
	req, hit := m.blocksRequested.Load(), m.blocksHit.Load()
	costReq, costSaved := m.costRequestedUS.Load(), m.costSavedUS.Load()

	fmt.Printf(`
PrefixMesh loadgen v0  (seed=%d, %d requests, %d workers, %v)
  workload: %d tenants × %d-block system prompts, %d docs (zipf %.1f) × %d blocks, unique suffix
  block hit rate         : %.1f%%  (%d / %d blocks)
  prefill compute saved  : %.1f%%  (%.1fs of %.1fs simulated prefill)
  full-chain hits        : %d
  match latency          : p50 %v   p99 %v
`,
		cfg.seed, cfg.requests, cfg.concurrency, elapsed.Round(time.Millisecond),
		cfg.tenants, cfg.sysBlocks, cfg.docs, cfg.zipfS, cfg.docBlocks,
		100*float64(hit)/float64(req), hit, req,
		100*float64(costSaved)/float64(costReq),
		float64(costSaved)/1e6, float64(costReq)/1e6,
		m.fullHits.Load(),
		percentile(m.latencies, 0.50).Round(10*time.Microsecond),
		percentile(m.latencies, 0.99).Round(10*time.Microsecond),
	)
}
