package cachenode

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/blockstore"
	"github.com/surya16122114/prefixmesh/internal/chain"
)

// Warmer executes cache.warm.v1 commands addressed to this node: fetch the
// block from the surviving owner and store it locally. Best-effort and
// rate-limited so warming never competes with foreground traffic; a warm of
// a present block is a no-op (content addressing makes redelivery free).
type Warmer struct {
	self  string
	store blockstore.Store

	// token bucket: RatePerSec warms max, burst of the same size
	mu      sync.Mutex
	tokens  float64
	lastRef time.Time
	rate    float64

	clients map[string]pmv1.CacheNodeServiceClient // by source addr

	// counters surfaced in logs (Prometheus in M4)
	executed, skippedPresent, dropped uint64
}

func NewWarmer(self string, store blockstore.Store, ratePerSec float64) *Warmer {
	if ratePerSec <= 0 {
		ratePerSec = 200
	}
	return &Warmer{
		self:    self,
		store:   store,
		tokens:  ratePerSec,
		lastRef: time.Now(),
		rate:    ratePerSec,
		clients: map[string]pmv1.CacheNodeServiceClient{},
	}
}

func (w *Warmer) allow() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	w.tokens = min(w.rate, w.tokens+now.Sub(w.lastRef).Seconds()*w.rate)
	w.lastRef = now
	if w.tokens < 1 {
		return false
	}
	w.tokens--
	return true
}

func (w *Warmer) client(addr string) (pmv1.CacheNodeServiceClient, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if c, ok := w.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := pmv1.NewCacheNodeServiceClient(conn)
	w.clients[addr] = c
	return c, nil
}

// Handle is the events.Handler for cache.warm.v1.
func (w *Warmer) Handle(_, value []byte) {
	var cmd pmv1.WarmCommand
	if err := proto.Unmarshal(value, &cmd); err != nil {
		return
	}
	if cmd.TargetNode != w.self || len(cmd.BlockId) != chain.HashSize {
		return
	}
	if cmd.DeadlineUnixMs > 0 && time.Now().UnixMilli() > cmd.DeadlineUnixMs {
		w.dropped++
		return // stale command; demand has moved on
	}
	if w.store.Contains(chain.BlockID(cmd.BlockId)) {
		w.skippedPresent++
		return
	}
	if cmd.SourceNode == "" {
		return // RECOMPUTE source: meaningless in the simulation
	}
	if !w.allow() {
		w.dropped++
		return // over budget; foreground traffic wins
	}
	c, err := w.client(cmd.SourceNode)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := c.GetBlocks(ctx, &pmv1.GetBlocksRequest{BlockIds: [][]byte{cmd.BlockId}})
	if err != nil {
		return
	}
	resp, err := stream.Recv()
	if err != nil || resp.Block == nil {
		return // source lost it; a client miss will refill eventually
	}
	b, err := fromProto(resp.Block)
	if err != nil {
		return
	}
	w.store.Put(b)
	w.executed++
	if w.executed%100 == 1 {
		slog.Info("warmer: progress",
			"executed", w.executed, "skipped_present", w.skippedPresent, "dropped", w.dropped)
	}
}
