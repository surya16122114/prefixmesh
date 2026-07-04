// Package ringwatch is the fleet side of the control plane: it holds a
// WatchRing stream open against any reachable directory replica and
// maintains the current hash ring (and, for gateways, live cache-node
// clients) from the epoch updates.
package ringwatch

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/hashring"
)

type Watcher struct {
	directories []string
	dialNodes   bool // gateways need cache-node clients; cache nodes don't

	mu      sync.RWMutex
	ring    *hashring.Ring
	conns   map[string]*grpc.ClientConn // nodeID -> conn
	clients map[string]pmv1.CacheNodeServiceClient
	onRing  func(*pmv1.Ring)
}

// New creates a watcher over the given directory replica addresses.
// dialNodes controls whether cache-node connections are maintained.
// onRing, if non-nil, is invoked for every ring update (cache nodes use it
// to track the epoch and detect their own eviction from membership).
func New(directories []string, dialNodes bool, onRing func(*pmv1.Ring)) *Watcher {
	return &Watcher{
		directories: directories,
		dialNodes:   dialNodes,
		conns:       map[string]*grpc.ClientConn{},
		clients:     map[string]pmv1.CacheNodeServiceClient{},
		onRing:      onRing,
	}
}

// Run blocks, cycling through directory replicas with backoff until ctx
// ends. Losing the stream never clears the last known ring: a frozen ring
// (directory quorum down) still serves traffic (DESIGN.md §6).
func (w *Watcher) Run(ctx context.Context) {
	backoff := 100 * time.Millisecond
	for i := 0; ctx.Err() == nil; i = (i + 1) % len(w.directories) {
		if err := w.watchOne(ctx, w.directories[i]); err != nil && ctx.Err() == nil {
			slog.Warn("ringwatch: stream lost", "directory", w.directories[i], "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 3*time.Second {
			backoff *= 2
		}
	}
}

func (w *Watcher) watchOne(ctx context.Context, addr string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := pmv1.NewDirectoryServiceClient(conn).WatchRing(ctx, &pmv1.WatchRingRequest{
		KnownEpoch: w.Epoch(),
	})
	if err != nil {
		return err
	}
	for {
		ring, err := stream.Recv()
		if err != nil {
			return err
		}
		w.apply(ring)
	}
}

func (w *Watcher) apply(ring *pmv1.Ring) {
	nodes := make(map[string]string, len(ring.Nodes))
	for _, n := range ring.Nodes {
		nodes[n.NodeId] = n.Addr
	}

	w.mu.Lock()
	if w.ring != nil && ring.Epoch <= w.ring.Epoch && ring.Epoch != 0 {
		w.mu.Unlock()
		return // stale replay from a lagging replica
	}
	w.ring = hashring.New(ring.Epoch, nodes, int(ring.VnodesPerNode))
	if w.dialNodes {
		for id, addr := range nodes {
			if _, ok := w.conns[id]; ok {
				continue
			}
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				slog.Warn("ringwatch: dial failed", "node", id, "err", err)
				continue
			}
			w.conns[id] = conn
			w.clients[id] = pmv1.NewCacheNodeServiceClient(conn)
		}
		for id, conn := range w.conns {
			if _, ok := nodes[id]; !ok {
				conn.Close()
				delete(w.conns, id)
				delete(w.clients, id)
			}
		}
	}
	w.mu.Unlock()

	slog.Info("ringwatch: ring updated", "epoch", ring.Epoch, "members", len(nodes))
	if w.onRing != nil {
		w.onRing(ring)
	}
}

// Ring returns the latest known ring, or nil before the first update.
func (w *Watcher) Ring() *hashring.Ring {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.ring
}

func (w *Watcher) Epoch() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.ring == nil {
		return 0
	}
	return w.ring.Epoch
}

func (w *Watcher) Client(nodeID string) (pmv1.CacheNodeServiceClient, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	c, ok := w.clients[nodeID]
	return c, ok
}
