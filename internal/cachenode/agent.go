package cachenode

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/blockstore"
	"github.com/surya16122114/prefixmesh/internal/ringwatch"
)

// Agent is a cache node's control-plane side (DESIGN.md §4.2/4.3): join the
// directory, heartbeat every replica, watch the ring for the current epoch,
// and rejoin if the failure detector ever (rightly or wrongly) removed us.
type Agent struct {
	Self        *pmv1.NodeInfo
	Directories []string
	Store       blockstore.Store // occupancy for heartbeats
	Every       time.Duration    // heartbeat interval, default 500ms

	watcher *ringwatch.Watcher
	rejoin  chan struct{}
}

// NewAgent wires the agent; Epoch() is valid immediately (0 until the first
// ring update arrives).
func NewAgent(self *pmv1.NodeInfo, directories []string, store blockstore.Store) *Agent {
	a := &Agent{
		Self:        self,
		Directories: directories,
		Store:       store,
		Every:       500 * time.Millisecond,
		rejoin:      make(chan struct{}, 1),
	}
	a.watcher = ringwatch.New(directories, false, func(ring *pmv1.Ring) {
		for _, n := range ring.Nodes {
			if n.NodeId == self.NodeId {
				return
			}
		}
		// We've been declared dead (or never joined). Self-heal by
		// rejoining — a false-positive removal becomes an epoch flap,
		// not an outage.
		select {
		case a.rejoin <- struct{}{}:
		default:
		}
	})
	return a
}

func (a *Agent) Epoch() uint64 { return a.watcher.Epoch() }

// Run blocks until ctx ends.
func (a *Agent) Run(ctx context.Context) {
	clients := make([]pmv1.DirectoryServiceClient, 0, len(a.Directories))
	for _, addr := range a.Directories {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("agent: directory dial failed", "addr", addr, "err", err)
			continue
		}
		defer conn.Close()
		clients = append(clients, pmv1.NewDirectoryServiceClient(conn))
	}
	if len(clients) == 0 {
		slog.Error("agent: no reachable directories configured")
		return
	}

	go a.watcher.Run(ctx)
	a.join(ctx, clients)

	t := time.NewTicker(a.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.rejoin:
			a.join(ctx, clients)
		case <-t.C:
			var occupancy uint64
			if a.Store != nil {
				occupancy = a.Store.Stats().OccupancyBytes
			}
			// Heartbeat every replica: each runs its own failure detector.
			for _, c := range clients {
				cctx, cancel := context.WithTimeout(ctx, a.Every)
				_, _ = c.Heartbeat(cctx, &pmv1.HeartbeatRequest{
					NodeId:         a.Self.NodeId,
					OccupancyBytes: occupancy,
					RingEpoch:      a.Epoch(),
				})
				cancel()
			}
		}
	}
}

// join tries replicas until one commits our membership.
func (a *Agent) join(ctx context.Context, clients []pmv1.DirectoryServiceClient) {
	for ctx.Err() == nil {
		for _, c := range clients {
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			resp, err := c.Join(cctx, &pmv1.JoinRequest{Node: a.Self})
			cancel()
			if err == nil {
				slog.Info("agent: joined", "node", a.Self.NodeId, "epoch", resp.Ring.Epoch)
				return
			}
			slog.Warn("agent: join attempt failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
