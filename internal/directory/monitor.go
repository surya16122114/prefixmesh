package directory

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
)

// hbTracker records the last heartbeat per member. Cache nodes heartbeat
// every replica (DESIGN.md §4.3), so each replica has an independent view;
// NodeDead is only proposed once a majority of replicas share the suspicion
// (runFailureDetector), so one replica's network blip cannot evict a healthy
// node. Should a false positive still slip through, the node sees itself
// missing from the ring and rejoins: an epoch flap, not an outage.
type hbTracker struct {
	cfg Config
	mu  sync.Mutex
	las map[string]time.Time
}

func newHBTracker(cfg Config) *hbTracker {
	return &hbTracker{cfg: cfg, las: map[string]time.Time{}}
}

func (h *hbTracker) record(nodeID string) {
	h.mu.Lock()
	h.las[nodeID] = time.Now()
	h.mu.Unlock()
}

// seed gives a member a full grace period, used on join application so a
// node isn't declared dead before its first heartbeat lands.
func (h *hbTracker) seed(nodeID string) {
	h.record(nodeID)
}

func (h *hbTracker) expired(nodeID string, now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	last, ok := h.las[nodeID]
	if !ok {
		return false // never heard of it; seed happens on join application
	}
	return now.Sub(last) > h.cfg.DeadAfter
}

// runFailureDetector proposes NodeDead for members whose heartbeats stopped —
// but only after confirming the suspicion with a majority of replicas
// (Suspect RPC). All replicas run this; duplicate proposals are safe
// (removing an absent member is a no-op that doesn't bump the epoch).
// Jitter reduces duels.
func (s *Server) runFailureDetector(ctx context.Context) {
	for {
		jitter := time.Duration(rand.Int63n(int64(s.hb.cfg.HeartbeatEvery / 2)))
		t := time.NewTimer(s.hb.cfg.HeartbeatEvery + jitter)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		now := time.Now()
		for _, id := range s.sm.Members() {
			if !s.hb.expired(id, now) {
				continue
			}
			if !s.quorumSuspects(ctx, id) {
				continue // only we stopped hearing it; probably our blip
			}
			slog.Info("directory: proposing NodeDead (quorum-suspected)", "node", id)
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := s.submit(cctx, &pmv1.ClusterCommand{
				Cmd: &pmv1.ClusterCommand_NodeDead{NodeDead: id},
			}); err != nil {
				slog.Warn("directory: NodeDead proposal failed", "node", id, "err", err)
			}
			cancel()
		}
	}
}

// quorumSuspects asks peer replicas whether they also lost the node's
// heartbeats; true iff a majority (self included) suspects. An unreachable
// peer counts as not suspecting — erring toward keeping members alive.
func (s *Server) quorumSuspects(ctx context.Context, nodeID string) bool {
	suspecting := 1 // self
	total := len(s.tr.peers) + 1
	for peer := range s.tr.peers {
		c, err := s.tr.client(peer)
		if err != nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, s.hb.cfg.HeartbeatEvery)
		resp, err := c.Suspect(cctx, &pmv1.SuspectRequest{NodeId: nodeID})
		cancel()
		if err == nil && resp.Suspect {
			suspecting++
		}
	}
	return suspecting > total/2
}
