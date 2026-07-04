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
// every replica (DESIGN.md §4.3), so each replica has an independent view.
//
// Honesty note vs the design's "suspected-by-quorum": in M1 a single replica
// that misses DeadAfter of heartbeats proposes NodeDead on its own — there
// is no suspicion exchange yet. A false positive (blip between one replica
// and a healthy node) removes the node, which then sees itself missing from
// the ring and rejoins: an epoch flap, not an outage. Quorum suspicion is
// tracked for M2.
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

// runFailureDetector proposes NodeDead for members whose heartbeats stopped.
// All replicas run this; duplicate proposals are safe (removing an absent
// member is a no-op that doesn't bump the epoch). Jitter reduces duels.
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
			slog.Info("directory: proposing NodeDead", "node", id)
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
