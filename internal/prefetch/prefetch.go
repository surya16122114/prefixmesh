// Package prefetch implements the event-plane consumer that turns access
// telemetry into cache-warming commands (DESIGN.md §4.4).
//
// The model: per-block demand as a time-decayed hit count, plus the
// parent->child chain edges observed in traffic (the first-order Markov
// structure — each event's chain IS a walk through the block graph). Warming
// triggers on ring-epoch changes: when a demanded block's owner set gains a
// node (join, or the ring healing around a death), the new owner probably
// doesn't have the block — so we tell it to fetch from a surviving owner.
// This both re-warms shifted ranges and restores RF=2 redundancy after a
// node loss, ahead of demand.
package prefetch

import (
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/chain"
	"github.com/surya16122114/prefixmesh/internal/events"
	"github.com/surya16122114/prefixmesh/internal/hashring"
)

type blockStat struct {
	demand   float64 // decayed access count as of lastSeen
	lastSeen time.Time
	parent   chain.BlockID
	modelID  string
}

type Config struct {
	// RF is the mesh's replication factor; warm targets are the first RF
	// ring owners.
	RF int
	// DemandThreshold gates warming: blocks seen fewer (decayed) times don't
	// get commands. Default 2 — one hit could be anything, two is a pattern.
	DemandThreshold float64
	// HalfLife of block demand. Default 60s.
	HalfLife time.Duration
	// MaxTracked bounds memory; lowest-demand entries are swept. Default 100k.
	MaxTracked int
	// WarmDeadline is how long a command stays actionable. Default 30s.
	WarmDeadline time.Duration
}

func (c *Config) defaults() {
	if c.RF == 0 {
		c.RF = 2
	}
	if c.DemandThreshold == 0 {
		c.DemandThreshold = 2
	}
	if c.HalfLife == 0 {
		c.HalfLife = time.Minute
	}
	if c.MaxTracked == 0 {
		c.MaxTracked = 100_000
	}
	if c.WarmDeadline == 0 {
		c.WarmDeadline = 30 * time.Second
	}
}

type Prefetcher struct {
	cfg      Config
	producer events.Producer

	mu     sync.Mutex
	blocks map[chain.BlockID]*blockStat
	ring   *hashring.Ring // last ring we planned against

	// counters for the effectiveness log line
	warmsIssued uint64
}

func New(cfg Config, producer events.Producer) *Prefetcher {
	cfg.defaults()
	return &Prefetcher{
		cfg:      cfg,
		producer: producer,
		blocks:   map[chain.BlockID]*blockStat{},
	}
}

// HandleAccess is the events.Handler for prefix.access.v1.
func (p *Prefetcher) HandleAccess(_, value []byte) {
	var ev pmv1.AccessEvent
	if err := proto.Unmarshal(value, &ev); err != nil {
		return // lossy plane, garbage in -> drop
	}
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	parent := chain.BlockID{} // roots keep a zero parent; nobody warms h_0
	for i, raw := range ev.Chain {
		if len(raw) != chain.HashSize {
			return
		}
		id := chain.BlockID(raw)
		st, ok := p.blocks[id]
		if !ok {
			st = &blockStat{lastSeen: now, parent: parent, modelID: ev.ModelId}
			p.blocks[id] = st
		}
		dt := now.Sub(st.lastSeen).Seconds() / p.cfg.HalfLife.Seconds()
		st.demand = 1 + st.demand*math.Exp2(-dt)
		st.lastSeen = now
		parent = id
		_ = i
	}
	if len(p.blocks) > p.cfg.MaxTracked {
		p.sweepLocked(now)
	}
}

// sweepLocked drops the coldest half. Called with p.mu held, rarely.
func (p *Prefetcher) sweepLocked(now time.Time) {
	type cand struct {
		id chain.BlockID
		d  float64
	}
	all := make([]cand, 0, len(p.blocks))
	for id, st := range p.blocks {
		dt := now.Sub(st.lastSeen).Seconds() / p.cfg.HalfLife.Seconds()
		all = append(all, cand{id, st.demand * math.Exp2(-dt)})
	}
	// A full sort at MaxTracked entries on a rare path is fine.
	sort.Slice(all, func(i, j int) bool { return all[i].d < all[j].d })
	for _, c := range all[:len(all)/2] {
		delete(p.blocks, c.id)
	}
}

// OnRing is wired to ringwatch: every committed epoch triggers a warm plan
// against the previous ring.
func (p *Prefetcher) OnRing(r *pmv1.Ring) {
	nodes := make(map[string]string, len(r.Nodes))
	for _, n := range r.Nodes {
		nodes[n.NodeId] = n.Addr
	}
	newRing := hashring.New(r.Epoch, nodes, int(r.VnodesPerNode))

	p.mu.Lock()
	oldRing := p.ring
	p.ring = newRing
	if oldRing == nil || newRing.Epoch <= oldRing.Epoch {
		p.mu.Unlock()
		return
	}
	now := time.Now()
	type warm struct {
		id      chain.BlockID
		st      blockStat
		targets []string // node ids
		source  string   // addr
		conf    float64
	}
	var plan []warm
	newNodes := newRing.Nodes()
	for id, st := range p.blocks {
		dt := now.Sub(st.lastSeen).Seconds() / p.cfg.HalfLife.Seconds()
		demand := st.demand * math.Exp2(-dt)
		if demand < p.cfg.DemandThreshold {
			continue
		}
		oldOwners := oldRing.Owners(id[:], p.cfg.RF)
		newOwners := newRing.Owners(id[:], p.cfg.RF)
		// A surviving old owner is the copy source; if none survived the
		// block is unrecoverable by warming (client recompute refills it).
		source := ""
		oldSet := map[string]struct{}{}
		for _, o := range oldOwners {
			oldSet[o] = struct{}{}
			if _, alive := newNodes[o]; alive && source == "" {
				source = newNodes[o]
			}
		}
		if source == "" {
			continue
		}
		var targets []string
		for _, o := range newOwners {
			if _, hadIt := oldSet[o]; !hadIt {
				targets = append(targets, o)
			}
		}
		if len(targets) == 0 {
			continue
		}
		plan = append(plan, warm{id: id, st: *st, targets: targets, source: source,
			conf: 1 - math.Exp2(-demand)})
	}
	p.mu.Unlock()

	for _, w := range plan {
		for _, target := range w.targets {
			cmd := &pmv1.WarmCommand{
				BlockId:        w.id[:],
				ParentId:       w.st.parent[:],
				ModelId:        w.st.modelID,
				SourceNode:     w.source,
				TargetNode:     target,
				Confidence:     float32(w.conf),
				DeadlineUnixMs: now.Add(p.cfg.WarmDeadline).UnixMilli(),
			}
			raw, err := proto.Marshal(cmd)
			if err != nil {
				continue
			}
			p.producer.Produce(events.TopicWarm, w.id[:], raw)
			p.warmsIssued++
		}
	}
	if len(plan) > 0 {
		slog.Info("prefetch: warm plan issued",
			"epoch", newRing.Epoch, "blocks", len(plan), "total_issued", p.warmsIssued)
	}
}
