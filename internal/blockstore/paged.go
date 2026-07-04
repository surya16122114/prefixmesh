package blockstore

import (
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/surya16122114/prefixmesh/internal/chain"
)

// EvictionPolicy selects the M2 paged store's victim strategy.
type EvictionPolicy string

const (
	// EvictLRU is the baseline: evict the least recently used block.
	EvictLRU EvictionPolicy = "lru"
	// EvictCost evicts the lowest frecency × cost / size score: cheap-to-
	// recompute blocks go first, expensive prefill survivors stay
	// (DESIGN.md §4.2).
	EvictCost EvictionPolicy = "cost"
)

// evictSample is how many candidates the cost policy samples per eviction
// (Redis-style approximated-min: O(sample) instead of a heap on every access).
const evictSample = 16

// frecencyHalfLife is the age, measured in store accesses, at which a
// block's access-frequency weight halves.
const frecencyHalfLife = 4096

// Paged is the M2 Store: block payloads live in fixed-size pages of one
// preallocated arena (simulating pinned GPU/host cache memory: no per-block
// allocations, occupancy is exact pages, fragmentation is impossible by
// construction since any free page fits any need).
type Paged struct {
	pageSize int
	arena    []byte
	free     []int32 // free page indices, LIFO

	mu    sync.Mutex
	items map[chain.BlockID]*pagedEntry
	order []*pagedEntry // index for O(1) random sampling (cost policy)
	head  *pagedEntry   // LRU list, most recent
	tail  *pagedEntry
	clock uint64 // logical time: one tick per access
	rng   *rand.Rand
	pol   EvictionPolicy
	stats Stats
}

type pagedEntry struct {
	id         chain.BlockID
	parentID   chain.BlockID
	modelID    string
	tokenCount uint32
	costUS     uint64
	size       int     // payload bytes
	pages      []int32 // arena pages, in payload order
	orderIdx   int     // position in Paged.order

	lastAccess uint64  // clock at last touch
	freq       float64 // decayed access count as of lastAccess

	prev, next *pagedEntry // LRU list
}

// NewPaged preallocates capacityBytes of arena in pageSize pages.
func NewPaged(capacityBytes uint64, pageSize int, pol EvictionPolicy) *Paged {
	if pageSize <= 0 {
		panic("blockstore: pageSize must be positive")
	}
	nPages := int(capacityBytes) / pageSize
	p := &Paged{
		pageSize: pageSize,
		arena:    make([]byte, nPages*pageSize),
		free:     make([]int32, nPages),
		items:    map[chain.BlockID]*pagedEntry{},
		rng:      rand.New(rand.NewSource(1)),
		pol:      pol,
	}
	for i := range p.free {
		p.free[i] = int32(i)
	}
	p.stats.CapacityBytes = uint64(nPages * pageSize)
	return p
}

func (p *Paged) pagesNeeded(size int) int {
	return (size + p.pageSize - 1) / p.pageSize
}

// touch updates recency + frecency. Callers hold p.mu.
func (p *Paged) touch(e *pagedEntry) {
	p.clock++
	age := float64(p.clock - e.lastAccess)
	e.freq = 1 + e.freq*math.Exp2(-age/frecencyHalfLife)
	e.lastAccess = p.clock
	p.lruMoveFront(e)
}

// score is the eviction priority (lower = evict first): frecency × cost per
// byte of arena actually held.
func (e *pagedEntry) score(pageSize int) float64 {
	held := float64(len(e.pages) * pageSize)
	return e.freq * float64(e.costUS) / held
}

func (p *Paged) Put(b Block) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.items[b.ID]; ok {
		p.touch(e) // re-put of a present block acts as Touch
		return false
	}
	need := p.pagesNeeded(len(b.Payload))
	if need == 0 || need > len(p.arena)/p.pageSize {
		return false // empty or can-never-fit blocks are uncacheable
	}
	for len(p.free) < need {
		if !p.evictOne() {
			return false // nothing evictable left (shouldn't happen)
		}
	}
	e := &pagedEntry{
		id:         b.ID,
		parentID:   b.ParentID,
		modelID:    b.ModelID,
		tokenCount: b.TokenCount,
		costUS:     b.CostUS,
		size:       len(b.Payload),
		pages:      make([]int32, need),
	}
	for i := 0; i < need; i++ {
		pg := p.free[len(p.free)-1]
		p.free = p.free[:len(p.free)-1]
		e.pages[i] = pg
		lo := i * p.pageSize
		hi := min(lo+p.pageSize, len(b.Payload))
		copy(p.arena[int(pg)*p.pageSize:], b.Payload[lo:hi])
	}
	e.orderIdx = len(p.order)
	p.order = append(p.order, e)
	p.items[b.ID] = e
	p.lruPushFront(e)
	p.touch(e)
	p.stats.OccupancyBytes += uint64(need * p.pageSize)
	return true
}

func (p *Paged) Get(id chain.BlockID) (Block, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.items[id]
	if !ok {
		p.stats.Misses++
		return Block{}, false
	}
	p.stats.Hits++
	p.touch(e)
	payload := make([]byte, e.size)
	for i, pg := range e.pages {
		lo := i * p.pageSize
		hi := min(lo+p.pageSize, e.size)
		copy(payload[lo:hi], p.arena[int(pg)*p.pageSize:])
	}
	return Block{
		ID:         e.id,
		ParentID:   e.parentID,
		ModelID:    e.modelID,
		Payload:    payload,
		TokenCount: e.tokenCount,
		CostUS:     e.costUS,
	}, true
}

func (p *Paged) Contains(id chain.BlockID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.items[id]
	if !ok {
		p.stats.Misses++
		return false
	}
	p.stats.Hits++
	p.touch(e) // a Contains hit means a reader is about to arrive
	return true
}

func (p *Paged) Touch(id chain.BlockID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.items[id]; ok {
		p.touch(e)
	}
}

func (p *Paged) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// evictOne frees one block's pages. Callers hold p.mu.
func (p *Paged) evictOne() bool {
	var victim *pagedEntry
	var victimScore float64
	switch p.pol {
	case EvictCost:
		// Approximated-min: sample candidates, evict the worst scorer. The
		// frecency in the score is decayed to "now" for a fair comparison.
		for i := 0; i < evictSample && i < len(p.order); i++ {
			e := p.order[p.rng.Intn(len(p.order))]
			age := float64(p.clock - e.lastAccess)
			decayed := e.freq * math.Exp2(-age/frecencyHalfLife)
			eScore := decayed * float64(e.costUS) / float64(len(e.pages)*p.pageSize)
			if victim == nil || eScore < victimScore {
				victim, victimScore = e, eScore
			}
		}
	default: // EvictLRU
		victim = p.tail
	}
	if victim == nil {
		return false
	}
	p.free = append(p.free, victim.pages...)
	p.lruRemove(victim)
	// swap-remove from the sampling index
	last := p.order[len(p.order)-1]
	p.order[victim.orderIdx] = last
	last.orderIdx = victim.orderIdx
	p.order = p.order[:len(p.order)-1]
	delete(p.items, victim.id)
	p.stats.OccupancyBytes -= uint64(len(victim.pages) * p.pageSize)
	p.stats.Evictions++
	p.stats.EvictedCostUS += victim.costUS
	return true
}

// --- intrusive LRU list (head = most recent) ---

func (p *Paged) lruPushFront(e *pagedEntry) {
	e.prev, e.next = nil, p.head
	if p.head != nil {
		p.head.prev = e
	}
	p.head = e
	if p.tail == nil {
		p.tail = e
	}
}

func (p *Paged) lruRemove(e *pagedEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else if p.head == e {
		p.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else if p.tail == e {
		p.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (p *Paged) lruMoveFront(e *pagedEntry) {
	if p.head == e {
		return
	}
	p.lruRemove(e)
	p.lruPushFront(e)
}

var _ Store = (*Paged)(nil)

// ParsePolicy converts a flag value.
func ParsePolicy(s string) (EvictionPolicy, error) {
	switch EvictionPolicy(s) {
	case EvictLRU, EvictCost:
		return EvictionPolicy(s), nil
	}
	return "", fmt.Errorf("unknown eviction policy %q (want lru|cost)", s)
}
