package blockstore

import (
	"container/list"
	"sync"

	"github.com/surya16122114/prefixmesh/internal/chain"
)

// perBlockOverhead approximates map/list bookkeeping per block so capacity
// budgets stay honest for small payloads.
const perBlockOverhead = 128

// LRU is the M0 Store: plain least-recently-used eviction over a byte budget.
// It stays behind a flag as the benchmark baseline once the M2 cost-aware
// store lands.
type LRU struct {
	mu    sync.Mutex
	cap   uint64
	used  uint64
	ll    *list.List // front = most recently used
	items map[chain.BlockID]*list.Element
	stats Stats
}

type entry struct {
	block Block
	size  uint64
}

func NewLRU(capacityBytes uint64) *LRU {
	return &LRU{
		cap:   capacityBytes,
		ll:    list.New(),
		items: make(map[chain.BlockID]*list.Element),
	}
}

func blockSize(b Block) uint64 {
	return uint64(len(b.Payload)) + perBlockOverhead
}

func (s *LRU) Put(b Block) bool {
	size := blockSize(b)
	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.items[b.ID]; ok {
		s.ll.MoveToFront(el) // re-put of a present block acts as Touch
		return false
	}
	if size > s.cap {
		return false // oversized block can never fit; treat as uncacheable
	}
	for s.used+size > s.cap {
		s.evictOldest()
	}
	s.items[b.ID] = s.ll.PushFront(&entry{block: b, size: size})
	s.used += size
	return true
}

func (s *LRU) Get(id chain.BlockID) (Block, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[id]
	if !ok {
		s.stats.Misses++
		return Block{}, false
	}
	s.stats.Hits++
	s.ll.MoveToFront(el)
	return el.Value.(*entry).block, true
}

func (s *LRU) Contains(id chain.BlockID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[id]
	if !ok {
		s.stats.Misses++
		return false
	}
	s.stats.Hits++
	s.ll.MoveToFront(el) // a Contains hit means a reader is about to arrive
	return true
}

func (s *LRU) Touch(id chain.BlockID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[id]; ok {
		s.ll.MoveToFront(el)
	}
}

func (s *LRU) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	st.OccupancyBytes = s.used
	st.CapacityBytes = s.cap
	return st
}

// evictOldest must be called with mu held and a non-empty list.
func (s *LRU) evictOldest() {
	el := s.ll.Back()
	e := el.Value.(*entry)
	s.ll.Remove(el)
	delete(s.items, e.block.ID)
	s.used -= e.size
	s.stats.Evictions++
	s.stats.EvictedCostUS += e.block.CostUS
}

var _ Store = (*LRU)(nil)
