package blockstore

import (
	"testing"

	"github.com/surya16122114/prefixmesh/internal/chain"
)

func blk(seed byte, payloadLen int) Block {
	var id chain.BlockID
	id[0] = seed
	return Block{ID: id, Payload: make([]byte, payloadLen), CostUS: uint64(seed)}
}

func TestPutGetRoundtrip(t *testing.T) {
	s := NewLRU(1 << 20)
	b := blk(1, 1000)
	if !s.Put(b) {
		t.Fatal("first put should store")
	}
	if s.Put(b) {
		t.Fatal("re-put should be a no-op")
	}
	got, ok := s.Get(b.ID)
	if !ok || len(got.Payload) != 1000 {
		t.Fatal("get after put failed")
	}
	if _, ok := s.Get(blk(9, 0).ID); ok {
		t.Fatal("absent block should miss")
	}
}

func TestEvictionOrderIsLRU(t *testing.T) {
	// Budget fits exactly two blocks (payload 1000 + overhead 128 each).
	s := NewLRU(2 * (1000 + perBlockOverhead))
	a, b, c := blk(1, 1000), blk(2, 1000), blk(3, 1000)
	s.Put(a)
	s.Put(b)
	s.Get(a.ID) // a is now most recent; b is the eviction candidate
	s.Put(c)    // must evict b, not a

	if !s.Contains(a.ID) {
		t.Fatal("recently used block was evicted")
	}
	if s.Contains(b.ID) {
		t.Fatal("LRU block survived eviction")
	}
	st := s.Stats()
	if st.Evictions != 1 || st.EvictedCostUS != 2 {
		t.Fatalf("eviction stats wrong: %+v", st)
	}
}

func TestOversizedBlockRejected(t *testing.T) {
	s := NewLRU(500)
	if s.Put(blk(1, 1000)) {
		t.Fatal("block larger than capacity must be rejected")
	}
	if s.Stats().OccupancyBytes != 0 {
		t.Fatal("rejected block must not consume budget")
	}
}

func TestOccupancyAccounting(t *testing.T) {
	s := NewLRU(1 << 20)
	s.Put(blk(1, 1000))
	s.Put(blk(2, 2000))
	want := uint64(1000+perBlockOverhead) + uint64(2000+perBlockOverhead)
	if got := s.Stats().OccupancyBytes; got != want {
		t.Fatalf("occupancy %d, want %d", got, want)
	}
}
