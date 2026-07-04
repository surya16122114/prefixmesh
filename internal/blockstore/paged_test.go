package blockstore

import (
	"bytes"
	"testing"

	"github.com/surya16122114/prefixmesh/internal/chain"
)

func pblk(seed int, payloadLen int, costUS uint64) Block {
	var id chain.BlockID
	id[0], id[1], id[2] = byte(seed), byte(seed>>8), byte(seed>>16)
	payload := make([]byte, payloadLen)
	for i := range payload {
		payload[i] = byte(seed + i)
	}
	return Block{ID: id, Payload: payload, CostUS: costUS}
}

func TestPagedRoundtripAcrossPageBoundaries(t *testing.T) {
	p := NewPaged(1<<20, 4096, EvictLRU)
	// 10000 bytes = 2 full pages + a 1808-byte tail.
	b := pblk(1, 10000, 100)
	if !p.Put(b) {
		t.Fatal("put failed")
	}
	got, ok := p.Get(b.ID)
	if !ok || !bytes.Equal(got.Payload, b.Payload) {
		t.Fatal("payload corrupted across page boundaries")
	}
	if occ := p.Stats().OccupancyBytes; occ != 3*4096 {
		t.Fatalf("occupancy %d, want 3 pages", occ)
	}
}

func TestPagedPageAccounting(t *testing.T) {
	// 8 pages of 1 KiB; each 1 KiB block takes exactly one page.
	p := NewPaged(8<<10, 1<<10, EvictLRU)
	for i := 0; i < 8; i++ {
		if !p.Put(pblk(i, 1<<10, 100)) {
			t.Fatalf("put %d failed with free pages available", i)
		}
	}
	if p.Stats().OccupancyBytes != 8<<10 {
		t.Fatal("arena should be exactly full")
	}
	// Ninth block must evict exactly one page's worth.
	if !p.Put(pblk(100, 1<<10, 100)) {
		t.Fatal("put under pressure failed")
	}
	st := p.Stats()
	if st.Evictions != 1 || st.OccupancyBytes != 8<<10 {
		t.Fatalf("evictions=%d occupancy=%d", st.Evictions, st.OccupancyBytes)
	}
}

func TestPagedLRUEvictsOldest(t *testing.T) {
	p := NewPaged(2<<10, 1<<10, EvictLRU)
	a, b, c := pblk(1, 1<<10, 100), pblk(2, 1<<10, 100), pblk(3, 1<<10, 100)
	p.Put(a)
	p.Put(b)
	p.Get(a.ID) // a most recent; b is LRU
	p.Put(c)
	if !p.Contains(a.ID) || p.Contains(b.ID) {
		t.Fatal("LRU policy evicted the wrong block")
	}
}

// TestCostAwareKeepsExpensiveBlocks is the M2 claim in miniature: under
// pressure, the cost policy sacrifices cheap blocks even when they're more
// recent than expensive ones.
func TestCostAwareKeepsExpensiveBlocks(t *testing.T) {
	const pages = 64
	p := NewPaged(pages<<10, 1<<10, EvictCost)

	// Fill half the arena with EXPENSIVE blocks, then touch them a few times
	// so frequency reflects real reuse.
	var expensive []Block
	for i := 0; i < pages/2; i++ {
		b := pblk(1000+i, 1<<10, 100_000)
		expensive = append(expensive, b)
		p.Put(b)
	}
	for range 3 {
		for _, b := range expensive {
			p.Get(b.ID)
		}
	}
	// Now churn twice the arena size of CHEAP one-shot blocks through the
	// store (recency-hot, never reused — loadgen's unique suffixes).
	for i := 0; i < pages*2; i++ {
		p.Put(pblk(5000+i, 1<<10, 100))
	}

	kept := 0
	for _, b := range expensive {
		// Stats-neutral existence check would be nicer, but Contains'
		// recency bump doesn't affect the assertion.
		if p.Contains(b.ID) {
			kept++
		}
	}
	if kept < len(expensive)*3/4 {
		t.Fatalf("cost policy kept only %d/%d expensive blocks under cheap churn",
			kept, len(expensive))
	}

	// Same experiment under LRU must do strictly worse: churn evicts all.
	q := NewPaged(pages<<10, 1<<10, EvictLRU)
	for _, b := range expensive {
		q.Put(b)
	}
	for range 3 {
		for _, b := range expensive {
			q.Get(b.ID)
		}
	}
	for i := 0; i < pages*2; i++ {
		q.Put(pblk(5000+i, 1<<10, 100))
	}
	lruKept := 0
	for _, b := range expensive {
		if q.Contains(b.ID) {
			lruKept++
		}
	}
	if lruKept >= kept {
		t.Fatalf("LRU kept %d expensive blocks, cost policy kept %d — no advantage measured",
			lruKept, kept)
	}
}

func TestPagedOversizedAndEmptyRejected(t *testing.T) {
	p := NewPaged(4<<10, 1<<10, EvictLRU)
	if p.Put(pblk(1, 8<<10, 100)) {
		t.Fatal("block larger than the arena must be rejected")
	}
	if p.Put(pblk(2, 0, 100)) {
		t.Fatal("empty payload must be rejected")
	}
	if p.Stats().OccupancyBytes != 0 {
		t.Fatal("rejected blocks must not hold pages")
	}
}

func TestPagedManyBlocksStress(t *testing.T) {
	p := NewPaged(1<<20, 4096, EvictCost)
	for i := 0; i < 2000; i++ {
		p.Put(pblk(i, 3000+i%5000, uint64(100+i)))
		if i%3 == 0 {
			p.Get(pblk(i/2, 0, 0).ID)
		}
	}
	st := p.Stats()
	if st.OccupancyBytes > st.CapacityBytes {
		t.Fatalf("occupancy %d exceeds capacity %d", st.OccupancyBytes, st.CapacityBytes)
	}
	// Every retrievable block must round-trip intact after heavy churn.
	found := 0
	for i := 0; i < 2000; i++ {
		want := pblk(i, 3000+i%5000, uint64(100+i))
		got, ok := p.Get(want.ID)
		if !ok {
			continue
		}
		found++
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("block %d corrupted after churn", i)
		}
	}
	if found == 0 {
		t.Fatal("nothing survived — accounting is broken")
	}
}

func TestParsePolicy(t *testing.T) {
	for _, ok := range []string{"lru", "cost"} {
		if _, err := ParsePolicy(ok); err != nil {
			t.Errorf("ParsePolicy(%q) errored: %v", ok, err)
		}
	}
	if _, err := ParsePolicy("arc"); err == nil {
		t.Error("unknown policy accepted")
	}
}
