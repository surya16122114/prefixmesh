package prefetch_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/blockstore"
	"github.com/surya16122114/prefixmesh/internal/cachenode"
	"github.com/surya16122114/prefixmesh/internal/chain"
	"github.com/surya16122114/prefixmesh/internal/events"
	"github.com/surya16122114/prefixmesh/internal/hashring"
	"github.com/surya16122114/prefixmesh/internal/prefetch"
)

type node struct {
	id    string
	addr  string
	store blockstore.Store
}

func startNode(t *testing.T, ctx context.Context, id string, bus *events.Bus) *node {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	store := blockstore.NewLRU(1 << 26)
	g := grpc.NewServer()
	pmv1.RegisterCacheNodeServiceServer(g, cachenode.New(store))
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	warmer := cachenode.NewWarmer(id, store, 1000)
	go bus.Subscribe(events.TopicWarm, warmer.Handle).Run(ctx)
	return &node{id: id, addr: lis.Addr().String(), store: store}
}

func ringOf(epoch uint64, nodes ...*node) *pmv1.Ring {
	r := &pmv1.Ring{Epoch: epoch, VnodesPerNode: 64}
	for _, n := range nodes {
		r.Nodes = append(r.Nodes, &pmv1.NodeInfo{NodeId: n.id, Addr: n.addr})
	}
	return r
}

// TestWarmOnRingChange drives the full event-plane loop over the in-memory
// bus: demand builds from access events, a ring change strands demanded
// blocks on nodes that no longer own them, and the warm plan copies them to
// the new owner before any client asks.
func TestWarmOnRingChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := events.NewBus()

	n1 := startNode(t, ctx, "cn-1", bus)
	n2 := startNode(t, ctx, "cn-2", bus)
	n3 := startNode(t, ctx, "cn-3", bus)

	p := prefetch.New(prefetch.Config{RF: 1, DemandThreshold: 2}, bus)
	go bus.Subscribe(events.TopicAccess, p.HandleAccess).Run(ctx)

	// Epoch 1: two nodes. Blocks live on their epoch-1 owners.
	p.OnRing(ringOf(1, n1, n2))
	byID := map[string]*node{"cn-1": n1, "cn-2": n2, "cn-3": n3}
	ring1 := testRing(1, n1, n2)

	ids := chain.Build("m", 4, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	for _, id := range ids {
		owner := ring1.Owners(id[:], 1)[0]
		byID[owner].store.Put(blockstore.Block{ID: id, Payload: []byte{1}, CostUS: 100})
	}

	// Two accesses push demand over the threshold of 2.
	ev := &pmv1.AccessEvent{ModelId: "m", ChainHead: ids[0][:], DepthRequested: 3, DepthMatched: 3}
	for _, id := range ids {
		ev.Chain = append(ev.Chain, id[:])
	}
	raw, _ := proto.Marshal(ev)
	for range 2 {
		bus.Produce(events.TopicAccess, ev.ChainHead, raw)
	}
	time.Sleep(100 * time.Millisecond) // let the bus consumer apply demand

	// Epoch 2: cn-3 joins; some blocks' ownership moves to it.
	p.OnRing(ringOf(2, n1, n2, n3))
	ring2 := testRing(2, n1, n2, n3)
	var moved []chain.BlockID
	for _, id := range ids {
		if ring2.Owners(id[:], 1)[0] == "cn-3" {
			moved = append(moved, id)
		}
	}
	if len(moved) == 0 {
		t.Skip("hash layout moved no test blocks to cn-3; enlarge the chain if this ever happens")
	}

	// The warm plan must land every moved block on cn-3 without any client
	// traffic.
	deadline := time.Now().Add(3 * time.Second)
	for {
		warmed := 0
		for _, id := range moved {
			if n3.store.Contains(id) {
				warmed++
			}
		}
		if warmed == len(moved) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d moved blocks warmed onto cn-3", warmed, len(moved))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// testRing mirrors the prefetcher's internal ring construction (same vnode
// count as ringOf) so the test can compute expected ownership.
func testRing(epoch uint64, nodes ...*node) *hashring.Ring {
	m := map[string]string{}
	for _, n := range nodes {
		m[n.id] = n.addr
	}
	return hashring.New(epoch, m, 64)
}
