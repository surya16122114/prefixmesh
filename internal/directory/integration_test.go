package directory_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/blockstore"
	"github.com/surya16122114/prefixmesh/internal/cachenode"
	"github.com/surya16122114/prefixmesh/internal/chain"
	"github.com/surya16122114/prefixmesh/internal/directory"
	"github.com/surya16122114/prefixmesh/internal/gateway"
	"github.com/surya16122114/prefixmesh/internal/ringwatch"
)

const (
	hbEvery   = 50 * time.Millisecond
	deadAfter = 250 * time.Millisecond
)

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// startDirectoryCluster brings up n Paxos replicas on real gRPC. The
// returned stops kill individual replicas (parallel to the addr slice).
func startDirectoryCluster(t *testing.T, ctx context.Context, n int) (addrsOut []string, stops []func()) {
	t.Helper()
	addrs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		addrs[fmt.Sprintf("dir-%d", i)] = freeAddr(t)
	}
	for id, addr := range addrs {
		peers := map[string]string{}
		for pid, paddr := range addrs {
			if pid != id {
				peers[pid] = paddr
			}
		}
		srv, err := directory.New(directory.Config{
			ReplicaID:      id,
			Peers:          peers,
			HeartbeatEvery: hbEvery,
			DeadAfter:      deadAfter,
		})
		if err != nil {
			t.Fatal(err)
		}
		srv.Run(ctx)
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		g := grpc.NewServer()
		pmv1.RegisterDirectoryServiceServer(g, srv)
		go g.Serve(lis)
		t.Cleanup(g.Stop)
		addrsOut = append(addrsOut, addr)
		stops = append(stops, g.Stop)
	}
	return addrsOut, stops
}

type liveCacheNode struct {
	id     string
	addr   string
	stop   func()
	cancel context.CancelFunc // stops agent (heartbeats) too
}

func startCacheNodeM1(t *testing.T, ctx context.Context, id string, dirs []string) *liveCacheNode {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	store := blockstore.NewLRU(1 << 28)
	agentCtx, cancel := context.WithCancel(ctx)
	agent := cachenode.NewAgent(&pmv1.NodeInfo{
		NodeId: id, Addr: lis.Addr().String(), CapacityBytes: 1 << 28,
	}, dirs, store)
	agent.Every = hbEvery
	go agent.Run(agentCtx)

	g := grpc.NewServer()
	pmv1.RegisterCacheNodeServiceServer(g, cachenode.New(store).WithEpoch(agent.Epoch))
	go g.Serve(lis)
	n := &liveCacheNode{id: id, addr: lis.Addr().String(), stop: g.Stop, cancel: cancel}
	t.Cleanup(func() { cancel(); g.Stop() })
	return n
}

func startGatewayM1(t *testing.T, ctx context.Context, dirs []string) pmv1.GatewayServiceClient {
	t.Helper()
	w := ringwatch.New(dirs, true, nil)
	go w.Run(ctx)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	pmv1.RegisterGatewayServiceServer(g, gateway.New(w))
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pmv1.NewGatewayServiceClient(conn)
}

func getEpoch(t *testing.T, dir string) (uint64, int) {
	t.Helper()
	conn, err := grpc.NewClient(dir, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ring, err := pmv1.NewDirectoryServiceClient(conn).GetRing(context.Background(), &pmv1.GetRingRequest{})
	if err != nil {
		return 0, -1 // replica briefly unreachable is fine for polling
	}
	return ring.Epoch, len(ring.Nodes)
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func putChain(t *testing.T, client pmv1.GatewayServiceClient, ids []chain.BlockID, tokens []uint32, blockSize int) {
	t.Helper()
	up, err := client.PutBlocks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	parent := chain.Root("m", blockSize)
	for i, blk := range chain.Chunk(tokens, blockSize) {
		if err := up.Send(&pmv1.PutBlocksRequest{Block: &pmv1.Block{
			BlockId:    ids[i][:],
			ParentId:   parent[:],
			ModelId:    "m",
			Payload:    []byte{1},
			TokenCount: uint32(len(blk)),
			CostUs:     100,
		}}); err != nil {
			t.Fatal(err)
		}
		parent = ids[i]
	}
	if _, err := up.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
}

// TestKillRecoverRejoin is M1's demo as a test:
//  1. cluster forms (join via Paxos), full-chain hits work
//  2. a cache node dies -> failure detector commits NodeDead -> epoch bumps
//  3. Match keeps answering (misses, zero errors) on the new ring
//  4. refill -> full hits again on the survivor
//  5. a new node joins -> epoch bumps again
func TestKillRecoverRejoin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dirs, _ := startDirectoryCluster(t, ctx, 3)
	startCacheNodeM1(t, ctx, "cn-1", dirs)
	n2 := startCacheNodeM1(t, ctx, "cn-2", dirs)
	client := startGatewayM1(t, ctx, dirs)

	waitFor(t, "both nodes in ring", 5*time.Second, func() bool {
		_, members := getEpoch(t, dirs[0])
		return members == 2
	})
	epochBefore, _ := getEpoch(t, dirs[0])

	const blockSize = 4
	tokens := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	ids := chain.Build("m", blockSize, tokens)
	raw := make([][]byte, len(ids))
	for i := range ids {
		raw[i] = ids[i][:]
	}

	// Gateway needs its first ring update before blocks route anywhere.
	waitFor(t, "gateway sees the ring", 5*time.Second, func() bool {
		putChain(t, client, ids, tokens, blockSize)
		resp, err := client.Match(ctx, &pmv1.MatchRequest{ModelId: "m", Chain: raw})
		return err == nil && int(resp.MatchedDepth) == len(ids)
	})

	// Kill cn-2: server down AND heartbeats stopped.
	n2.cancel()
	n2.stop()

	waitFor(t, "failure detector removes cn-2", 5*time.Second, func() bool {
		_, members := getEpoch(t, dirs[0])
		return members == 1
	})
	epochAfterKill, _ := getEpoch(t, dirs[0])
	if epochAfterKill <= epochBefore {
		t.Fatalf("epoch did not advance on node death: %d -> %d", epochBefore, epochAfterKill)
	}

	// Match must keep working — misses allowed, errors not.
	resp, err := client.Match(ctx, &pmv1.MatchRequest{ModelId: "m", Chain: raw})
	if err != nil {
		t.Fatalf("Match errored after node death: %v", err)
	}

	// Refill on the new ring; hits must fully recover (everything now owned
	// by the survivor).
	waitFor(t, "hit rate recovers after refill", 5*time.Second, func() bool {
		putChain(t, client, ids, tokens, blockSize)
		resp, err = client.Match(ctx, &pmv1.MatchRequest{ModelId: "m", Chain: raw})
		return err == nil && int(resp.MatchedDepth) == len(ids)
	})

	// Join a fresh node; epoch bumps again and the mesh keeps serving.
	startCacheNodeM1(t, ctx, "cn-3", dirs)
	waitFor(t, "cn-3 joins", 5*time.Second, func() bool {
		_, members := getEpoch(t, dirs[0])
		return members == 2
	})
	epochAfterJoin, _ := getEpoch(t, dirs[0])
	if epochAfterJoin <= epochAfterKill {
		t.Fatalf("epoch did not advance on join: %d -> %d", epochAfterKill, epochAfterJoin)
	}
	if _, err := client.Match(ctx, &pmv1.MatchRequest{ModelId: "m", Chain: raw}); err != nil {
		t.Fatalf("Match errored after join: %v", err)
	}
}

// TestDirectoryMinorityDown: with 1 of 3 replicas killed, the control plane
// keeps committing (quorum holds).
func TestDirectoryMinorityDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dirs, stops := startDirectoryCluster(t, ctx, 3)
	startCacheNodeM1(t, ctx, "cn-1", dirs)
	waitFor(t, "cn-1 joins", 5*time.Second, func() bool {
		_, members := getEpoch(t, dirs[0])
		return members == 1
	})

	stops[2]() // kill one replica; 2 of 3 remain

	conn, err := grpc.NewClient(dirs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cctx, ccancel := context.WithTimeout(ctx, 10*time.Second)
	defer ccancel()
	if _, err := pmv1.NewDirectoryServiceClient(conn).Join(cctx, &pmv1.JoinRequest{
		Node: &pmv1.NodeInfo{NodeId: "cn-x", Addr: "127.0.0.1:1"},
	}); err != nil {
		t.Fatalf("join with a minority replica down failed: %v", err)
	}
	waitFor(t, "epoch reflects the join on a survivor", 5*time.Second, func() bool {
		_, members := getEpoch(t, dirs[1])
		return members == 2
	})
}
