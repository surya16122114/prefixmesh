package gateway_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/blockstore"
	"github.com/surya16122114/prefixmesh/internal/cachenode"
	"github.com/surya16122114/prefixmesh/internal/chain"
	"github.com/surya16122114/prefixmesh/internal/gateway"
	"github.com/surya16122114/prefixmesh/internal/hashring"
)

// startCacheNode runs a real cache node on an ephemeral port.
func startCacheNode(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pmv1.RegisterCacheNodeServiceServer(s, cachenode.New(blockstore.NewLRU(1<<28)))
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func startGateway(t *testing.T, nodes map[string]string) pmv1.GatewayServiceClient {
	t.Helper()
	clients := map[string]pmv1.CacheNodeServiceClient{}
	for id, addr := range nodes {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		clients[id] = pmv1.NewCacheNodeServiceClient(conn)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pmv1.RegisterGatewayServiceServer(s, gateway.New(&gateway.Static{
		R:       hashring.New(1, nodes, 0),
		Clients: clients,
	}))
	go s.Serve(lis)
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pmv1.NewGatewayServiceClient(conn)
}

func rawChain(ids []chain.BlockID) [][]byte {
	raw := make([][]byte, len(ids))
	for i := range ids {
		raw[i] = ids[i][:]
	}
	return raw
}

// TestSharedPrefixHitsAcrossRequests is the M0 demo as a test: request B
// shares request A's system prompt and must hit exactly those blocks.
func TestSharedPrefixHitsAcrossRequests(t *testing.T) {
	nodes := map[string]string{
		"cn-1": startCacheNode(t),
		"cn-2": startCacheNode(t),
	}
	client := startGateway(t, nodes)
	ctx := context.Background()

	const blockSize = 4
	system := []uint32{1, 2, 3, 4, 5, 6, 7, 8} // 2 shared blocks
	promptA := append(append([]uint32{}, system...), 100, 101, 102, 103)
	promptB := append(append([]uint32{}, system...), 200, 201, 202, 203)

	chainA := chain.Build("m", blockSize, promptA)

	// Cold Match: everything misses.
	resp, err := client.Match(ctx, &pmv1.MatchRequest{ModelId: "m", Chain: rawChain(chainA)})
	if err != nil {
		t.Fatal(err)
	}
	if resp.MatchedDepth != 0 {
		t.Fatalf("cold cache matched %d blocks", resp.MatchedDepth)
	}

	// Write back A's blocks (what an inference client does after prefill).
	up, err := client.PutBlocks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	parent := chain.Root("m", blockSize)
	for i, blk := range chain.Chunk(promptA, blockSize) {
		if err := up.Send(&pmv1.PutBlocksRequest{Block: &pmv1.Block{
			BlockId:    chainA[i][:],
			ParentId:   parent[:],
			ModelId:    "m",
			Payload:    []byte{0xAA},
			TokenCount: uint32(len(blk)),
			CostUs:     100,
		}}); err != nil {
			t.Fatal(err)
		}
		parent = chainA[i]
	}
	putResp, err := up.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if putResp.Stored != 3 {
		t.Fatalf("stored %d blocks, want 3", putResp.Stored)
	}

	// A again: full-chain hit.
	resp, err = client.Match(ctx, &pmv1.MatchRequest{ModelId: "m", Chain: rawChain(chainA)})
	if err != nil {
		t.Fatal(err)
	}
	if int(resp.MatchedDepth) != len(chainA) {
		t.Fatalf("repeat of A matched %d/%d", resp.MatchedDepth, len(chainA))
	}
	if len(resp.Refs) != len(chainA) {
		t.Fatalf("got %d refs, want %d", len(resp.Refs), len(chainA))
	}
	for _, ref := range resp.Refs {
		if ref.NodeAddr != nodes["cn-1"] && ref.NodeAddr != nodes["cn-2"] {
			t.Fatalf("ref points at unknown node %q", ref.NodeAddr)
		}
	}

	// B shares exactly the 2 system-prompt blocks.
	chainB := chain.Build("m", blockSize, promptB)
	resp, err = client.Match(ctx, &pmv1.MatchRequest{ModelId: "m", Chain: rawChain(chainB)})
	if err != nil {
		t.Fatal(err)
	}
	if resp.MatchedDepth != 2 {
		t.Fatalf("B matched %d blocks, want the 2 shared system blocks", resp.MatchedDepth)
	}

	// Re-put of A must be all no-ops (idempotent by content address).
	up, err = client.PutBlocks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	parent = chain.Root("m", blockSize)
	for i, blk := range chain.Chunk(promptA, blockSize) {
		_ = blk
		if err := up.Send(&pmv1.PutBlocksRequest{Block: &pmv1.Block{
			BlockId:  chainA[i][:],
			ParentId: parent[:],
			ModelId:  "m",
			Payload:  []byte{0xAA},
		}}); err != nil {
			t.Fatal(err)
		}
		parent = chainA[i]
	}
	putResp, err = up.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if putResp.Stored != 0 || putResp.Existing != 3 {
		t.Fatalf("re-put: stored=%d existing=%d, want 0/3", putResp.Stored, putResp.Existing)
	}
}

// TestNodeDownDegradesToMisses: killing a cache node must turn its blocks
// into misses, never into errors (DESIGN.md §5).
func TestNodeDownDegradesToMisses(t *testing.T) {
	addr1 := startCacheNode(t)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := grpc.NewServer()
	pmv1.RegisterCacheNodeServiceServer(dead, cachenode.New(blockstore.NewLRU(1<<20)))
	go dead.Serve(lis)

	nodes := map[string]string{"cn-1": addr1, "cn-2": lis.Addr().String()}
	client := startGateway(t, nodes)
	ctx := context.Background()

	dead.Stop() // kill cn-2 before any traffic

	ids := chain.Build("m", 4, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	resp, err := client.Match(ctx, &pmv1.MatchRequest{ModelId: "m", Chain: rawChain(ids)})
	if err != nil {
		t.Fatalf("Match must not error when a node is down, got: %v", err)
	}
	if resp.MatchedDepth != 0 {
		t.Fatalf("matched %d on an empty/half-dead mesh", resp.MatchedDepth)
	}
}
