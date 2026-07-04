// Gateway: stateless data-plane entrypoint (DESIGN.md §4.1).
//
// M0 uses a static ring from --nodes; M1 replaces it with a
// DirectoryService.WatchRing subscription.
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/gateway"
	"github.com/surya16122114/prefixmesh/internal/hashring"
)

// parseNodes parses "id=host:port,id=host:port".
func parseNodes(s string) (map[string]string, error) {
	nodes := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, os.ErrInvalid
		}
		nodes[id] = addr
	}
	return nodes, nil
}

func main() {
	listen := flag.String("listen", ":7000", "gRPC listen address")
	nodesFlag := flag.String("nodes", "", `static ring membership: "cn-1=host:7100,cn-2=host:7101" (required until M1)`)
	flag.Parse()
	if *nodesFlag == "" {
		slog.Error("--nodes is required (static ring; directory watch lands in M1)")
		os.Exit(1)
	}
	nodes, err := parseNodes(*nodesFlag)
	if err != nil {
		slog.Error("bad --nodes", "value", *nodesFlag)
		os.Exit(1)
	}

	clients := make(map[string]pmv1.CacheNodeServiceClient, len(nodes))
	for id, addr := range nodes {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("dial failed", "node", id, "addr", addr, "err", err)
			os.Exit(1)
		}
		clients[id] = pmv1.NewCacheNodeServiceClient(conn)
	}
	ring := hashring.New(1, nodes, 0)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen failed", "addr", *listen, "err", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	pmv1.RegisterGatewayServiceServer(s, gateway.New(ring, clients))
	healthpb.RegisterHealthServer(s, health.NewServer())

	slog.Info("gateway listening", "addr", *listen, "ring_nodes", ring.Size())
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
