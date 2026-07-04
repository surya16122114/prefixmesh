// Cache node: stateful data-plane block server (DESIGN.md §4.2).
// M0 wires up the server skeleton; the LRU blockstore lands next.
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
)

type server struct {
	pmv1.UnimplementedCacheNodeServiceServer
}

func main() {
	listen := flag.String("listen", ":7100", "gRPC listen address")
	nodeID := flag.String("node-id", "", "unique node id (required)")
	capacity := flag.Uint64("capacity-bytes", 1<<30, "block store capacity")
	flag.Parse()
	if *nodeID == "" {
		slog.Error("--node-id is required")
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen failed", "addr", *listen, "err", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	pmv1.RegisterCacheNodeServiceServer(s, &server{})
	healthpb.RegisterHealthServer(s, health.NewServer())

	slog.Info("cache node listening",
		"node_id", *nodeID, "addr", *listen, "capacity_bytes", *capacity)
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
