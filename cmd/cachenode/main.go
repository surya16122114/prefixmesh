// Cache node: stateful data-plane block server (DESIGN.md §4.2).
// With --directory it joins the cluster and enforces ring epochs; without,
// it serves statically (M0 mode).
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/blockstore"
	"github.com/surya16122114/prefixmesh/internal/cachenode"
)

func main() {
	listen := flag.String("listen", ":7100", "gRPC listen address")
	advertise := flag.String("advertise", "", "address other services reach us at (default: listen addr)")
	nodeID := flag.String("node-id", "", "unique node id (required)")
	capacity := flag.Uint64("capacity-bytes", 1<<30, "block store capacity")
	dirs := flag.String("directory", "", "comma-separated directory replica addrs (empty = static M0 mode)")
	flag.Parse()
	if *nodeID == "" {
		slog.Error("--node-id is required")
		os.Exit(1)
	}
	if *advertise == "" {
		*advertise = *listen
	}

	store := blockstore.NewLRU(*capacity)
	srv := cachenode.New(store)

	if *dirs != "" {
		agent := cachenode.NewAgent(&pmv1.NodeInfo{
			NodeId:        *nodeID,
			Addr:          *advertise,
			CapacityBytes: *capacity,
		}, strings.Split(*dirs, ","), store)
		srv = srv.WithEpoch(agent.Epoch)
		go agent.Run(context.Background())
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen failed", "addr", *listen, "err", err)
		os.Exit(1)
	}
	s := grpc.NewServer()
	pmv1.RegisterCacheNodeServiceServer(s, srv)
	healthpb.RegisterHealthServer(s, health.NewServer())

	slog.Info("cache node listening",
		"node_id", *nodeID, "addr", *listen, "capacity_bytes", *capacity,
		"mode", map[bool]string{true: "directory", false: "static"}[*dirs != ""])
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
