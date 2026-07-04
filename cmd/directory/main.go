// Directory: Paxos control-plane replica (DESIGN.md §4.3). M1 work.
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
)

type server struct {
	pmv1.UnimplementedDirectoryServiceServer
}

func main() {
	listen := flag.String("listen", ":7200", "gRPC listen address")
	replicaID := flag.String("replica-id", "", "unique replica id (required)")
	peers := flag.String("peers", "", "comma-separated peer replica addrs")
	flag.Parse()
	if *replicaID == "" {
		slog.Error("--replica-id is required")
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen failed", "addr", *listen, "err", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	pmv1.RegisterDirectoryServiceServer(s, &server{})
	healthpb.RegisterHealthServer(s, health.NewServer())

	slog.Info("directory replica listening",
		"replica_id", *replicaID, "addr", *listen,
		"peers", strings.Split(*peers, ","))
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
