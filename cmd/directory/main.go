// Directory: Paxos control-plane replica (DESIGN.md §4.3).
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/directory"
	"github.com/surya16122114/prefixmesh/internal/metrics"
)

// parsePeers parses "dir-2=host:7200,dir-3=host:7200".
func parsePeers(s string) (map[string]string, bool) {
	peers := map[string]string{}
	if s == "" {
		return peers, true // single-replica dev cluster
	}
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, false
		}
		peers[id] = addr
	}
	return peers, true
}

func main() {
	listen := flag.String("listen", ":7200", "gRPC listen address")
	replicaID := flag.String("replica-id", "", "unique replica id (required)")
	peersFlag := flag.String("peers", "", `other replicas: "dir-2=host:7200,dir-3=host:7200"`)
	metricsAddr := flag.String("metrics", ":9100", "Prometheus /metrics address (empty = disabled)")
	flag.Parse()
	metrics.Serve(*metricsAddr)
	if *replicaID == "" {
		slog.Error("--replica-id is required")
		os.Exit(1)
	}
	peers, ok := parsePeers(*peersFlag)
	if !ok {
		slog.Error("bad --peers", "value", *peersFlag)
		os.Exit(1)
	}

	srv, err := directory.New(directory.Config{
		ReplicaID: *replicaID,
		Peers:     peers,
	})
	if err != nil {
		slog.Error("directory init failed", "err", err)
		os.Exit(1)
	}
	srv.Run(context.Background())
	prometheus.MustRegister(directory.NewCollector(srv))

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen failed", "addr", *listen, "err", err)
		os.Exit(1)
	}
	s := grpc.NewServer()
	pmv1.RegisterDirectoryServiceServer(s, srv)
	healthpb.RegisterHealthServer(s, health.NewServer())

	slog.Info("directory replica listening",
		"replica_id", *replicaID, "addr", *listen, "peers", len(peers))
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
