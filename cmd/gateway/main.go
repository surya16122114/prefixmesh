// Gateway: stateless data-plane entrypoint (DESIGN.md §4.1).
// With --directory the ring comes from WatchRing; --nodes keeps the M0
// static mode for directory-less dev runs.
package main

import (
	"context"
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
	"github.com/surya16122114/prefixmesh/internal/ringwatch"
)

// parseNodes parses "id=host:port,id=host:port".
func parseNodes(s string) (map[string]string, bool) {
	nodes := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, false
		}
		nodes[id] = addr
	}
	return nodes, true
}

func staticSource(nodesFlag string) (gateway.RingSource, bool) {
	nodes, ok := parseNodes(nodesFlag)
	if !ok {
		return nil, false
	}
	clients := make(map[string]pmv1.CacheNodeServiceClient, len(nodes))
	for id, addr := range nodes {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("dial failed", "node", id, "addr", addr, "err", err)
			return nil, false
		}
		clients[id] = pmv1.NewCacheNodeServiceClient(conn)
	}
	return &gateway.Static{R: hashring.New(1, nodes, 0), Clients: clients}, true
}

func main() {
	listen := flag.String("listen", ":7000", "gRPC listen address")
	nodesFlag := flag.String("nodes", "", `static ring: "cn-1=host:7100,cn-2=host:7101" (M0 mode)`)
	dirs := flag.String("directory", "", "comma-separated directory replica addrs")
	flag.Parse()

	var src gateway.RingSource
	switch {
	case *dirs != "":
		w := ringwatch.New(strings.Split(*dirs, ","), true, nil)
		go w.Run(context.Background())
		src = w
	case *nodesFlag != "":
		s, ok := staticSource(*nodesFlag)
		if !ok {
			os.Exit(1)
		}
		src = s
	default:
		slog.Error("one of --directory or --nodes is required")
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen failed", "addr", *listen, "err", err)
		os.Exit(1)
	}
	s := grpc.NewServer()
	pmv1.RegisterGatewayServiceServer(s, gateway.New(src))
	healthpb.RegisterHealthServer(s, health.NewServer())

	slog.Info("gateway listening", "addr", *listen,
		"mode", map[bool]string{true: "directory", false: "static"}[*dirs != ""])
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
