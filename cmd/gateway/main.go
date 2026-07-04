// Gateway: stateless data-plane entrypoint (DESIGN.md §4.1).
// M0 wires up the server skeleton; Match/PutBlocks land with the static-ring
// milestone.
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
	pmv1.UnimplementedGatewayServiceServer
}

func main() {
	listen := flag.String("listen", ":7000", "gRPC listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen failed", "addr", *listen, "err", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	pmv1.RegisterGatewayServiceServer(s, &server{})
	healthpb.RegisterHealthServer(s, health.NewServer())

	slog.Info("gateway listening", "addr", *listen)
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
