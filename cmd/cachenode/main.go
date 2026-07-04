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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/blockstore"
	"github.com/surya16122114/prefixmesh/internal/cachenode"
	"github.com/surya16122114/prefixmesh/internal/events"
	"github.com/surya16122114/prefixmesh/internal/metrics"
)

func main() {
	listen := flag.String("listen", ":7100", "gRPC listen address")
	advertise := flag.String("advertise", "", "address other services reach us at (default: listen addr)")
	nodeID := flag.String("node-id", "", "unique node id (required)")
	capacity := flag.Uint64("capacity-bytes", 1<<30, "block store capacity")
	pageBytes := flag.Int("page-bytes", 4096, "arena page size")
	eviction := flag.String("eviction", "cost", "eviction policy: cost | lru (benchmark baseline)")
	dirs := flag.String("directory", "", "comma-separated directory replica addrs (empty = static M0 mode)")
	kafka := flag.String("kafka", "", "comma-separated Kafka brokers (empty = no warm consumer)")
	warmRate := flag.Float64("warm-rate", 200, "max warm fetches per second")
	metricsAddr := flag.String("metrics", ":9100", "Prometheus /metrics address (empty = disabled)")
	flag.Parse()
	metrics.Serve(*metricsAddr)
	if *nodeID == "" {
		slog.Error("--node-id is required")
		os.Exit(1)
	}
	if *advertise == "" {
		*advertise = *listen
	}
	policy, err := blockstore.ParsePolicy(*eviction)
	if err != nil {
		slog.Error("bad --eviction", "err", err)
		os.Exit(1)
	}

	store := blockstore.NewPaged(*capacity, *pageBytes, policy)
	prometheus.MustRegister(cachenode.NewStoreCollector(store))
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

	if *kafka != "" {
		warmer := cachenode.NewWarmer(*nodeID, store, *warmRate)
		prometheus.MustRegister(cachenode.NewWarmerCollector(warmer))
		// Group per node: every node sees every warm command and filters by
		// target — command volume is low and this avoids partition/target
		// alignment machinery.
		consumer, err := events.NewKafkaConsumer(strings.Split(*kafka, ","),
			"warm-"+*nodeID, events.TopicWarm, warmer.Handle)
		if err != nil {
			slog.Error("kafka warm consumer init failed", "err", err)
			os.Exit(1)
		}
		go consumer.Run(context.Background())

		producer, err := events.NewKafkaProducer(strings.Split(*kafka, ","))
		if err != nil {
			slog.Error("kafka telemetry producer init failed", "err", err)
			os.Exit(1)
		}
		go cachenode.RunTelemetry(context.Background(), producer, *nodeID, store, 5*time.Second)
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
		"eviction", string(policy),
		"mode", map[bool]string{true: "directory", false: "static"}[*dirs != ""])
	if err := s.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
