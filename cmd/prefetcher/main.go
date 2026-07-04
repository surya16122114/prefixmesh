// Prefetcher: consumes prefix.access.v1, tracks per-block demand and chain
// structure, and emits cache.warm.v1 when ring changes leave demanded blocks
// under-placed (DESIGN.md §4.4).
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/surya16122114/prefixmesh/internal/events"
	"github.com/surya16122114/prefixmesh/internal/prefetch"
	"github.com/surya16122114/prefixmesh/internal/ringwatch"
)

func main() {
	kafka := flag.String("kafka", "", "comma-separated Kafka brokers (required)")
	dirs := flag.String("directory", "", "comma-separated directory replica addrs (required)")
	rf := flag.Int("replication", 2, "mesh replication factor (must match the gateway)")
	threshold := flag.Float64("demand-threshold", 2, "min decayed demand before a block is warm-worthy")
	flag.Parse()
	if *kafka == "" || *dirs == "" {
		slog.Error("--kafka and --directory are required")
		os.Exit(1)
	}
	brokers := strings.Split(*kafka, ",")

	producer, err := events.NewKafkaProducer(brokers)
	if err != nil {
		slog.Error("kafka producer init failed", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	p := prefetch.New(prefetch.Config{
		RF:              *rf,
		DemandThreshold: *threshold,
	}, producer)

	consumer, err := events.NewKafkaConsumer(brokers, "prefetcher", events.TopicAccess, p.HandleAccess)
	if err != nil {
		slog.Error("kafka access consumer init failed", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	watcher := ringwatch.New(strings.Split(*dirs, ","), false, p.OnRing)
	go watcher.Run(ctx)

	slog.Info("prefetcher running", "brokers", brokers, "rf", *rf, "threshold", *threshold)
	consumer.Run(ctx)
}
