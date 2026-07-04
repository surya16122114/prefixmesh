package cachenode

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/blockstore"
	"github.com/surya16122114/prefixmesh/internal/events"
)

// StoreCollector exports blockstore.Stats as Prometheus metrics. A custom
// collector (rather than promauto counters inside the store) keeps the store
// dependency-free and lets tests build stores without touching the default
// registry.
type StoreCollector struct {
	store blockstore.Store

	occupancy   *prometheus.Desc
	capacity    *prometheus.Desc
	hits        *prometheus.Desc
	misses      *prometheus.Desc
	evictions   *prometheus.Desc
	evictedCost *prometheus.Desc
}

func NewStoreCollector(store blockstore.Store) *StoreCollector {
	return &StoreCollector{
		store:       store,
		occupancy:   prometheus.NewDesc("pm_cachenode_occupancy_bytes", "Arena bytes in use.", nil, nil),
		capacity:    prometheus.NewDesc("pm_cachenode_capacity_bytes", "Arena capacity.", nil, nil),
		hits:        prometheus.NewDesc("pm_cachenode_hits_total", "Store lookups that hit.", nil, nil),
		misses:      prometheus.NewDesc("pm_cachenode_misses_total", "Store lookups that missed.", nil, nil),
		evictions:   prometheus.NewDesc("pm_cachenode_evictions_total", "Blocks evicted.", nil, nil),
		evictedCost: prometheus.NewDesc("pm_cachenode_evicted_cost_us_total", "Prefill cost evicted, µs — eviction-quality signal.", nil, nil),
	}
}

func (c *StoreCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.occupancy
	ch <- c.capacity
	ch <- c.hits
	ch <- c.misses
	ch <- c.evictions
	ch <- c.evictedCost
}

func (c *StoreCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.store.Stats()
	ch <- prometheus.MustNewConstMetric(c.occupancy, prometheus.GaugeValue, float64(st.OccupancyBytes))
	ch <- prometheus.MustNewConstMetric(c.capacity, prometheus.GaugeValue, float64(st.CapacityBytes))
	ch <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(st.Hits))
	ch <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(st.Misses))
	ch <- prometheus.MustNewConstMetric(c.evictions, prometheus.CounterValue, float64(st.Evictions))
	ch <- prometheus.MustNewConstMetric(c.evictedCost, prometheus.CounterValue, float64(st.EvictedCostUS))
}

// WarmerCollector exports warm-execution counters.
type WarmerCollector struct {
	w        *Warmer
	executed *prometheus.Desc
	skipped  *prometheus.Desc
	dropped  *prometheus.Desc
}

func NewWarmerCollector(w *Warmer) *WarmerCollector {
	return &WarmerCollector{
		w:        w,
		executed: prometheus.NewDesc("pm_cachenode_warms_executed_total", "Warm commands executed (block fetched+stored).", nil, nil),
		skipped:  prometheus.NewDesc("pm_cachenode_warms_skipped_present_total", "Warm commands skipped: block already present.", nil, nil),
		dropped:  prometheus.NewDesc("pm_cachenode_warms_dropped_total", "Warm commands dropped: stale deadline or over rate budget.", nil, nil),
	}
}

func (c *WarmerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.executed
	ch <- c.skipped
	ch <- c.dropped
}

func (c *WarmerCollector) Collect(ch chan<- prometheus.Metric) {
	e, s, d := c.w.Counters()
	ch <- prometheus.MustNewConstMetric(c.executed, prometheus.CounterValue, float64(e))
	ch <- prometheus.MustNewConstMetric(c.skipped, prometheus.CounterValue, float64(s))
	ch <- prometheus.MustNewConstMetric(c.dropped, prometheus.CounterValue, float64(d))
}

// RunTelemetry emits NodeTelemetry deltas on cache.telemetry.v1 every
// interval (DESIGN.md §4.4) — the feed a future eviction learner trains on.
func RunTelemetry(ctx context.Context, producer events.Producer, nodeID string,
	store blockstore.Store, interval time.Duration) {

	t := time.NewTicker(interval)
	defer t.Stop()
	var prev blockstore.Stats
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		st := store.Stats()
		ev := &pmv1.NodeTelemetry{
			NodeId:         nodeID,
			OccupancyBytes: st.OccupancyBytes,
			CapacityBytes:  st.CapacityBytes,
			Hits:           st.Hits - prev.Hits,
			Misses:         st.Misses - prev.Misses,
			Evictions:      st.Evictions - prev.Evictions,
			EvictedCostUs:  st.EvictedCostUS - prev.EvictedCostUS,
			TsUnixMs:       time.Now().UnixMilli(),
		}
		prev = st
		raw, err := proto.Marshal(ev)
		if err != nil {
			continue
		}
		producer.Produce(events.TopicTelemetry, []byte(nodeID), raw)
	}
}
