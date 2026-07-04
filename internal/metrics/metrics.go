// Package metrics serves Prometheus /metrics (DESIGN.md §7). Each service
// calls Serve with its --metrics address; collectors live with the code they
// observe.
package metrics

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Serve exposes /metrics on addr in a goroutine. An empty addr disables
// metrics; a bind failure is logged, not fatal — observability must never
// take a service down.
func Serve(addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Warn("metrics: serve failed", "addr", addr, "err", err)
		}
	}()
}
