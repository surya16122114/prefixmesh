// Loadgen: reproducible benchmark client (docs/BENCHMARKS.md §2).
// M0 delivers v0: synthetic workload (system prompts + Zipfian RAG docs +
// unique suffixes), seeded PRNG, prints hit rate and p50/p99.
package main

import "log/slog"

func main() {
	slog.Info("loadgen v0 is the last M0 item — see docs/MILESTONES.md")
}
