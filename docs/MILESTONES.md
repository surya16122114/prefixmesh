# PrefixMesh — Milestone Plan

Each milestone ends in something **runnable and demoable**. No milestone depends on a
stretch goal. Estimates assume evenings/weekends pace.

## M0 — Skeleton that answers a request ✅ (done 2026-07-04)
- [x] Repo scaffold, proto contracts, CI (build + vet + test)
- [x] Hash-chain library (`internal/chain`) with tests
- [x] Consistent hash ring (`internal/hashring`) with virtual nodes + tests
- [x] In-memory block store with plain LRU (`internal/blockstore`)
- [x] Gateway + cache nodes, **static ring from config** (no directory yet)
- [x] `loadgen` v0: synthetic workload (N system prompts, Zipfian RAG docs), prints
      hit rate + p50/p99
- [x] **Demo:** two prompts sharing a system prompt; second one hits
      (`internal/gateway/e2e_test.go` + live run: 85.8% block hit rate,
      p50 0.47 ms / p99 1.5 ms on 3 local nodes, 2000 req; killing a node
      produced zero errors — misses only).
- Measured M1 motivation: on the static ring, one dead node collapsed the hit
  rate to 7.7% (a dead owner anywhere in the prefix truncates the match, and
  membership can't change). Epoch bumps + rebalancing are what turn that into
  a dip-and-recover.

## M1 — Control plane: Paxos directory (≈2–3 weeks, the meaty one)
- [ ] Multi-decree Paxos log (port DKV `PaxosCoordinator` design to Go) — unit-tested
      with a simulated lossy/reordering network harness
- [ ] Membership + heartbeats + quorum-confirmed `NodeDead`
- [ ] Epoch-numbered ring, `WatchRing` streams, `WRONG_EPOCH` retry protocol
- [ ] Join/leave rebalancing with grace-period leases
- **Demo:** `docker compose up`, kill a cache node mid-load, watch epoch bump + hit
  rate recover; add a node, watch keyspace shift without a miss storm.

## M2 — Cache quality (≈1 week)
- [ ] Paged arena block store (preallocated pages, occupancy metric)
- [ ] Cost-aware eviction (frecency × cost / size) behind a flag vs plain-LRU baseline
- [ ] Optional RF=2 replication for hot blocks (immutability makes this cheap)
- **Demo:** benchmark showing cost-aware eviction beats LRU on prefill-µs-saved at
  equal memory.

## M3 — Event plane: Kafka + prefetcher (≈1–2 weeks)
- [ ] `prefix.access.v1` producer in gateway (fire-and-forget, buffered)
- [ ] Prefetcher service: Markov next-block model, emits `cache.warm.v1`
- [ ] Cache nodes consume warm commands (rate-limited, idempotent)
- **Demo:** A/B run of loadgen with prefetcher on/off; measured hit-rate lift.

## M4 — Benchmarks, chaos, observability (≈1 week)
- [ ] Prometheus + Grafana in compose, the one dashboard
- [ ] Reproducible benchmark suite (`make bench`) per BENCHMARKS.md — honest numbers
      with hardware + workload disclosed
- [ ] Chaos harness: scripted node kill / broker kill / directory-minority kill
- **Demo:** the README numbers, regenerable by anyone with `make bench`.

## M5 — Stretch (pick at most one)
- (a) Tiny real-model end-to-end: llama.cpp/vLLM node using PrefixMesh as external
      prefix store — proves the interface is real
- (b) Eviction learner: train eviction scorer from `cache.telemetry.v1` history,
      backtest against replay

## Explicitly cut
- Multi-region, durability, auth, cross-model sharing, real GPU memory management.
