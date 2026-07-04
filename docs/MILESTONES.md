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

## M1 — Control plane: Paxos directory ✅ (done 2026-07-04)
- [x] Multi-decree Paxos log (port DKV `PaxosCoordinator` design to Go) — unit-tested
      with a simulated lossy/reordering network harness (15% loss, concurrent
      proposers; safety = identical applied sequences on every replica)
- [x] Membership + heartbeats + `NodeDead` via consensus commit
      (per-replica suspicion in M1 — a false positive self-heals by rejoin;
      true quorum suspicion-exchange moved to M2)
- [x] Epoch-numbered ring, `WatchRing` streams, `WRONG_EPOCH` retry protocol
- [x] Join/leave rebalancing via epoch bumps + lazy refill
      (grace-period **leases moved to M2** — they belong with replication,
      where "old owner still serves reads" has data to serve)
- [x] **Demo (measured):** kill a cache node mid-load → `NodeDead` committed and
      epoch bumped in ~2 s → next run back at **85.9%** hit rate (M0's static
      ring stayed collapsed at 7.7% forever). Live join → epoch bump, zero
      disruption. Directory minority kill → commits keep flowing
      (`internal/directory/integration_test.go`).

## M2 — Cache quality ✅ (done 2026-07-04)
- [x] Paged arena block store: preallocated page arena, exact page-level
      occupancy, fragmentation impossible by construction; LRU and cost
      policies share the arena so A/B runs compare at truly equal memory
- [x] Cost-aware eviction (decayed-frecency × cost / bytes, sampled
      approximated-min à la Redis) behind `--eviction cost|lru`
- [x] RF=2 replication (`--replication` on the gateway): every block on its
      ring owner + successor; Match retries misses at the next replica
- [x] Quorum suspicion-exchange (from M1): a replica proposes NodeDead only
      after a majority of replicas confirm they also lost the node's
      heartbeats — one replica's blip can't evict a healthy node
- [x] Grace-period leases: **cut, deliberately** — RF=2 subsumes them. A
      membership change shifts at most one member of an owner pair
      (hashring test: <1% of keys lose both), so the surviving replica
      serves through rebalances with no lease machinery at all.
- **Measured (4 nodes × 8 MB, uniform doc popularity, 20% of docs 10× cost,
  seeds disclosed):** cost-aware saves **50.5%** of prefill compute vs LRU's
  **46.1%** at equal memory — while accepting a lower raw hit rate (62.9% vs
  64.5%): it evicts by value, not recency. Under cache-plenty (zipf 1.2) the
  policies tie, as they should — eviction policy only matters under pressure.
- **Measured resilience:** with ALL 3 directory replicas killed (ring frozen,
  no epoch bump possible) and then a cache node killed, RF=2 held the hit
  rate at **86.0%** (vs 85.8% healthy) — replication and consensus are
  independent failure-recovery layers.

## M3 — Event plane: Kafka + prefetcher ✅ (done 2026-07-04)
- [x] `prefix.access.v1` producer in gateway (franz-go, acks=0 fire-and-forget;
      events carry the full chain so the prefetcher sees blocks + edges)
- [x] Prefetcher service: time-decayed per-block demand over the observed
      chain structure; on every ring-epoch change it warms demanded blocks
      onto their NEW owners, sourced from surviving old owners — one
      mechanism covers join re-warming and post-death RF=2 restoration
- [x] Cache nodes consume `cache.warm.v1` (token-bucket rate-limited,
      idempotent by content address, deadline-dropped when stale)
- [x] Transport seam (`internal/events`): Kafka in production, in-memory Bus
      in tests — the full warm loop runs in CI without a broker
- [x] **Demo (measured, real Kafka in Docker):** double node-kill with an
      idle window between kills. Without the event plane: 83.5% hit rate /
      80.3% prefill saved (blocks with both replicas on the dead pair are
      lost). With the prefetcher: **89.8% / 92.1%** — back at the workload's
      ~92% ceiling, warmed ahead of demand.
- [x] **Chaos check:** `docker kill` the broker mid-mesh → hot path unaffected
      (latency actually improved; the plane is lossy by contract).
- Found & fixed en route: loadgen derived corpus content from `--seed`, so
  cross-run cache continuity was unmeasurable — split into `--workload-seed`
  (corpus identity, fixed) and `--seed` (request stream).

## M4 — Benchmarks, chaos, observability ✅ (done 2026-07-04)
- [x] Prometheus `/metrics` on every service (`--metrics`, default :9100):
      gateway match histogram + hit counters, cache-node store/warmer
      collectors, directory epoch/members/log gauges
- [x] Grafana + Prometheus in compose, provisioned datasource + the one
      dashboard (hit rate, p50/p99, epoch, occupancy, evictions, warms)
- [x] `cache.telemetry.v1` NodeTelemetry producer (5 s deltas) — the feed a
      future eviction learner trains on
- [x] Reproducible benchmark suite: `make bench` runs all four scenarios
      (steady+kill, frozen-ring failover, eviction A/B, prefetcher A/B with
      auto-started Kafka) with cleanup verification and bind checks baked in —
      every pitfall that produced a garbage number during development is now
      an automated guard
- [x] Results table in BENCHMARKS.md §0, regenerated from one scripted run

## M5 — Stretch (pick at most one) — NOT BUILT, deliberately
Both options were costed and consciously deferred rather than half-shipped:
- (a) Real-model end-to-end (llama.cpp/vLLM external prefix store) needs
      inference-server surgery to export/import KV state — a project of its
      own; the simulation already proves the distributed-systems claims.
- (b) Eviction learner: the training feed (`cache.telemetry.v1`) now exists;
      the honest next step is collecting replay corpora before fitting
      anything. Backtest harness sketched in BENCHMARKS.md §3.

## Explicitly cut
- Multi-region, durability, auth, cross-model sharing, real GPU memory management.
