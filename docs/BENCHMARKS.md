# PrefixMesh — Benchmark & Demo Plan

Rule: **every published number is reproducible with `make bench` and states its
hardware, workload seed, and duration.** No synthetic vanity metrics.

## 0. Results (one `make bench` run, Apple M4, Go 1.26, 2026-07-04)

| Scenario | Result |
|---|---|
| Steady state (4 nodes, RF=2) | 85.8% hit rate / 87.0% prefill saved, p50 ~0.7 ms |
| Kill a cache node (epoch heals ~2 s) | 89.9% / 92.9% right after — no visible dip |
| Kill ALL 3 directory replicas + a node (frozen ring) | 89.9% / 92.9% — RF=2 failover alone carries it |
| Eviction at equal memory (4×8 MB, 20% docs 10× cost) | LRU **46.0%** vs cost-aware **58.4%** prefill saved (same 64.6% block hit rate — the policy trades hits for value; gap ranged 4–12 pts across runs, sampling variance) |
| Prefetcher A/B (double kill, idle window between) | off **83.3% / 80.2%** vs on **89.3% / 91.8%** — warming restores redundancy ahead of demand |
| Kafka broker killed mid-run | hot path unaffected |

Regenerate with `make bench` (writes `bench-results.md`; the Kafka scenario
needs Docker and is skipped loudly without it).

## 1. Metrics

| Metric | Definition |
|---|---|
| **Hit rate** | blocks served from cache / blocks requested |
| **Prefill compute saved** | Σ `cost_us` of hit blocks / Σ `cost_us` of all requested blocks — the headline number, in "% of prefill work avoided" |
| **Match latency p50/p99** | client-observed `Match` RPC latency |
| **Time-to-blocks p50/p99** | `Match` + streaming first byte of blocks |
| **Recovery time** | node kill → hit rate back within 5% of steady state |
| **Warm effectiveness** | warmed blocks that receive a hit within 60 s / warm commands issued |

## 2. Workload model (`loadgen`)

Synthetic but shaped like production RAG/agent traffic, seeded PRNG for reproducibility:
- K tenants, each with 1–3 **system prompts** (long shared prefixes, near-every request)
- Zipfian document pool per tenant (RAG chunks — mid-length shared prefixes)
- Random unique suffixes (user questions — always misses, by design)
- Configurable QPS, chain depth distribution, working-set-vs-cache-size ratio

## 3. Comparisons

1. **No cache** (baseline: every block recomputed)
2. **Per-node cache only** (simulates status quo: vLLM prefix cache without sharing) —
   same total memory, partitioned, no cross-node hits
3. **PrefixMesh** (shared mesh, same total memory)
4. PrefixMesh **± prefetcher** (M3), **cost-aware vs plain LRU** (M2)

The story is (3) vs (2) at equal memory: how much does *sharing* buy.

## 4. Chaos scenarios (M4)

| Scenario | Expected behavior |
|---|---|
| Kill 1 of M cache nodes mid-run | hit-rate dip ∝ 1/M, epoch bump < 2 s, full recovery, **zero errors** (misses only) |
| Kill Kafka broker | hot path unaffected; warming pauses; resumes on recovery |
| Kill 1 of 3 directory replicas | nothing observable (quorum holds) |
| Kill 2 of 3 directory replicas | data plane keeps serving on frozen ring; joins/leaves queue until quorum returns |
| Rolling restart of everything | no wrong answers at any point |

## 5. Demo script (for portfolio / interviews)

90-second recorded terminal demo:
1. `docker compose up` — mesh of 4 cache nodes + 3 directory + kafka
2. loadgen starts; Grafana shows hit rate climbing to steady state
3. `docker kill cachenode-2` — dip, epoch bump in logs, recovery
4. toggle prefetcher on — visible hit-rate lift
5. point at the dashboard: "X% of prefill compute avoided, p99 still Y ms"
