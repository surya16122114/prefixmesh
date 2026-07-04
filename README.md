# PrefixMesh

**A distributed prefix-cache mesh for LLM inference** — shares computed prefix/KV-cache
blocks across an inference fleet so repeated prompt prefixes (system prompts, RAG
context, few-shot examples) skip prefill everywhere, not just on the node that first
computed them.

Per-process prefix caches (vLLM, SGLang) recompute a shared system prompt once *per
node*. PrefixMesh lifts that cache out of the process: content-addressed blocks on a
consistent-hash ring, a Paxos-backed control plane for membership and rebalancing, and
a Kafka event plane that learns access patterns and warms caches predictively.

> KV-cache blocks are simulated as opaque keyed blobs — this project demonstrates the
> distributed-systems layer (routing, consensus, failure recovery, event-driven
> warming) and its measured effect, without requiring GPUs.

## Architecture

Three planes, four services — the synchronous hot path touches only the data plane:

- **Data plane** (gRPC): stateless **gateways** route content-addressed block IDs over a
  consistent-hash ring to stateful **cache nodes** (paged blocks, cost-aware eviction).
- **Control plane**: a 3-replica **directory** running multi-decree Paxos owns
  membership, epoch-numbered rings, and rebalance leases. Design ported from my
  [distributed-kv-store](https://github.com/surya16122114/distributed-kv-store) (Java),
  reimplemented in Go.
- **Event plane** (Kafka): access telemetry feeds a **prefetcher** whose next-block
  predictions warm caches ahead of demand — strictly off the hot path; every consumer
  is idempotent by content addressing.

The invariant that drives the design: **every failure mode is a cache miss, never a
wrong answer.** Kill any node mid-run and correctness holds; only the hit rate dips.

Full spec: [docs/DESIGN.md](docs/DESIGN.md) · roadmap:
[docs/MILESTONES.md](docs/MILESTONES.md) · benchmark methodology:
[docs/BENCHMARKS.md](docs/BENCHMARKS.md)

## Status

**Complete through its committed scope (M0–M4)** — see
[milestones](docs/MILESTONES.md). The mesh runs a 3-replica **Paxos directory**
(membership changes are consensus commits; quorum suspicion-exchange before any
eviction; epoch-numbered rings streamed to the fleet; `WRONG_EPOCH` staleness
rejection), **RF=2 replication**, a **paged arena block store** with cost-aware
eviction, the **Kafka event plane** (access telemetry → prefetcher → cache
warming; node telemetry for future learners), and **Prometheus/Grafana
observability**. The Paxos core is tested under a simulated lossy/reordering
network with concurrent proposers (`go test -race`); the full warm loop runs in
CI over an in-memory bus, no broker required.

## Measured results

One `make bench` run (Apple M4, seeded; regenerate anytime — the suite
verifies its own cleanup and startup so numbers can't come from a stale mesh):

| Scenario | Result |
|---|---|
| Steady state (4 nodes, RF=2) | **85.8%** hit rate / **87.0%** prefill saved, match p50 <1 ms |
| Cache node killed (Paxos heals epoch in ~2 s) | **89.9% / 92.9%** right after — zero errors, no visible dip (M0's static ring had collapsed to 7.7% forever) |
| ALL 3 directory replicas + a node killed (frozen ring) | **89.9% / 92.9%** — RF=2 failover alone carries it; replication and consensus are independent recovery layers |
| Eviction at equal memory, cache ⅔ short of working set | LRU **46.0%** vs cost-aware **58.4%** prefill saved at the same block hit rate — evict by value, not recency |
| Prefetcher A/B (double kill, idle window between) | off **80.2%** saved vs on **91.8%** — warming restores redundancy ahead of demand |
| Kafka broker killed mid-run | hot path unaffected (the plane is lossy by contract) |

Benchmarks will be published only once they're reproducible via `make bench`, with
hardware and workload seeds disclosed.

## Development

```sh
make proto       # regenerate gRPC stubs (protoc + protoc-gen-go/-go-grpc)
make test        # go test -race ./...
make bench       # full reproducible benchmark suite -> bench-results.md
make compose-up  # kafka + 3 directory + 4 cache nodes + prefetcher + gateway
                 #   + prometheus (:9090) + grafana (:3000, anonymous admin)
```

Every service serves Prometheus at `--metrics` (default `:9100`); the compose
stack ships a provisioned Grafana dashboard: hit rate, match p50/p99, ring
epoch, per-node occupancy, evictions, and warm-command throughput.

## Stack

Go · gRPC/protobuf · Apache Kafka (KRaft) · hand-rolled multi-decree Paxos ·
Prometheus/Grafana · Docker Compose
