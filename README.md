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

**M0 + M1 complete** (see [milestones](docs/MILESTONES.md)): the mesh now runs a
3-replica **Paxos directory** — membership changes are consensus commits, rings are
epoch-numbered and streamed to the fleet, and stale routing is rejected via the
`WRONG_EPOCH` protocol. The Paxos core is tested under a simulated lossy/reordering
network with concurrent proposers (`go test -race`).

Numbers from one M2 MacBook (4 cache nodes, 2000 requests/run, seeded — reproduce
with `bin/loadgen --seed 42`): **85.8% block hit rate / prefill compute saved**,
match p50 ~0.5 ms, p99 ~1.5 ms. Kill a cache node mid-load: **zero errors**,
`NodeDead` commits and the epoch bumps in ~2 s, and the next run is back at 85.9% —
on M0's static ring the same kill left the hit rate collapsed at 7.7% permanently.
That delta is the reason the control plane exists.

Benchmarks will be published only once they're reproducible via `make bench`, with
hardware and workload seeds disclosed.

## Development

```sh
make proto   # regenerate gRPC stubs (protoc + protoc-gen-go/-go-grpc)
make test    # go test -race ./...
make compose-up  # kafka + 3 directory + 4 cache nodes + gateway
```

## Stack

Go · gRPC/protobuf · Apache Kafka (KRaft) · hand-rolled multi-decree Paxos ·
Prometheus/Grafana · Docker Compose
