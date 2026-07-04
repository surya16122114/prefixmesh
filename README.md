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

**M0 complete** (see [milestones](docs/MILESTONES.md)): gateway + cache nodes serve
the full Match/PutBlocks happy path on a static ring, with an end-to-end test and a
reproducible loadgen. First local numbers (3 cache nodes, one M2 MacBook, seed 42,
2000 requests — reproduce with `bin/loadgen --seed 42`): **85.8% block hit rate /
prefill compute saved**, match p50 0.47 ms, p99 1.5 ms. Killing a node mid-run
produces zero errors (misses only) — but on a static ring the hit rate can't recover,
which is precisely the job of the M1 Paxos directory.

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
