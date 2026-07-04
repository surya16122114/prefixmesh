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

**M0–M3 complete** (see [milestones](docs/MILESTONES.md)): 3-replica **Paxos
directory** (membership changes are consensus commits; quorum
suspicion-exchange before any eviction; epoch-numbered rings streamed to the
fleet; `WRONG_EPOCH` staleness rejection), **RF=2 replication**, a **paged
arena block store** with cost-aware eviction, and the **Kafka event plane**:
access telemetry feeds a prefetcher that re-warms demanded blocks onto new
owners on every ring change — join re-warming and post-death redundancy
restoration are the same mechanism. The Paxos core is tested under a simulated
lossy/reordering network with concurrent proposers (`go test -race`); the full
warm loop runs in CI over an in-memory bus, no broker required.

All numbers from one M2 MacBook, seeded and reproducible (`bin/loadgen --seed …`):

- Steady state (4 nodes): **~86% block hit rate / prefill compute saved**,
  match p50 ~0.5 ms, p99 ~1.5 ms.
- **Kill a cache node**: zero errors; `NodeDead` commits and the epoch bumps in
  ~2 s; next run back at 85.9% (M0's static ring stayed collapsed at 7.7%).
- **Kill the entire directory *and* a cache node** (ring frozen, no healing
  possible): RF=2 failover holds the hit rate at **86.0%** — replication and
  consensus are independent recovery layers.
- **Eviction under pressure** (4×8 MB for a ~53 MB working set, 20% of docs
  10× prefill cost): cost-aware eviction saves **50.5%** of prefill compute vs
  LRU's **46.1%** at equal memory, by accepting a lower raw hit rate (62.9% vs
  64.5%) — evict by value, not recency. With ample cache the policies tie, as
  they should.
- **Predictive warming** (real Kafka, double node-kill with an idle window):
  without the event plane the mesh drops to 83.5% hit rate / 80.3% saved;
  with the prefetcher re-replicating demanded blocks between the kills it
  holds **89.8% / 92.1%** — the workload's ceiling. Killing the Kafka broker
  mid-run changes nothing on the hot path.

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
