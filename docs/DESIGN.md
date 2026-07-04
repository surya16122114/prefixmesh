# PrefixMesh — Design Specification

**A distributed prefix-cache mesh for LLM inference.**
PrefixMesh shares computed prefix/KV-cache blocks across an inference fleet so that
repeated prompt prefixes (system prompts, RAG context, few-shot examples) skip prefill
on every node — not just the one that happened to see them first.

> Scope guard: KV-cache blocks are simulated as opaque keyed blobs. PrefixMesh proves the
> **distributed-systems properties** (routing, consensus, rebalancing, failure recovery,
> event-driven cache warming) without requiring GPUs. A real-model demo is a stretch goal
> ([MILESTONES.md](MILESTONES.md), M5).

---

## 1. Problem

Modern inference servers (vLLM, SGLang, TensorRT-LLM) cache the transformer KV-state of
prompt prefixes so identical prefixes skip prefill. But that cache is **per-process**: in a
fleet behind a load balancer, a system prompt computed on node A is recomputed from scratch
on nodes B–Z. Prefill is compute-bound and dominates time-to-first-token for long prompts,
so fleet-wide duplicate prefill is a real cost (RAG and agentic workloads share huge
prefixes across requests).

PrefixMesh is the missing layer: a **fleet-wide, content-addressed cache of prefix blocks**
with a consistent-hash data plane, a Paxos-backed control plane, and a Kafka event plane
for predictive cache warming.

## 2. Non-goals

- Running actual model inference or holding real GPU memory (simulated blobs).
- Cross-model cache sharing (a cache is scoped to one `model_id`; blocks from different
  models never mix).
- Durability. This is a cache: a lost block is a cache miss, never an error.
- Multi-region. Single-cluster design.

## 3. Data model

### 3.1 Prefix hash chain (content addressing)

Prompts are tokenized and split into fixed-size blocks of `block_size` tokens
(default 128). Block IDs form a **hash chain**, so a block's identity encodes its entire
prefix — the same trick vLLM uses for prefix caching, lifted out of the process:

```
h_0 = SHA-256(model_id || block_size)
h_i = SHA-256(h_{i-1} || tokens_i)          // tokens_i = i-th block of token IDs
```

Two prompts that share their first *k* blocks produce identical `h_1..h_k`. Lookup is
"longest prefix match": walk the chain until the first miss. Everything after the first
miss is by construction also a miss (the chain diverges), so one round-trip resolves it.

### 3.2 Block

| Field        | Type              | Notes                                             |
|--------------|-------------------|---------------------------------------------------|
| `block_id`   | 32 bytes          | chain hash `h_i`                                  |
| `parent_id`  | 32 bytes          | `h_{i-1}`; roots point at `h_0`                   |
| `model_id`   | string            | cache namespace                                   |
| `payload`    | bytes             | simulated KV tensor (`payload_bytes` config, default 64 KiB — stands in for the ~MBs/block of a real KV cache) |
| `token_count`| uint32            | tokens this block covers (== block_size except tail) |
| `cost_us`    | uint64            | simulated prefill cost to recreate this block — input to cost-aware eviction |

## 4. Architecture

Three planes, four services. **The synchronous hot path touches only the data plane.**

```
                          ┌────────────────────────────┐
        inference client  │  GATEWAY (stateless, xN)   │   data plane (gRPC, sync)
        ───── gRPC ─────► │  chain hashing, routing by │
                          │  consistent hash(block_id) │
                          └──────┬──────────────┬──────┘
                                 │ Get/Put      │ ring updates (watch)
                     ┌───────────▼───┐   ┌──────▼─────────────────────┐
                     │ CACHE NODES   │   │ DIRECTORY (3–5 nodes)      │  control plane
                     │ (stateful, xM)│   │ Paxos: membership, ring    │  (Paxos, ported
                     │ paged blocks, │◄──┤ epochs, rebalance leases   │   from DKV)
                     │ cost-aware LRU│   └────────────────────────────┘
                     └──────┬────────┘
                            │ access telemetry (async, fire-and-forget)
                     ┌──────▼─────────────────────────────────────────┐
                     │ KAFKA event plane                              │
                     │ prefix.access.v1 ──► PREFETCHER ──► cache.warm.v1 ──► cache nodes
                     └────────────────────────────────────────────────┘
```

### 4.1 Gateway (data plane, stateless)

- Accepts `Match` / `PutBlocks` from inference clients.
- Computes the hash chain locally (client may also precompute and send hashes only —
  the token IDs themselves never need to leave the client).
- Routes each `block_id` to its owner via a **consistent hash ring** (virtual nodes,
  ~128 vnodes per cache node) cached locally; subscribes to `DirectoryService.WatchRing`
  for epoch-numbered updates.
- **Ring epoch protocol:** every data-plane request carries the gateway's ring epoch.
  A cache node that has seen a newer epoch rejects with `WRONG_EPOCH` + the new epoch;
  the gateway refreshes and retries once. This bounds staleness without putting the
  directory on the hot path.
- Emits one `AccessEvent` per request to Kafka, `acks=0` fire-and-forget, buffered —
  the hot path never blocks on the event plane.

### 4.2 Cache node (data plane, stateful)

- **Paged block store:** fixed-size pages in a preallocated arena (simulating GPU/host
  memory paging), block payloads mapped onto pages, O(1) id→page index.
- **Cost-aware LRU eviction:** evict by lowest `frecency × cost_us / size` score — cheap-
  to-recompute blocks go first, expensive prefill survivors stay. (Plain LRU kept behind
  a flag as the benchmark baseline.)
- Serves `GetBlocks` (streamed), `PutBlocks` (streamed, idempotent by content address —
  a re-put of an existing block is a no-op `Touch`), `Touch`.
- Consumes `cache.warm.v1` for its own partitions and executes prefetch `PutBlocks`
  pulled from peer nodes; warming is best-effort and rate-limited so it never competes
  with foreground traffic.
- Heartbeats to the directory; on `JOINING`/`LEAVING` it participates in rebalancing
  (§4.3).

### 4.3 Directory (control plane — Paxos, ported from DKV)

The concepts port directly from `distributed-kv-store` (`PaxosCoordinator`,
`Membership`, `ConsistentHashRing`), reimplemented in Go:

- 3 or 5 directory replicas run **multi-decree Paxos** over a log of `ClusterCommand`s:
  `NodeJoin`, `NodeLeave`, `NodeDead(suspected-by-quorum)`, `RingEpochBump`,
  `RebalanceLease{grant,release}`.
- The replicated state machine yields the authoritative **ring epoch → member set**
  mapping. Gateways and cache nodes watch it; nobody else needs consensus.
- **Failure detection:** cache nodes heartbeat every 500 ms; a directory replica that
  misses 3 heartbeats proposes `NodeDead`; the ring only changes when the proposal
  commits — so a network blip at one replica can't flap the ring.
- **Rebalancing on join/leave:** new epoch shifts ~1/M of the key space (consistent
  hashing). Affected ranges are moved lazily: a miss on the new owner triggers
  recompute-or-peer-fetch; a `RebalanceLease` lets the old owner serve reads for its
  former range for a grace period (default 30 s) so a join doesn't cause a miss storm.
- Why consensus at all (and not just gossip): the ring must be a **single agreed
  sequence of epochs**. Two gateways disagreeing about ownership means duplicated blocks
  at worst — but two *cache nodes* disagreeing about rebalance leases means both evicting
  "someone else's" range during a churn window, i.e. correlated cache wipeout. The
  directory makes churn decisions linearizable while staying entirely off the hot path.

### 4.4 Prefetcher + event plane (Kafka)

Kafka earns its place with **one primary job**: predictive cache warming. (If prefetch
were ever cut, the honest move is Redis Streams/NATS, not Kafka-as-décor.)

| Topic              | Key                       | Producer   | Consumers                  | Payload |
|--------------------|---------------------------|------------|----------------------------|---------|
| `prefix.access.v1` | first-block hash `h_1`    | gateway    | prefetcher, (M4: eviction learner) | AccessEvent: chain head, depth matched, depth requested, tenant, ts |
| `cache.warm.v1`    | target `block_id`         | prefetcher | cache nodes                | WarmCommand: block_id, parent_id, source hint (peer node or RECOMPUTE) |
| `cache.telemetry.v1`| node_id                  | cache nodes| metrics sink, eviction learner | evictions, occupancy, hit/miss counters |

- **Partitioning by prefix/block hash** keeps each prefix family's events ordered within
  a partition and spreads load uniformly (hashes are uniform by construction).
- **Consumers are idempotent by design**: warming an already-present block is a no-op
  (content addressing), so at-least-once delivery is safe with zero dedup machinery.
- **Prediction model (MVP):** per chain-head, a first-order Markov model over observed
  "descend to child block X next" transitions with exponential decay. When
  `P(child | head)` exceeds a threshold and the child is absent from its owner, emit a
  `WarmCommand`. Deliberately simple — the interesting claim is the *plumbing* (measured
  hit-rate lift), not the model.

## 5. Request flows

**Match (read):** client sends chain hashes → gateway binary-searches the chain for the
divergence point using batched `Contains` probes to owner nodes (one batch per owner,
fan-out in parallel) → returns `matched_depth` + per-block `(node, lease)` handles →
client streams `GetBlocks` directly from owner nodes.
Target: ≤ 2 sequential network hops before block streaming starts.

**Put (write-back after prefill):** client streams new blocks to gateway → gateway
routes each to its ring owner → owners store + ack. Duplicate concurrent puts of the
same block are benign (same content, same address).

**Node death mid-read:** `GetBlocks` fails → client reports the miss and recomputes
those blocks (correctness never depends on the cache) → directory commits `NodeDead`,
bumps epoch → misses migrate to the new owner and refill. **The failure mode of every
component is "cache miss," never "wrong answer" — this invariant drives the whole
design and is what the chaos benchmark (BENCHMARKS.md §4) demonstrates.**

## 6. Consistency model

- **Blocks are immutable** (content-addressed) — no read-your-writes problem exists; any
  copy of a block is the right copy. Replication (M2, RF=2 for hot blocks) needs no
  write coordination for the same reason.
- **Ring/membership is linearizable** via Paxos; data-plane staleness is bounded by the
  epoch-check protocol (§4.1).
- **Event plane is eventually consistent** and lossy by contract (`acks=0` telemetry);
  nothing correctness-bearing rides on it.

## 7. Observability

Every service exposes Prometheus `/metrics`; the compose stack ships Prometheus +
Grafana with one dashboard: hit rate, prefill-µs saved, p50/p99 match latency, ring
epoch, occupancy per node, warm-command effectiveness (warmed block hit within 60 s).

## 8. gRPC contracts

Authoritative definitions in [`api/proto/prefixmesh/v1/`](../api/proto/prefixmesh/v1/):
`gateway.proto` (client-facing `Match`/`PutBlocks`), `cachenode.proto`
(`Contains`/`GetBlocks`/`PutBlocks`/`Touch`), `directory.proto`
(`WatchRing`/`Heartbeat`/`Join` + internal `Prepare`/`Accept`/`Learn` Paxos RPCs),
`events.proto` (Kafka payload schemas — also protobuf, one schema language everywhere).

## 9. Technology choices

| Choice | Why |
|---|---|
| **Go** | DS lingua franca (etcd, k8s, NATS); single static binaries; goroutines fit the fan-out-heavy gateway. Deliberately a second language next to the Java DKV. |
| **gRPC/protobuf** | Streaming block transfer, typed contracts, deadline propagation. |
| **Kafka (KRaft, single broker in dev)** | Replayable access log → prefetcher can be rebuilt/backtested against history; partition ordering per prefix family. |
| **franz-go** | Modern pure-Go Kafka client, no CGO. |
| **Paxos hand-rolled (not etcd/raft lib)** | The point is demonstrating consensus competence; ports the DKV design. Kept off the hot path so its blast radius is bounded. |
