#!/bin/bash
# PrefixMesh reproducible benchmark suite (docs/BENCHMARKS.md).
#
# Runs every scenario the README quotes, on localhost, with the pitfalls that
# bit us during development baked in as checks:
#   - explicit process cleanup with verification (a leftover mesh once served
#     both sides of an A/B, producing identical garbage numbers)
#   - startup/bind verification before any measurement
#   - fixed --workload-seed so runs share a corpus; only --seed varies
#   - bash, not zsh, so flag strings word-split predictably
#
# Usage: scripts/bench.sh [outfile]
# Kafka scenarios need Docker; they are skipped (loudly) without it.
set -u
cd "$(dirname "$0")/.."

OUT="${1:-bench-results.md}"
: > "$OUT"
SP="$(mktemp -d)"
DIRS="127.0.0.1:7201,127.0.0.1:7202,127.0.0.1:7203"
GW="127.0.0.1:7001"

say()  { echo "== $*" >&2; }
emit() { echo "$*" >> "$OUT"; }

kill_mesh() {
  pkill -9 -f "bin/(cachenode|gateway|directory|prefetcher)" 2>/dev/null
  sleep 1
  if pgrep -f "bin/(cachenode|gateway|directory|prefetcher)" >/dev/null; then
    echo "FATAL: mesh processes survived cleanup" >&2; exit 1
  fi
}
trap kill_mesh EXIT

check_started() {
  sleep 2
  if grep -l "listen failed\|flag provided" "$SP"/*.log >/dev/null 2>&1; then
    echo "FATAL: a service failed to start:" >&2
    grep -h "listen failed\|flag provided" "$SP"/*.log >&2
    exit 1
  fi
}

start_dirs() {
  ./bin/directory --listen 127.0.0.1:7201 --replica-id dir-1 --metrics "" \
    --peers "dir-2=127.0.0.1:7202,dir-3=127.0.0.1:7203" > "$SP/dir1.log" 2>&1 &
  ./bin/directory --listen 127.0.0.1:7202 --replica-id dir-2 --metrics "" \
    --peers "dir-1=127.0.0.1:7201,dir-3=127.0.0.1:7203" > "$SP/dir2.log" 2>&1 &
  ./bin/directory --listen 127.0.0.1:7203 --replica-id dir-3 --metrics "" \
    --peers "dir-1=127.0.0.1:7201,dir-2=127.0.0.1:7202" > "$SP/dir3.log" 2>&1 &
  sleep 1
}

# start_nodes <count> <capacity-bytes> <eviction> [kafka-brokers]
start_nodes() {
  local n=$1 cap=$2 pol=$3 kafka=${4:-}
  local extra=()
  [ -n "$kafka" ] && extra=(--kafka "$kafka" --warm-rate 2000)
  for i in $(seq 1 "$n"); do
    ./bin/cachenode --listen "127.0.0.1:710$i" --node-id "cn-$i" --metrics "" \
      --capacity-bytes "$cap" --eviction "$pol" --directory "$DIRS" \
      ${extra[@]+"${extra[@]}"} > "$SP/cn$i.log" 2>&1 &
  done
}

# start_gateway <rf> [kafka-brokers]
start_gateway() {
  local rf=$1 kafka=${2:-}
  local extra=()
  [ -n "$kafka" ] && extra=(--kafka "$kafka")
  ./bin/gateway --listen "$GW" --directory "$DIRS" --replication "$rf" --metrics "" \
    ${extra[@]+"${extra[@]}"} > "$SP/gw.log" 2>&1 &
}

loadgen() { ./bin/loadgen --gateway "$GW" "$@"; }
hits()    { grep -E "hit rate|saved|latency" ; }

say "building"
go build -o bin/ ./cmd/... || exit 1
emit "# PrefixMesh benchmark results"
emit ""
emit "\`$(date '+%Y-%m-%d %H:%M %Z')\` · \`$(uname -m)\` · \`$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown-cpu)\` · Go \`$(go version | awk '{print $3}')\`"
emit ""

### Scenario 1: steady state + node kill/recovery ##########################
say "scenario 1: steady state + kill/recovery"
emit "## 1. Steady state and node-kill recovery (4 nodes, RF=2, 256 MB each)"
emit '```'
start_dirs; start_nodes 4 $((256*1024*1024)) cost; start_gateway 2; check_started
emit "--- steady state (seed 42) ---"
loadgen --requests 2000 --seed 42 | hits >> "$OUT"
pkill -9 -f "node-id cn-2"; sleep 4
emit "--- immediately after killing cn-2 (epoch healed) ---"
loadgen --requests 2000 --seed 44 | hits >> "$OUT"
emit '```'
kill_mesh; trap kill_mesh EXIT

### Scenario 2: total control-plane loss + node kill (RF=2 failover) #######
say "scenario 2: frozen ring + node kill"
emit "## 2. All directory replicas killed, THEN a cache node killed (RF=2 failover on a frozen ring)"
emit '```'
start_dirs; start_nodes 4 $((256*1024*1024)) cost; start_gateway 2; check_started
loadgen --requests 2000 --seed 42 > /dev/null
pkill -9 -f "bin/directory"; sleep 0.5
pkill -9 -f "node-id cn-2"; sleep 0.5
emit "--- ring frozen, one node dead ---"
loadgen --requests 2000 --seed 44 | hits >> "$OUT"
emit '```'
kill_mesh; trap kill_mesh EXIT

### Scenario 3: eviction policy A/B at equal memory ########################
say "scenario 3: eviction A/B"
emit "## 3. Cost-aware vs LRU eviction, equal memory (4×8 MB for ~53 MB working set, uniform popularity, 20% of docs 10× cost)"
emit '```'
for pol in lru cost; do
  start_dirs; start_nodes 4 $((8*1024*1024)) "$pol"; start_gateway 1; check_started
  loadgen --requests 4000 --seed 42 --zipf-s 0 > /dev/null   # warm to steady state
  emit "--- $pol ---"
  loadgen --requests 4000 --seed 43 --zipf-s 0 | hits >> "$OUT"
  kill_mesh; trap kill_mesh EXIT
done
emit '```'

### Scenario 4: prefetcher A/B (needs Kafka via Docker) ####################
if docker info >/dev/null 2>&1; then
  say "scenario 4: prefetcher A/B (starting Kafka)"
  docker rm -f pm-bench-kafka >/dev/null 2>&1
  docker run -d --rm --name pm-bench-kafka -p 9092:9092 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
    apache/kafka:3.9.0 >/dev/null && sleep 12

  emit "## 4. Predictive warming: double node-kill with idle window, prefetcher off vs on"
  emit '```'
  for mode in off on; do
    start_dirs
    if [ "$mode" = on ]; then
      start_nodes 4 $((256*1024*1024)) cost 127.0.0.1:9092
      start_gateway 2 127.0.0.1:9092
      ./bin/prefetcher --kafka 127.0.0.1:9092 --directory "$DIRS" --replication 2 > "$SP/pf.log" 2>&1 &
    else
      start_nodes 4 $((256*1024*1024)) cost
      start_gateway 2
    fi
    check_started
    loadgen --requests 2000 --seed 42 --zipf-s 0 > /dev/null
    loadgen --requests 2000 --seed 43 --zipf-s 0 > /dev/null
    pkill -9 -f "node-id cn-2"; sleep 8   # idle window: only the event plane can act
    pkill -9 -f "node-id cn-3"; sleep 4
    emit "--- prefetcher $mode ---"
    loadgen --requests 600 --seed 44 --zipf-s 0 | hits >> "$OUT"
    kill_mesh; trap kill_mesh EXIT
  done
  emit '```'
  docker kill pm-bench-kafka >/dev/null 2>&1
else
  say "scenario 4 SKIPPED: Docker unavailable (Kafka required)"
  emit "## 4. Predictive warming A/B: **skipped** (Docker/Kafka unavailable on this run)"
fi

say "done -> $OUT"
cat "$OUT"
