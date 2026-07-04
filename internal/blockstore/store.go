// Package blockstore defines the cache node's block storage. M0 ships the
// interface plus a plain-LRU in-memory implementation; M2 replaces the
// internals with a paged arena and cost-aware eviction (DESIGN.md §4.2)
// behind the same interface.
package blockstore

import "github.com/surya16122114/prefixmesh/internal/chain"

type Block struct {
	ID         chain.BlockID
	ParentID   chain.BlockID
	ModelID    string
	Payload    []byte
	TokenCount uint32
	CostUS     uint64
}

type Stats struct {
	OccupancyBytes uint64
	CapacityBytes  uint64
	Hits           uint64
	Misses         uint64
	Evictions      uint64
	EvictedCostUS  uint64 // prefill cost thrown away — eviction-quality signal
}

// Store is the single seam between the cache node's RPC layer and its memory
// management. Implementations must be safe for concurrent use.
type Store interface {
	// Put stores a block; returns false if it was already present (content
	// addressing makes re-puts no-ops that count as a Touch).
	Put(b Block) (stored bool)

	// Get returns the block and bumps its recency.
	Get(id chain.BlockID) (Block, bool)

	// Contains probes existence without transferring payloads. It still bumps
	// recency: a Contains hit means the gateway is about to route a reader here.
	Contains(id chain.BlockID) bool

	Touch(id chain.BlockID)
	Stats() Stats
}

// TODO(M0): implement lru.go — plain LRU over a capacity budget in bytes.
// TODO(M2): implement paged.go — preallocated page arena, cost-aware eviction
// (frecency × cost / size), plain LRU kept behind a flag as benchmark baseline.
