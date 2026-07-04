// Package hashring implements the consistent hash ring the gateway uses to
// route block IDs to owner cache nodes (DESIGN.md §4.1). Rings are immutable
// value objects stamped with the directory's epoch; membership changes produce
// a new ring, never mutate one in place.
package hashring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

const DefaultVNodes = 128

type vnode struct {
	point  uint64
	nodeID string
}

type Ring struct {
	Epoch  uint64
	vnodes []vnode          // sorted by point
	addrs  map[string]string // nodeID -> addr
}

// New builds a ring from node membership. nodes maps nodeID -> gRPC addr.
func New(epoch uint64, nodes map[string]string, vnodesPerNode int) *Ring {
	if vnodesPerNode <= 0 {
		vnodesPerNode = DefaultVNodes
	}
	r := &Ring{
		Epoch:  epoch,
		vnodes: make([]vnode, 0, len(nodes)*vnodesPerNode),
		addrs:  make(map[string]string, len(nodes)),
	}
	for id, addr := range nodes {
		r.addrs[id] = addr
		for i := 0; i < vnodesPerNode; i++ {
			h := sha256.Sum256(fmt.Appendf(nil, "%s#%d", id, i))
			r.vnodes = append(r.vnodes, vnode{
				point:  binary.BigEndian.Uint64(h[:8]),
				nodeID: id,
			})
		}
	}
	sort.Slice(r.vnodes, func(a, b int) bool {
		va, vb := r.vnodes[a], r.vnodes[b]
		if va.point != vb.point {
			return va.point < vb.point
		}
		return va.nodeID < vb.nodeID // deterministic on (vanishingly rare) collisions
	})
	return r
}

// Owner returns the node owning a block ID, walking clockwise from the key's
// point. ok is false on an empty ring.
func (r *Ring) Owner(blockID []byte) (nodeID, addr string, ok bool) {
	if len(r.vnodes) == 0 {
		return "", "", false
	}
	key := binary.BigEndian.Uint64(blockID[:8]) // chain hashes are uniform; reuse prefix
	i := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i].point >= key })
	if i == len(r.vnodes) {
		i = 0 // wrap
	}
	id := r.vnodes[i].nodeID
	return id, r.addrs[id], true
}

func (r *Ring) Nodes() map[string]string {
	out := make(map[string]string, len(r.addrs))
	for k, v := range r.addrs {
		out[k] = v
	}
	return out
}

func (r *Ring) Size() int { return len(r.addrs) }
