// Package gateway implements GatewayService: chain routing over a consistent
// hash ring (DESIGN.md §4.1, §5).
//
// The ring comes from a RingSource — a ringwatch.Watcher in production, a
// Static source for tests and directory-less dev runs.
package gateway

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/chain"
	"github.com/surya16122114/prefixmesh/internal/hashring"
)

// RingSource provides the current ring and a client for each member.
// Implementations must be safe for concurrent use.
type RingSource interface {
	// Ring returns the latest known ring; nil means "no ring yet".
	Ring() *hashring.Ring
	Client(nodeID string) (pmv1.CacheNodeServiceClient, bool)
}

// Static is a fixed RingSource (M0-style deployments and tests).
type Static struct {
	R       *hashring.Ring
	Clients map[string]pmv1.CacheNodeServiceClient
}

func (s *Static) Ring() *hashring.Ring { return s.R }
func (s *Static) Client(id string) (pmv1.CacheNodeServiceClient, bool) {
	c, ok := s.Clients[id]
	return c, ok
}

type Server struct {
	pmv1.UnimplementedGatewayServiceServer
	src RingSource
	rf  int // replication factor: blocks are Put to rf ring owners
}

// New creates a gateway. rf is the replication factor (clamped to >= 1);
// with rf=2 every block is written to its primary and successor owner, and
// Match falls back to the successor when the primary misses or is down —
// this is what lets the mesh serve straight through single-node loss and
// ring rebalances (a membership change shifts at most one member of an
// owner pair; see hashring.Owners).
func New(src RingSource, rf int) *Server {
	if rf < 1 {
		rf = 1
	}
	return &Server{src: src, rf: rf}
}

// Match resolves the longest cached prefix of the chain: one parallel
// Contains fan-out (one batch per owner node), then matched_depth = the run
// of consecutive hits from the root. Everything past the first miss is a
// guaranteed miss by chain construction, so one round suffices.
//
// Epoch protocol: if any node rejects with FAILED_PRECONDITION (it has seen
// a newer ring than ours), we refresh the ring and retry the whole fan-out
// once. A mesh with no ring yet, or an empty ring, answers "all misses" —
// never an error (DESIGN.md §5).
func (s *Server) Match(ctx context.Context, req *pmv1.MatchRequest) (*pmv1.MatchResponse, error) {
	for _, id := range req.Chain {
		if len(id) != chain.HashSize {
			return nil, status.Errorf(codes.InvalidArgument,
				"block id must be %d bytes, got %d", chain.HashSize, len(id))
		}
	}
	for attempt := 0; ; attempt++ {
		ring := s.src.Ring()
		if ring == nil || ring.Size() == 0 || len(req.Chain) == 0 {
			return &pmv1.MatchResponse{}, nil
		}
		resp, wrongEpoch := s.matchOnRing(ctx, ring, req)
		// Retry once iff a node told us our ring is stale AND we actually
		// have a newer one to retry with.
		if wrongEpoch && attempt == 0 {
			if newer := s.src.Ring(); newer != nil && newer.Epoch > ring.Epoch {
				continue
			}
		}
		return resp, nil
	}
}

func (s *Server) matchOnRing(ctx context.Context, ring *hashring.Ring, req *pmv1.MatchRequest) (*pmv1.MatchResponse, bool) {
	present := make([]bool, len(req.Chain))
	holders := make([]string, len(req.Chain)) // node that answered "have it"

	// Round 1: probe each block's primary owner. Round 2 (rf>1): re-probe
	// the blocks the primary missed — or couldn't answer — at their next
	// replica. Two bounded rounds, still zero directory involvement.
	probe := make([]int, len(req.Chain))
	for i := range probe {
		probe[i] = i
	}
	var wrongEpoch bool
	for replica := 0; replica < s.rf && len(probe) > 0; replica++ {
		retry, we := s.probeReplica(ctx, ring, req.Chain, probe, replica, present, holders)
		wrongEpoch = wrongEpoch || we
		probe = retry
	}

	depth := 0
	for depth < len(present) && present[depth] {
		depth++
	}
	refs := make([]*pmv1.BlockRef, depth)
	nodes := ring.Nodes()
	for i := 0; i < depth; i++ {
		refs[i] = &pmv1.BlockRef{
			BlockId:  req.Chain[i],
			NodeAddr: nodes[holders[i]],
		}
	}
	return &pmv1.MatchResponse{
		MatchedDepth: uint32(depth),
		Refs:         refs,
		RingEpoch:    ring.Epoch,
	}, wrongEpoch
}

// probeReplica asks the replica-th owner of each listed block whether it has
// it, filling present/holders for hits and returning the indices worth
// retrying at the next replica.
func (s *Server) probeReplica(ctx context.Context, ring *hashring.Ring, chainIDs [][]byte,
	indices []int, replica int, present []bool, holders []string) (retry []int, wrongEpoch bool) {

	type batch struct {
		client  pmv1.CacheNodeServiceClient
		indices []int
		ids     [][]byte
	}
	batches := map[string]*batch{}
	var unroutable []int
	for _, i := range indices {
		owners := ring.Owners(chainIDs[i], replica+1)
		if len(owners) <= replica {
			continue // fewer nodes than replicas: nothing further to ask
		}
		nodeID := owners[replica]
		b, ok := batches[nodeID]
		if !ok {
			client, haveClient := s.src.Client(nodeID)
			if !haveClient {
				unroutable = append(unroutable, i) // no conn yet: try next replica
				continue
			}
			b = &batch{client: client}
			batches[nodeID] = b
		}
		b.indices = append(b.indices, i)
		b.ids = append(b.ids, chainIDs[i])
	}

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	retry = unroutable
	for nodeID, b := range batches {
		wg.Add(1)
		go func(nodeID string, b *batch) {
			defer wg.Done()
			resp, err := b.client.Contains(ctx, &pmv1.ContainsRequest{
				RingEpoch: ring.Epoch,
				BlockIds:  b.ids,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				// Unreachable owner: its blocks go to the next replica; with
				// rf=1 they simply miss — never an error (DESIGN.md §5).
				if status.Code(err) == codes.FailedPrecondition {
					wrongEpoch = true
				}
				retry = append(retry, b.indices...)
				return
			}
			for j, idx := range b.indices {
				if j < len(resp.Present) && resp.Present[j] {
					present[idx] = true
					holders[idx] = nodeID
				} else {
					retry = append(retry, idx)
				}
			}
		}(nodeID, b)
	}
	wg.Wait()
	return retry, wrongEpoch
}

// PutBlocks buffers the client stream, routes blocks to their ring owners,
// and forwards one PutBlocks stream per owner in parallel. An owner that is
// down or resharded simply doesn't store — a future miss refills.
func (s *Server) PutBlocks(stream pmv1.GatewayService_PutBlocksServer) error {
	byOwner := map[string][]*pmv1.Block{}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if req.Block == nil || len(req.Block.BlockId) != chain.HashSize {
			return status.Error(codes.InvalidArgument, "missing or malformed block")
		}
		ring := s.src.Ring()
		if ring == nil || ring.Size() == 0 {
			continue // nowhere to store; misses refill later
		}
		for _, nodeID := range ring.Owners(req.Block.BlockId, s.rf) {
			byOwner[nodeID] = append(byOwner[nodeID], req.Block)
		}
	}

	ring := s.src.Ring()
	var epoch uint64
	if ring != nil {
		epoch = ring.Epoch
	}
	var (
		wg               sync.WaitGroup
		mu               sync.Mutex
		stored, existing uint32
	)
	for nodeID, blocks := range byOwner {
		client, ok := s.src.Client(nodeID)
		if !ok {
			continue
		}
		wg.Add(1)
		go func(client pmv1.CacheNodeServiceClient, blocks []*pmv1.Block) {
			defer wg.Done()
			up, err := client.PutBlocks(stream.Context())
			if err != nil {
				return
			}
			for _, b := range blocks {
				if err := up.Send(&pmv1.CachePutRequest{RingEpoch: epoch, Block: b}); err != nil {
					return
				}
			}
			resp, err := up.CloseAndRecv()
			if err != nil {
				return
			}
			mu.Lock()
			stored += resp.Stored
			existing += resp.Existing
			mu.Unlock()
		}(client, blocks)
	}
	wg.Wait()

	return stream.SendAndClose(&pmv1.PutBlocksResponse{
		Stored: stored, Existing: existing,
	})
}
