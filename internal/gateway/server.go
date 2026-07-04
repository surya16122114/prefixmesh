// Package gateway implements GatewayService: chain routing over a consistent
// hash ring (DESIGN.md §4.1, §5).
//
// M0 runs on a static ring (epoch 1, membership from flags); WatchRing
// subscription replaces it in M1 without changing the request paths.
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

type Server struct {
	pmv1.UnimplementedGatewayServiceServer
	ring    *hashring.Ring
	clients map[string]pmv1.CacheNodeServiceClient // nodeID -> client
}

func New(ring *hashring.Ring, clients map[string]pmv1.CacheNodeServiceClient) *Server {
	return &Server{ring: ring, clients: clients}
}

// ownerOf routes one block id; every id must resolve on a non-empty ring.
func (s *Server) ownerOf(blockID []byte) (string, pmv1.CacheNodeServiceClient, error) {
	if len(blockID) != chain.HashSize {
		return "", nil, status.Errorf(codes.InvalidArgument,
			"block id must be %d bytes, got %d", chain.HashSize, len(blockID))
	}
	nodeID, _, ok := s.ring.Owner(blockID)
	if !ok {
		return "", nil, status.Error(codes.Unavailable, "ring is empty")
	}
	c, ok := s.clients[nodeID]
	if !ok {
		return "", nil, status.Errorf(codes.Internal, "no client for ring member %s", nodeID)
	}
	return nodeID, c, nil
}

// Match resolves the longest cached prefix of the chain: one parallel
// Contains fan-out (one batch per owner node), then matched_depth = the run
// of consecutive hits from the root. Everything past the first miss is a
// guaranteed miss by chain construction, so one round suffices.
func (s *Server) Match(ctx context.Context, req *pmv1.MatchRequest) (*pmv1.MatchResponse, error) {
	if len(req.Chain) == 0 {
		return &pmv1.MatchResponse{RingEpoch: s.ring.Epoch}, nil
	}

	type batch struct {
		client  pmv1.CacheNodeServiceClient
		indices []int // positions in req.Chain owned by this node
		ids     [][]byte
	}
	batches := map[string]*batch{}
	owners := make([]string, len(req.Chain))
	for i, id := range req.Chain {
		nodeID, client, err := s.ownerOf(id)
		if err != nil {
			return nil, err
		}
		owners[i] = nodeID
		b, ok := batches[nodeID]
		if !ok {
			b = &batch{client: client}
			batches[nodeID] = b
		}
		b.indices = append(b.indices, i)
		b.ids = append(b.ids, id)
	}

	present := make([]bool, len(req.Chain))
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for _, b := range batches {
		wg.Add(1)
		go func(b *batch) {
			defer wg.Done()
			resp, err := b.client.Contains(ctx, &pmv1.ContainsRequest{
				RingEpoch: s.ring.Epoch,
				BlockIds:  b.ids,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				// An unreachable node means its blocks are misses, never an
				// error surfaced to the client (DESIGN.md §5) — but remember
				// the first error for logging via trailers later (M1).
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			for j, idx := range b.indices {
				if j < len(resp.Present) {
					present[idx] = resp.Present[j]
				}
			}
		}(b)
	}
	wg.Wait()
	_ = firstErr // surfaced as a metric in M4; correctness unaffected

	depth := 0
	for depth < len(present) && present[depth] {
		depth++
	}
	refs := make([]*pmv1.BlockRef, depth)
	nodes := s.ring.Nodes()
	for i := 0; i < depth; i++ {
		refs[i] = &pmv1.BlockRef{
			BlockId:  req.Chain[i],
			NodeAddr: nodes[owners[i]],
		}
	}
	return &pmv1.MatchResponse{
		MatchedDepth: uint32(depth),
		Refs:         refs,
		RingEpoch:    s.ring.Epoch,
	}, nil
}

// PutBlocks buffers the client stream, routes blocks to their ring owners,
// and forwards one PutBlocks stream per owner in parallel.
func (s *Server) PutBlocks(stream pmv1.GatewayService_PutBlocksServer) error {
	byOwner := map[string][]*pmv1.Block{}
	clients := map[string]pmv1.CacheNodeServiceClient{}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if req.Block == nil {
			return status.Error(codes.InvalidArgument, "missing block")
		}
		nodeID, client, err := s.ownerOf(req.Block.BlockId)
		if err != nil {
			return err
		}
		byOwner[nodeID] = append(byOwner[nodeID], req.Block)
		clients[nodeID] = client
	}

	var (
		wg               sync.WaitGroup
		mu               sync.Mutex
		stored, existing uint32
	)
	for nodeID, blocks := range byOwner {
		wg.Add(1)
		go func(client pmv1.CacheNodeServiceClient, blocks []*pmv1.Block) {
			defer wg.Done()
			up, err := client.PutBlocks(stream.Context())
			if err != nil {
				return // owner down: blocks stay uncached; a future miss refills
			}
			for _, b := range blocks {
				if err := up.Send(&pmv1.CachePutRequest{
					RingEpoch: s.ring.Epoch,
					Block:     b,
				}); err != nil {
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
		}(clients[nodeID], blocks)
	}
	wg.Wait()

	return stream.SendAndClose(&pmv1.PutBlocksResponse{
		Stored: stored, Existing: existing,
	})
}
