// Package cachenode implements CacheNodeService over a blockstore.Store
// (DESIGN.md §4.2).
//
// M0 note: ring_epoch is carried on every request but not yet enforced —
// epoch rejection (WRONG_EPOCH) lands with the M1 directory, since a static
// ring has exactly one epoch.
package cachenode

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/blockstore"
	"github.com/surya16122114/prefixmesh/internal/chain"
)

type Server struct {
	pmv1.UnimplementedCacheNodeServiceServer
	store blockstore.Store
}

func New(store blockstore.Store) *Server {
	return &Server{store: store}
}

func toID(b []byte) (chain.BlockID, error) {
	if len(b) != chain.HashSize {
		return chain.BlockID{}, status.Errorf(codes.InvalidArgument,
			"block id must be %d bytes, got %d", chain.HashSize, len(b))
	}
	return chain.BlockID(b), nil
}

func (s *Server) Contains(_ context.Context, req *pmv1.ContainsRequest) (*pmv1.ContainsResponse, error) {
	present := make([]bool, len(req.BlockIds))
	for i, raw := range req.BlockIds {
		id, err := toID(raw)
		if err != nil {
			return nil, err
		}
		present[i] = s.store.Contains(id)
	}
	return &pmv1.ContainsResponse{Present: present}, nil
}

func (s *Server) GetBlocks(req *pmv1.GetBlocksRequest, stream pmv1.CacheNodeService_GetBlocksServer) error {
	for _, raw := range req.BlockIds {
		id, err := toID(raw)
		if err != nil {
			return err
		}
		b, ok := s.store.Get(id)
		if !ok {
			continue // a miss is not an error; the client recomputes
		}
		if err := stream.Send(&pmv1.GetBlocksResponse{Block: toProto(b)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) PutBlocks(stream pmv1.CacheNodeService_PutBlocksServer) error {
	var stored, existing uint32
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&pmv1.CachePutResponse{
				Stored: stored, Existing: existing,
			})
		}
		if err != nil {
			return err
		}
		b, err := fromProto(req.Block)
		if err != nil {
			return err
		}
		if s.store.Put(b) {
			stored++
		} else {
			existing++
		}
	}
}

func (s *Server) Touch(_ context.Context, req *pmv1.TouchRequest) (*pmv1.TouchResponse, error) {
	for _, raw := range req.BlockIds {
		id, err := toID(raw)
		if err != nil {
			return nil, err
		}
		s.store.Touch(id)
	}
	return &pmv1.TouchResponse{}, nil
}

func toProto(b blockstore.Block) *pmv1.Block {
	return &pmv1.Block{
		BlockId:    b.ID[:],
		ParentId:   b.ParentID[:],
		ModelId:    b.ModelID,
		Payload:    b.Payload,
		TokenCount: b.TokenCount,
		CostUs:     b.CostUS,
	}
}

func fromProto(p *pmv1.Block) (blockstore.Block, error) {
	if p == nil {
		return blockstore.Block{}, status.Error(codes.InvalidArgument, "missing block")
	}
	id, err := toID(p.BlockId)
	if err != nil {
		return blockstore.Block{}, err
	}
	parent, err := toID(p.ParentId)
	if err != nil {
		return blockstore.Block{}, err
	}
	return blockstore.Block{
		ID:         id,
		ParentID:   parent,
		ModelID:    p.ModelId,
		Payload:    p.Payload,
		TokenCount: p.TokenCount,
		CostUS:     p.CostUs,
	}, nil
}
