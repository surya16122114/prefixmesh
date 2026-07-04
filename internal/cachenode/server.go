// Package cachenode implements CacheNodeService over a blockstore.Store
// (DESIGN.md §4.2).
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
	// epoch returns the newest ring epoch this node has seen; nil disables
	// enforcement (static-ring deployments have exactly one epoch).
	epoch func() uint64
}

func New(store blockstore.Store) *Server {
	return &Server{store: store}
}

// WithEpoch enables the WRONG_EPOCH protocol (DESIGN.md §4.1): callers whose
// ring is older than ours get FAILED_PRECONDITION and must refresh. Callers
// AHEAD of us are served — their routing is at least as fresh as our view.
func (s *Server) WithEpoch(epoch func() uint64) *Server {
	s.epoch = epoch
	return s
}

func (s *Server) checkEpoch(callerEpoch uint64) error {
	if s.epoch == nil || callerEpoch == 0 {
		return nil
	}
	if mine := s.epoch(); callerEpoch < mine {
		return status.Errorf(codes.FailedPrecondition,
			"caller ring epoch %d < node epoch %d", callerEpoch, mine)
	}
	return nil
}

func toID(b []byte) (chain.BlockID, error) {
	if len(b) != chain.HashSize {
		return chain.BlockID{}, status.Errorf(codes.InvalidArgument,
			"block id must be %d bytes, got %d", chain.HashSize, len(b))
	}
	return chain.BlockID(b), nil
}

func (s *Server) Contains(_ context.Context, req *pmv1.ContainsRequest) (*pmv1.ContainsResponse, error) {
	if err := s.checkEpoch(req.RingEpoch); err != nil {
		return nil, err
	}
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
	if err := s.checkEpoch(req.RingEpoch); err != nil {
		return err
	}
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
		if err := s.checkEpoch(req.RingEpoch); err != nil {
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
	if err := s.checkEpoch(req.RingEpoch); err != nil {
		return nil, err
	}
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
