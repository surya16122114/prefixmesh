package directory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/paxos"
)

// Server is one directory replica: the DirectoryService gRPC surface plus
// the colocated Paxos roles.
type Server struct {
	pmv1.UnimplementedDirectoryServiceServer
	node *paxos.Node
	sm   *StateMachine
	hb   *hbTracker
}

type Config struct {
	ReplicaID string
	Peers     map[string]string // replicaID -> addr, EXCLUDING self
	// Failure detection: a member missing heartbeats for DeadAfter is
	// proposed dead. Defaults: 500ms / 1.5s.
	HeartbeatEvery time.Duration
	DeadAfter      time.Duration
}

func (c *Config) defaults() {
	if c.HeartbeatEvery == 0 {
		c.HeartbeatEvery = 500 * time.Millisecond
	}
	if c.DeadAfter == 0 {
		c.DeadAfter = 3 * c.HeartbeatEvery
	}
}

// New assembles a replica. Call Run to start its background loops, and
// register the returned server on a grpc.Server.
func New(cfg Config) (*Server, error) {
	cfg.defaults()
	sm := NewStateMachine()
	log := paxos.NewLog(sm.Apply)

	replicas := []string{cfg.ReplicaID}
	for id := range cfg.Peers {
		replicas = append(replicas, id)
	}
	noop, err := proto.Marshal(&pmv1.ClusterCommand{Cmd: &pmv1.ClusterCommand_Noop{Noop: true}})
	if err != nil {
		return nil, err
	}
	tr := &grpcTransport{peers: cfg.Peers, self: cfg.ReplicaID}
	node := paxos.NewNode(paxos.Config{
		Self:     cfg.ReplicaID,
		Replicas: replicas,
		Equal:    commandsEqual,
		Noop:     noop,
	}, tr, log)
	tr.local = node

	s := &Server{node: node, sm: sm, hb: newHBTracker(cfg)}
	sm.onJoin = s.hb.seed // joining members get a fresh grace period
	return s, nil
}

// Run starts the failure detector and log gap filler until ctx ends.
func (s *Server) Run(ctx context.Context) {
	go s.node.RunGapFiller(ctx, 300*time.Millisecond)
	go s.runFailureDetector(ctx)
}

// commandsEqual compares marshaled ClusterCommands semantically — protobuf
// bytes are not canonical across a wire round-trip.
func commandsEqual(a, b []byte) bool {
	var ca, cb pmv1.ClusterCommand
	if proto.Unmarshal(a, &ca) != nil || proto.Unmarshal(b, &cb) != nil {
		return false
	}
	return proto.Equal(&ca, &cb)
}

func (s *Server) submit(ctx context.Context, cmd *pmv1.ClusterCommand) error {
	raw, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	return s.node.Submit(ctx, raw)
}

// --- fleet-facing RPCs ---

func (s *Server) Join(ctx context.Context, req *pmv1.JoinRequest) (*pmv1.JoinResponse, error) {
	if req.Node == nil || req.Node.NodeId == "" || req.Node.Addr == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and addr are required")
	}
	if err := s.submit(ctx, &pmv1.ClusterCommand{
		Cmd: &pmv1.ClusterCommand_NodeJoin{NodeJoin: req.Node},
	}); err != nil {
		return nil, status.Errorf(codes.Unavailable, "consensus: %v", err)
	}
	// Submit returning nil means the command is committed and applied locally.
	if !s.sm.HasMember(req.Node.NodeId) {
		return nil, status.Error(codes.Internal, "join committed but not applied")
	}
	return &pmv1.JoinResponse{Ring: s.sm.Ring()}, nil
}

func (s *Server) Leave(ctx context.Context, req *pmv1.LeaveRequest) (*pmv1.LeaveResponse, error) {
	if err := s.submit(ctx, &pmv1.ClusterCommand{
		Cmd: &pmv1.ClusterCommand_NodeLeave{NodeLeave: req.NodeId},
	}); err != nil {
		return nil, status.Errorf(codes.Unavailable, "consensus: %v", err)
	}
	return &pmv1.LeaveResponse{}, nil
}

func (s *Server) Heartbeat(_ context.Context, req *pmv1.HeartbeatRequest) (*pmv1.HeartbeatResponse, error) {
	s.hb.record(req.NodeId)
	return &pmv1.HeartbeatResponse{}, nil
}

func (s *Server) GetRing(context.Context, *pmv1.GetRingRequest) (*pmv1.Ring, error) {
	return s.sm.Ring(), nil
}

func (s *Server) WatchRing(req *pmv1.WatchRingRequest, stream pmv1.DirectoryService_WatchRingServer) error {
	sig := s.sm.Subscribe()
	defer s.sm.Unsubscribe(sig)

	lastSent := uint64(0)
	send := func() error {
		ring := s.sm.Ring()
		if ring.Epoch == lastSent && lastSent != 0 {
			return nil
		}
		lastSent = ring.Epoch
		return stream.Send(ring)
	}
	// Current state first, even if the watcher already knows it — cheap, and
	// it makes reconnect logic trivial for clients.
	if err := send(); err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-sig:
			if err := send(); err != nil {
				return err
			}
		}
	}
}

// --- replica-internal Paxos RPCs ---

func (s *Server) Prepare(_ context.Context, req *pmv1.PrepareRequest) (*pmv1.PrepareResponse, error) {
	promised, ab, av, pb := s.node.Acceptor.Prepare(req.Slot, paxos.Ballot(req.Ballot))
	resp := &pmv1.PrepareResponse{
		Promised:       promised,
		AcceptedBallot: uint64(ab),
		PromisedBallot: uint64(pb),
	}
	if av != nil {
		var cmd pmv1.ClusterCommand
		if err := proto.Unmarshal(av, &cmd); err != nil {
			return nil, status.Errorf(codes.Internal, "corrupt accepted value: %v", err)
		}
		resp.AcceptedValue = &cmd
	}
	return resp, nil
}

func (s *Server) Accept(_ context.Context, req *pmv1.AcceptRequest) (*pmv1.AcceptResponse, error) {
	raw, err := proto.Marshal(req.Value)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad value: %v", err)
	}
	accepted, pb := s.node.Acceptor.Accept(req.Slot, paxos.Ballot(req.Ballot), raw)
	return &pmv1.AcceptResponse{Accepted: accepted, PromisedBallot: uint64(pb)}, nil
}

func (s *Server) Learn(_ context.Context, req *pmv1.LearnRequest) (*pmv1.LearnResponse, error) {
	raw, err := proto.Marshal(req.Value)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad value: %v", err)
	}
	s.node.Log.Commit(req.Slot, raw)
	return &pmv1.LearnResponse{}, nil
}

// --- transport: paxos messages over DirectoryService clients ---

type grpcTransport struct {
	self  string
	local *paxos.Node // self fast-path, set after NewNode
	peers map[string]string

	mu      sync.Mutex
	clients map[string]pmv1.DirectoryServiceClient
}

func (t *grpcTransport) client(peer string) (pmv1.DirectoryServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[peer]; ok {
		return c, nil
	}
	addr, ok := t.peers[peer]
	if !ok {
		return nil, fmt.Errorf("unknown replica %q", peer)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	if t.clients == nil {
		t.clients = map[string]pmv1.DirectoryServiceClient{}
	}
	c := pmv1.NewDirectoryServiceClient(conn)
	t.clients[peer] = c
	return c, nil
}

func (t *grpcTransport) Prepare(ctx context.Context, peer string, slot uint64, b paxos.Ballot) (paxos.PrepareReply, error) {
	if peer == t.self {
		promised, ab, av, pb := t.local.Acceptor.Prepare(slot, b)
		return paxos.PrepareReply{Promised: promised, AcceptedBallot: ab, AcceptedValue: av, PromisedBallot: pb}, nil
	}
	c, err := t.client(peer)
	if err != nil {
		return paxos.PrepareReply{}, err
	}
	resp, err := c.Prepare(ctx, &pmv1.PrepareRequest{Slot: slot, Ballot: uint64(b)})
	if err != nil {
		return paxos.PrepareReply{}, err
	}
	reply := paxos.PrepareReply{
		Promised:       resp.Promised,
		AcceptedBallot: paxos.Ballot(resp.AcceptedBallot),
		PromisedBallot: paxos.Ballot(resp.PromisedBallot),
	}
	if resp.AcceptedValue != nil {
		raw, err := proto.Marshal(resp.AcceptedValue)
		if err != nil {
			return paxos.PrepareReply{}, err
		}
		reply.AcceptedValue = raw
	}
	return reply, nil
}

func (t *grpcTransport) Accept(ctx context.Context, peer string, slot uint64, b paxos.Ballot, value []byte) (paxos.AcceptReply, error) {
	if peer == t.self {
		accepted, pb := t.local.Acceptor.Accept(slot, b, value)
		return paxos.AcceptReply{Accepted: accepted, PromisedBallot: pb}, nil
	}
	c, err := t.client(peer)
	if err != nil {
		return paxos.AcceptReply{}, err
	}
	var cmd pmv1.ClusterCommand
	if err := proto.Unmarshal(value, &cmd); err != nil {
		return paxos.AcceptReply{}, err
	}
	resp, err := c.Accept(ctx, &pmv1.AcceptRequest{Slot: slot, Ballot: uint64(b), Value: &cmd})
	if err != nil {
		return paxos.AcceptReply{}, err
	}
	return paxos.AcceptReply{Accepted: resp.Accepted, PromisedBallot: paxos.Ballot(resp.PromisedBallot)}, nil
}

func (t *grpcTransport) Learn(ctx context.Context, peer string, slot uint64, value []byte) error {
	if peer == t.self {
		t.local.Log.Commit(slot, value)
		return nil
	}
	c, err := t.client(peer)
	if err != nil {
		return err
	}
	var cmd pmv1.ClusterCommand
	if err := proto.Unmarshal(value, &cmd); err != nil {
		return err
	}
	_, err = c.Learn(ctx, &pmv1.LearnRequest{Slot: slot, Value: &cmd})
	return err
}
