package paxos

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// simNet is an in-memory transport with configurable message loss and random
// delivery delay — the harness DESIGN.md §4.3 promises Paxos is tested under.
type simNet struct {
	mu    sync.Mutex
	nodes map[string]*Node
	drop  float64 // per-message drop probability
	maxDelay time.Duration
	rng   *rand.Rand
}

func newSimNet(drop float64, maxDelay time.Duration) *simNet {
	return &simNet{
		nodes:    map[string]*Node{},
		drop:     drop,
		maxDelay: maxDelay,
		rng:      rand.New(rand.NewSource(1)),
	}
}

var errDropped = errors.New("simnet: dropped")

// deliver simulates the network: maybe drop, maybe delay (reordering falls
// out of independent random delays).
func (s *simNet) deliver() error {
	s.mu.Lock()
	drop := s.rng.Float64() < s.drop
	delay := time.Duration(s.rng.Int63n(int64(s.maxDelay) + 1))
	s.mu.Unlock()
	if drop {
		return errDropped
	}
	time.Sleep(delay)
	return nil
}

func (s *simNet) node(peer string) *Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodes[peer]
}

func (s *simNet) Prepare(ctx context.Context, peer string, slot uint64, b Ballot) (PrepareReply, error) {
	if err := s.deliver(); err != nil {
		return PrepareReply{}, err
	}
	n := s.node(peer)
	if n == nil {
		return PrepareReply{}, errDropped // partitioned/removed replica
	}
	promised, ab, av, pb := n.Acceptor.Prepare(slot, b)
	if err := s.deliver(); err != nil { // response can be lost too
		return PrepareReply{}, err
	}
	return PrepareReply{Promised: promised, AcceptedBallot: ab, AcceptedValue: av, PromisedBallot: pb}, nil
}

func (s *simNet) Accept(ctx context.Context, peer string, slot uint64, b Ballot, value []byte) (AcceptReply, error) {
	if err := s.deliver(); err != nil {
		return AcceptReply{}, err
	}
	n := s.node(peer)
	if n == nil {
		return AcceptReply{}, errDropped
	}
	accepted, pb := n.Acceptor.Accept(slot, b, value)
	if err := s.deliver(); err != nil {
		return AcceptReply{}, err
	}
	return AcceptReply{Accepted: accepted, PromisedBallot: pb}, nil
}

func (s *simNet) Learn(ctx context.Context, peer string, slot uint64, value []byte) error {
	if err := s.deliver(); err != nil {
		return err
	}
	n := s.node(peer)
	if n == nil {
		return errDropped
	}
	n.Log.Commit(slot, value)
	return nil
}

type applied struct {
	mu   sync.Mutex
	seqs map[string][]string // replica -> applied values in order
}

func buildCluster(t *testing.T, n int, drop float64) (*simNet, []*Node, *applied) {
	t.Helper()
	net := newSimNet(drop, 2*time.Millisecond)
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("r%d", i)
	}
	state := &applied{seqs: map[string][]string{}}
	nodes := make([]*Node, n)
	for i, id := range ids {
		id := id
		log := NewLog(func(slot uint64, v []byte) {
			state.mu.Lock()
			state.seqs[id] = append(state.seqs[id], string(v))
			state.mu.Unlock()
		})
		nodes[i] = NewNode(Config{
			Self:       id,
			Replicas:   ids,
			RPCTimeout: 100 * time.Millisecond,
			Noop:       []byte("noop"),
		}, net, log)
		net.mu.Lock()
		net.nodes[id] = nodes[i]
		net.mu.Unlock()
	}
	return net, nodes, state
}

// mustSubmit retries until consensus; duplicates on retry are allowed by the
// protocol (Submit's contract) and tolerated by the assertions.
func mustSubmit(t *testing.T, n *Node, v string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if err := n.Submit(ctx, []byte(v)); err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("submit %q never succeeded", v)
		}
	}
}

// TestSafetyUnderContention: three replicas propose concurrently over a lossy
// network. THE Paxos property: every replica applies the same sequence.
func TestSafetyUnderContention(t *testing.T) {
	_, nodes, state := buildCluster(t, 3, 0.15)

	const perNode = 10
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n *Node) {
			defer wg.Done()
			for j := 0; j < perNode; j++ {
				mustSubmit(t, n, fmt.Sprintf("cmd-%d-%d", i, j))
			}
		}(i, n)
	}
	wg.Wait()

	// Let stragglers learn via gap fillers.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, n := range nodes {
		go n.RunGapFiller(ctx, 50*time.Millisecond)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if seqsConverged(state, 3) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()

	state.mu.Lock()
	defer state.mu.Unlock()
	// Safety: applied sequences must be prefixes of each other (identical up
	// to the shortest — a replica may simply not have learned the tail yet).
	var shortest []string
	for _, seq := range state.seqs {
		if shortest == nil || len(seq) < len(shortest) {
			shortest = seq
		}
	}
	for id, seq := range state.seqs {
		for i := range shortest {
			if seq[i] != shortest[i] {
				t.Fatalf("replica %s diverges at slot %d: %q vs %q", id, i, seq[i], shortest[i])
			}
		}
	}
	// Liveness/completeness: every submitted command appears at least once
	// in the longest sequence (duplicates from retries are legal).
	var longest []string
	for _, seq := range state.seqs {
		if len(seq) > len(longest) {
			longest = seq
		}
	}
	seen := map[string]bool{}
	for _, v := range longest {
		seen[v] = true
	}
	for i := 0; i < 3; i++ {
		for j := 0; j < perNode; j++ {
			cmd := fmt.Sprintf("cmd-%d-%d", i, j)
			if !seen[cmd] {
				t.Errorf("%s was chosen (Submit returned) but never applied", cmd)
			}
		}
	}
}

func seqsConverged(state *applied, n int) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.seqs) < n {
		return false
	}
	var l = -1
	for _, seq := range state.seqs {
		if l == -1 {
			l = len(seq)
		}
		if len(seq) != l {
			return false
		}
	}
	return true
}

// TestMinorityCannotChoose: with 2 of 3 replicas unreachable, Submit must
// fail rather than "choose" anything.
func TestMinorityCannotChoose(t *testing.T) {
	net, nodes, _ := buildCluster(t, 3, 0)
	net.mu.Lock()
	delete(net.nodes, "r1")
	delete(net.nodes, "r2")
	net.mu.Unlock()

	// Unreachable peers panic on lookup; guard the transport instead.
	// (A nil node dereference would be a test bug, not a protocol result.)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := nodes[0].Submit(ctx, []byte("must-not-commit"))
	if err == nil {
		t.Fatal("submit succeeded without a quorum")
	}
	if nodes[0].Log.Applied() != 0 {
		t.Fatal("value applied without a quorum")
	}
}

// TestAdoptsPreviouslyAccepted: a value accepted by a majority must survive
// a competing proposer — the essence of phase 1.
func TestAdoptsPreviouslyAccepted(t *testing.T) {
	_, nodes, state := buildCluster(t, 3, 0)

	// r0 gets "first" chosen at slot 0.
	mustSubmit(t, nodes[0], "first")
	// r1, which may not have learned slot 0, proposes "second". It must land
	// in a later slot, never overwrite slot 0.
	mustSubmit(t, nodes[1], "second")

	state.mu.Lock()
	defer state.mu.Unlock()
	seq := state.seqs["r0"]
	if len(seq) == 0 || seq[0] != "first" {
		t.Fatalf("slot 0 = %v, want first", seq)
	}
}
