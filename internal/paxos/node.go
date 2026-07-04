package paxos

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Transport carries Paxos messages to one peer. Implementations route
// peer == self to the local Acceptor/Log without a network hop.
type Transport interface {
	Prepare(ctx context.Context, peer string, slot uint64, b Ballot) (PrepareReply, error)
	Accept(ctx context.Context, peer string, slot uint64, b Ballot, value []byte) (AcceptReply, error)
	Learn(ctx context.Context, peer string, slot uint64, value []byte) error
}

type PrepareReply struct {
	Promised       bool
	AcceptedBallot Ballot
	AcceptedValue  []byte
	PromisedBallot Ballot
}

type AcceptReply struct {
	Accepted       bool
	PromisedBallot Ballot
}

type Config struct {
	Self     string   // this replica's ID
	Replicas []string // all replica IDs including Self
	// Equal compares two candidate values; defaults to bytes.Equal. The
	// directory injects proto.Equal since protobuf encoding is not canonical
	// across a wire round-trip.
	Equal      func(a, b []byte) bool
	RPCTimeout time.Duration // per-message timeout, default 500ms
	Noop       []byte        // value proposed to fill log gaps
}

// Node is one Paxos replica: local acceptor + log + proposer.
type Node struct {
	cfg      Config
	idx      uint8
	Acceptor *Acceptor
	Log      *Log
	tr       Transport
	round    atomic.Uint64
	// submitMu serializes local proposals; concurrent proposals from one
	// replica would just fight each other for the same slot.
	submitMu sync.Mutex
}

var ErrSubmitFailed = errors.New("paxos: submit did not achieve consensus")

func NewNode(cfg Config, tr Transport, log *Log) *Node {
	if cfg.Equal == nil {
		cfg.Equal = bytes.Equal
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = 500 * time.Millisecond
	}
	sorted := append([]string{}, cfg.Replicas...)
	sort.Strings(sorted)
	idx := -1
	for i, id := range sorted {
		if id == cfg.Self {
			idx = i
		}
	}
	if idx < 0 {
		panic("paxos: Self not in Replicas")
	}
	return &Node{
		cfg:      cfg,
		idx:      uint8(idx),
		Acceptor: NewAcceptor(),
		Log:      log,
		tr:       tr,
	}
}

func (n *Node) majority() int { return len(n.cfg.Replicas)/2 + 1 }

// Submit drives value through consensus. It returns once the value is chosen
// in some slot and committed locally. If another proposer's value wins the
// targeted slot, Submit completes that slot (helping liveness) and retries
// on the next one.
//
// A false negative is possible: an error return does not prove the value was
// NOT chosen (our accepts may have landed after we gave up). Callers submit
// idempotent commands (DESIGN.md §4.3), so retrying on error is safe.
func (n *Node) Submit(ctx context.Context, value []byte) error {
	n.submitMu.Lock()
	defer n.submitMu.Unlock()

	for attempt := 0; attempt < 20; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		slot := n.Log.FirstUnchosen()
		chosen, err := n.runSlot(ctx, slot, value)
		if err != nil {
			n.backoff(ctx, attempt)
			continue
		}
		if n.cfg.Equal(chosen, value) {
			return nil
		}
		// Someone else's value won this slot; move on to the next.
	}
	return ErrSubmitFailed
}

// runSlot runs one full Prepare/Accept/Learn round for a slot and returns
// the value that was chosen there (ours or a recovered one).
func (n *Node) runSlot(ctx context.Context, slot uint64, value []byte) ([]byte, error) {
	ballot := MakeBallot(n.round.Add(1), n.idx)

	// Phase 1: collect promises; adopt the highest already-accepted value.
	type prepResult struct {
		reply PrepareReply
		err   error
	}
	prepCh := make(chan prepResult, len(n.cfg.Replicas))
	for _, peer := range n.cfg.Replicas {
		go func(peer string) {
			cctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
			defer cancel()
			r, err := n.tr.Prepare(cctx, peer, slot, ballot)
			prepCh <- prepResult{r, err}
		}(peer)
	}
	promises := 0
	var adopted []byte
	var adoptedBallot Ballot
	var maxPromised Ballot
	for range n.cfg.Replicas {
		res := <-prepCh
		if res.err != nil {
			continue
		}
		if res.reply.Promised {
			promises++
			if res.reply.AcceptedValue != nil && res.reply.AcceptedBallot >= adoptedBallot {
				adopted = res.reply.AcceptedValue
				adoptedBallot = res.reply.AcceptedBallot
			}
		} else if res.reply.PromisedBallot > maxPromised {
			maxPromised = res.reply.PromisedBallot
		}
	}
	if promises < n.majority() {
		n.catchUpRound(maxPromised)
		return nil, ErrSubmitFailed
	}
	proposal := value
	if adopted != nil {
		proposal = adopted
	}

	// Phase 2: accepts.
	type accResult struct {
		reply AcceptReply
		err   error
	}
	accCh := make(chan accResult, len(n.cfg.Replicas))
	for _, peer := range n.cfg.Replicas {
		go func(peer string) {
			cctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
			defer cancel()
			r, err := n.tr.Accept(cctx, peer, slot, ballot, proposal)
			accCh <- accResult{r, err}
		}(peer)
	}
	accepts := 0
	maxPromised = 0
	for range n.cfg.Replicas {
		res := <-accCh
		if res.err != nil {
			continue
		}
		if res.reply.Accepted {
			accepts++
		} else if res.reply.PromisedBallot > maxPromised {
			maxPromised = res.reply.PromisedBallot
		}
	}
	if accepts < n.majority() {
		n.catchUpRound(maxPromised)
		return nil, ErrSubmitFailed
	}

	// Chosen. Learn is best-effort fan-out; the local commit is what the
	// caller depends on, stragglers recover via fillGaps.
	for _, peer := range n.cfg.Replicas {
		go func(peer string) {
			cctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			_ = n.tr.Learn(cctx, peer, slot, proposal)
		}(peer)
	}
	n.Log.Commit(slot, proposal)
	return proposal, nil
}

// catchUpRound fast-forwards our round past a competitor's so the next
// attempt doesn't lose the same way twice.
func (n *Node) catchUpRound(promised Ballot) {
	target := promised.Round()
	for {
		cur := n.round.Load()
		if cur >= target || n.round.CompareAndSwap(cur, target) {
			return
		}
	}
}

func (n *Node) backoff(ctx context.Context, attempt int) {
	base := 10 * time.Millisecond << min(attempt, 5)
	jitter := time.Duration(rand.Int63n(int64(base)))
	t := time.NewTimer(base + jitter)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// RunGapFiller periodically proposes the configured no-op at stalled log
// gaps: phase 1 either recovers the half-chosen command stuck there or burns
// the slot, letting application proceed.
func (n *Node) RunGapFiller(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, stalled := n.Log.Gap(); stalled {
				_ = n.Submit(ctx, n.cfg.Noop)
			}
		}
	}
}
