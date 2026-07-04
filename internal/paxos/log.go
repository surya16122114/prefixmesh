package paxos

import (
	"bytes"
	"log/slog"
	"sync"
)

// Log is the learner: it records chosen values by slot and applies them to
// the state machine in strict slot order, holding back everything behind a
// gap until the gap is filled (see Node.fillGaps).
type Log struct {
	mu      sync.Mutex
	chosen  map[uint64][]byte
	applied uint64 // next slot to apply
	maxSeen uint64 // highest chosen slot + 1
	apply   func(slot uint64, value []byte)
}

// NewLog wires the apply callback. apply runs with the log's lock held, in
// slot order, exactly once per slot on this replica — keep it fast and
// non-blocking (the directory state machine hands watch notifications off to
// a separate goroutine for this reason).
func NewLog(apply func(slot uint64, value []byte)) *Log {
	return &Log{chosen: make(map[uint64][]byte), apply: apply}
}

// Commit records a chosen value and applies any newly contiguous prefix.
// Paxos guarantees one value per slot; a conflicting commit indicates a bug
// (or an acceptor restart violating promise durability) and is dropped
// loudly rather than applied.
func (l *Log) Commit(slot uint64, value []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prev, ok := l.chosen[slot]; ok {
		if !bytes.Equal(prev, value) {
			slog.Error("paxos: conflicting commit for slot dropped", "slot", slot)
		}
		return
	}
	l.chosen[slot] = value
	if slot+1 > l.maxSeen {
		l.maxSeen = slot + 1
	}
	for {
		v, ok := l.chosen[l.applied]
		if !ok {
			return
		}
		l.apply(l.applied, v)
		l.applied++
	}
}

// FirstUnchosen is the slot a proposer should target next.
func (l *Log) FirstUnchosen() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	for s := l.applied; ; s++ {
		if _, ok := l.chosen[s]; !ok {
			return s
		}
	}
}

// Gap reports a slot known-chosen further ahead than the first unchosen one:
// application is stalled until someone (fillGaps) resolves the hole.
func (l *Log) Gap() (slot uint64, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for s := l.applied; s < l.maxSeen; s++ {
		if _, chosen := l.chosen[s]; !chosen {
			return s, true
		}
	}
	return 0, false
}

func (l *Log) Applied() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.applied
}
