// Package paxos implements multi-decree Paxos over an opaque []byte value
// type (DESIGN.md §4.3). Each replica colocates the three roles: an Acceptor
// (per-slot promise/accept state), a Log (chosen values, applied in slot
// order), and a proposer (Node.Submit).
//
// The design is a Go port of distributed-kv-store's PaxosCoordinator, kept
// deliberately classic: no leader leases, no batching — correctness first,
// with liveness helped by per-replica round jitter. Acceptor state is
// in-memory; replica restart durability is an accepted M1 limitation (a
// restarted acceptor may re-promise a lower ballot). Documented, not hidden.
package paxos

import "sync"

// Ballot orders proposals globally: (round << 8) | replicaIndex, so rounds
// dominate and replica index breaks ties deterministically.
type Ballot uint64

func MakeBallot(round uint64, replicaIdx uint8) Ballot {
	return Ballot(round<<8 | uint64(replicaIdx))
}

func (b Ballot) Round() uint64 { return uint64(b) >> 8 }

type acceptorSlot struct {
	promised       Ballot
	acceptedBallot Ballot
	acceptedValue  []byte // nil = nothing accepted
}

// Acceptor holds per-slot promise/accept state. Safe for concurrent use.
type Acceptor struct {
	mu    sync.Mutex
	slots map[uint64]*acceptorSlot
}

func NewAcceptor() *Acceptor {
	return &Acceptor{slots: make(map[uint64]*acceptorSlot)}
}

func (a *Acceptor) slot(s uint64) *acceptorSlot {
	sl, ok := a.slots[s]
	if !ok {
		sl = &acceptorSlot{}
		a.slots[s] = sl
	}
	return sl
}

// Prepare answers phase 1: promise iff b is strictly newer than any promise.
func (a *Acceptor) Prepare(slot uint64, b Ballot) (promised bool, acceptedBallot Ballot, acceptedValue []byte, promisedBallot Ballot) {
	a.mu.Lock()
	defer a.mu.Unlock()
	sl := a.slot(slot)
	if b > sl.promised {
		sl.promised = b
		return true, sl.acceptedBallot, sl.acceptedValue, b
	}
	return false, sl.acceptedBallot, sl.acceptedValue, sl.promised
}

// Accept answers phase 2: accept iff we haven't promised a newer ballot.
func (a *Acceptor) Accept(slot uint64, b Ballot, v []byte) (accepted bool, promisedBallot Ballot) {
	a.mu.Lock()
	defer a.mu.Unlock()
	sl := a.slot(slot)
	if b >= sl.promised {
		sl.promised = b
		sl.acceptedBallot = b
		sl.acceptedValue = v
		return true, b
	}
	return false, sl.promised
}
