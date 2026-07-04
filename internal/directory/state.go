// Package directory implements the control plane (DESIGN.md §4.3): replicas
// agree on a log of ClusterCommands via internal/paxos; the applied log
// yields the authoritative epoch-numbered ring, streamed to the fleet.
package directory

import (
	"log/slog"
	"sync"

	"google.golang.org/protobuf/proto"

	pmv1 "github.com/surya16122114/prefixmesh/gen/prefixmesh/v1"
	"github.com/surya16122114/prefixmesh/internal/hashring"
)

// StateMachine applies committed ClusterCommands in log order. Every
// membership change bumps the epoch; the ring is a pure function of
// (epoch, members), so all replicas derive identical rings.
type StateMachine struct {
	mu      sync.Mutex
	epoch   uint64
	members map[string]*pmv1.NodeInfo
	watch   map[chan struct{}]struct{} // signal-only; watchers re-read Ring()
	onJoin  func(nodeID string)        // heartbeat-grace seeding hook
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		members: map[string]*pmv1.NodeInfo{},
		watch:   map[chan struct{}]struct{}{},
	}
}

// Apply is the paxos.Log callback. It runs in slot order under the log lock:
// keep it non-blocking — watcher notification is a non-blocking signal send.
func (sm *StateMachine) Apply(slot uint64, raw []byte) {
	var cmd pmv1.ClusterCommand
	if err := proto.Unmarshal(raw, &cmd); err != nil {
		slog.Error("directory: undecodable command skipped", "slot", slot, "err", err)
		return
	}
	sm.mu.Lock()
	changed := false
	switch c := cmd.Cmd.(type) {
	case *pmv1.ClusterCommand_NodeJoin:
		prev, ok := sm.members[c.NodeJoin.NodeId]
		if !ok || prev.Addr != c.NodeJoin.Addr {
			sm.members[c.NodeJoin.NodeId] = c.NodeJoin
			changed = true
			if sm.onJoin != nil {
				sm.onJoin(c.NodeJoin.NodeId)
			}
		}
	case *pmv1.ClusterCommand_NodeLeave:
		if _, ok := sm.members[c.NodeLeave]; ok {
			delete(sm.members, c.NodeLeave)
			changed = true
		}
	case *pmv1.ClusterCommand_NodeDead:
		if _, ok := sm.members[c.NodeDead]; ok {
			delete(sm.members, c.NodeDead)
			changed = true
			slog.Info("directory: member declared dead", "node", c.NodeDead, "slot", slot)
		}
	case *pmv1.ClusterCommand_Noop:
		// gap filler; nothing to do
	default:
		// lease commands are M2 scope; agreeing on them is harmless
	}
	if changed {
		sm.epoch++
	}
	watchers := make([]chan struct{}, 0, len(sm.watch))
	for ch := range sm.watch {
		watchers = append(watchers, ch)
	}
	sm.mu.Unlock()

	if changed {
		for _, ch := range watchers {
			select {
			case ch <- struct{}{}:
			default: // watcher already has a pending signal; it re-reads state
			}
		}
	}
}

// Ring snapshots the current epoch's ring as a proto message.
func (sm *StateMachine) Ring() *pmv1.Ring {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	nodes := make([]*pmv1.NodeInfo, 0, len(sm.members))
	for _, n := range sm.members {
		nodes = append(nodes, n)
	}
	return &pmv1.Ring{
		Epoch:         sm.epoch,
		Nodes:         nodes,
		VnodesPerNode: hashring.DefaultVNodes,
	}
}

func (sm *StateMachine) HasMember(nodeID string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.members[nodeID]
	return ok
}

func (sm *StateMachine) Members() []string {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ids := make([]string, 0, len(sm.members))
	for id := range sm.members {
		ids = append(ids, id)
	}
	return ids
}

// Subscribe registers a change signal; the caller re-reads Ring() on each
// signal and must Unsubscribe when done.
func (sm *StateMachine) Subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	sm.mu.Lock()
	sm.watch[ch] = struct{}{}
	sm.mu.Unlock()
	return ch
}

func (sm *StateMachine) Unsubscribe(ch chan struct{}) {
	sm.mu.Lock()
	delete(sm.watch, ch)
	sm.mu.Unlock()
}
