package hashring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"
)

func testNodes(n int) map[string]string {
	m := make(map[string]string, n)
	for i := 0; i < n; i++ {
		m[fmt.Sprintf("node-%d", i)] = fmt.Sprintf("10.0.0.%d:7100", i)
	}
	return m
}

func key(i int) []byte {
	h := sha256.Sum256(binary.BigEndian.AppendUint64(nil, uint64(i)))
	return h[:]
}

func TestDeterministicOwnership(t *testing.T) {
	r1 := New(1, testNodes(5), 0)
	r2 := New(1, testNodes(5), 0)
	for i := 0; i < 1000; i++ {
		a, _, _ := r1.Owner(key(i))
		b, _, _ := r2.Owner(key(i))
		if a != b {
			t.Fatalf("key %d: %s vs %s", i, a, b)
		}
	}
}

func TestBalance(t *testing.T) {
	const nodes, keys = 8, 100_000
	r := New(1, testNodes(nodes), 0)
	counts := map[string]int{}
	for i := 0; i < keys; i++ {
		id, _, _ := r.Owner(key(i))
		counts[id]++
	}
	mean := keys / nodes
	for id, c := range counts {
		if c < mean*70/100 || c > mean*130/100 {
			t.Errorf("%s owns %d keys, want within ±30%% of %d", id, c, mean)
		}
	}
}

func TestMinimalMovementOnJoin(t *testing.T) {
	const keys = 50_000
	before := New(1, testNodes(8), 0)
	after8 := testNodes(9) // adds node-8
	afterRing := New(2, after8, 0)

	moved := 0
	for i := 0; i < keys; i++ {
		a, _, _ := before.Owner(key(i))
		b, _, _ := afterRing.Owner(key(i))
		if a != b {
			if b != "node-8" {
				t.Fatalf("key %d moved %s->%s, not to the new node", i, a, b)
			}
			moved++
		}
	}
	// Expect ~1/9 of the keyspace to move; fail if wildly off.
	frac := float64(moved) / keys
	if frac < 0.05 || frac > 0.20 {
		t.Errorf("join moved %.1f%% of keys, want ~11%%", frac*100)
	}
}

func TestEmptyRing(t *testing.T) {
	r := New(1, nil, 0)
	if _, _, ok := r.Owner(key(1)); ok {
		t.Fatal("empty ring must report !ok")
	}
}
