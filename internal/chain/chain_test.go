package chain

import (
	"testing"
)

func tokens(n int, seed uint32) []uint32 {
	ts := make([]uint32, n)
	for i := range ts {
		ts[i] = seed + uint32(i)
	}
	return ts
}

func TestSharedPrefixSharesChain(t *testing.T) {
	// Two prompts with identical first 256 tokens, then divergence.
	shared := tokens(256, 1)
	a := append(append([]uint32{}, shared...), tokens(128, 1000)...)
	b := append(append([]uint32{}, shared...), tokens(128, 2000)...)

	ca := Build("m", 128, a)
	cb := Build("m", 128, b)

	if len(ca) != 3 || len(cb) != 3 {
		t.Fatalf("expected 3 blocks each, got %d and %d", len(ca), len(cb))
	}
	for i := 0; i < 2; i++ {
		if ca[i] != cb[i] {
			t.Fatalf("shared prefix block %d differs", i)
		}
	}
	if ca[2] == cb[2] {
		t.Fatal("divergent block 2 should differ")
	}
}

func TestDivergencePropagates(t *testing.T) {
	// Differ in the FIRST block: every subsequent ID must differ even though
	// later token blocks are identical.
	tail := tokens(256, 500)
	a := append([]uint32{1}, tail...)
	b := append([]uint32{2}, tail...)

	ca := Build("m", 128, a)
	cb := Build("m", 128, b)
	for i := range ca {
		if ca[i] == cb[i] {
			t.Fatalf("block %d identical after early divergence", i)
		}
	}
}

func TestNamespacing(t *testing.T) {
	ts := tokens(128, 1)
	if Build("model-a", 128, ts)[0] == Build("model-b", 128, ts)[0] {
		t.Fatal("different models must not share chains")
	}
	if Build("m", 128, ts)[0] == Build("m", 64, ts)[0] {
		t.Fatal("different block sizes must not share chains")
	}
}

func TestChunkTail(t *testing.T) {
	got := Chunk(tokens(300, 0), 128)
	if len(got) != 3 || len(got[0]) != 128 || len(got[2]) != 44 {
		t.Fatalf("bad chunking: %d blocks, tail %d", len(got), len(got[len(got)-1]))
	}
	if Chunk(nil, 128) != nil {
		t.Fatal("empty prompt should yield no blocks")
	}
}

func TestDeterminism(t *testing.T) {
	ts := tokens(1000, 42)
	c1 := Build("m", 128, ts)
	c2 := Build("m", 128, ts)
	for i := range c1 {
		if c1[i] != c2[i] {
			t.Fatal("chain not deterministic")
		}
	}
}
