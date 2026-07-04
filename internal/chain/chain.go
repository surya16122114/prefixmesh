// Package chain implements PrefixMesh's content addressing: prompts are split
// into fixed-size token blocks whose IDs form a hash chain, so a block's
// identity encodes its entire prefix (DESIGN.md §3.1).
package chain

import (
	"crypto/sha256"
	"encoding/binary"
)

const HashSize = sha256.Size

type BlockID = [HashSize]byte

// Root derives h_0, the chain root. Chains for different models or block sizes
// never collide.
func Root(modelID string, blockSize int) BlockID {
	h := sha256.New()
	h.Write([]byte(modelID))
	var bs [4]byte
	binary.BigEndian.PutUint32(bs[:], uint32(blockSize))
	h.Write(bs[:])
	return BlockID(h.Sum(nil))
}

// Next computes h_i = SHA-256(h_{i-1} || tokens_i).
func Next(parent BlockID, tokens []uint32) BlockID {
	h := sha256.New()
	h.Write(parent[:])
	var b [4]byte
	for _, t := range tokens {
		binary.BigEndian.PutUint32(b[:], t)
		h.Write(b[:])
	}
	return BlockID(h.Sum(nil))
}

// Chunk splits token IDs into blockSize-sized blocks; the final block may be
// shorter. An empty prompt yields no blocks.
func Chunk(tokens []uint32, blockSize int) [][]uint32 {
	if blockSize <= 0 {
		panic("chain: blockSize must be positive")
	}
	var out [][]uint32
	for start := 0; start < len(tokens); start += blockSize {
		end := min(start+blockSize, len(tokens))
		out = append(out, tokens[start:end])
	}
	return out
}

// Build computes the full chain h_1..h_n for a prompt.
func Build(modelID string, blockSize int, tokens []uint32) []BlockID {
	parent := Root(modelID, blockSize)
	blocks := Chunk(tokens, blockSize)
	ids := make([]BlockID, len(blocks))
	for i, blk := range blocks {
		parent = Next(parent, blk)
		ids[i] = parent
	}
	return ids
}
