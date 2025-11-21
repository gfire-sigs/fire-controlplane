package dsst

import (
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/wyhash"
)

// FilterPolicy defines the interface for a filter policy (like Bloom filter).
type FilterPolicy interface {
	Name() string
	CreateFilter(keys [][]byte) []byte
	KeyMayMatch(key []byte, filter []byte) bool
}

// BloomFilterPolicy implements a Bloom filter.
type BloomFilterPolicy struct {
	bitsPerKey int
}

// NewBloomFilterPolicy creates a new Bloom filter policy.
func NewBloomFilterPolicy(bitsPerKey int) *BloomFilterPolicy {
	return &BloomFilterPolicy{
		bitsPerKey: bitsPerKey,
	}
}

func (p *BloomFilterPolicy) Name() string {
	return "sepia.BuiltinBloomFilter"
}

func (p *BloomFilterPolicy) CreateFilter(keys [][]byte) []byte {
	n := len(keys)
	bits := n * p.bitsPerKey
	if bits < 64 {
		bits = 64
	}
	bytes := (bits + 7) / 8
	bits = bytes * 8

	filter := make([]byte, bytes+1) // +1 for k (probes)

	// 0.69 * bitsPerKey is optimal k
	k := uint8(float64(p.bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}
	filter[bytes] = k

	for _, key := range keys {
		h := Hash(key)
		delta := (h >> 17) | (h << 15)
		for j := uint8(0); j < k; j++ {
			bitPos := h % uint32(bits)
			filter[bitPos/8] |= (1 << (bitPos % 8))
			h += delta
		}
	}
	return filter
}

func (p *BloomFilterPolicy) KeyMayMatch(key []byte, filter []byte) bool {
	if len(filter) < 2 {
		return false
	}
	k := filter[len(filter)-1]
	if k > 30 {
		return true // Reserved/Error, treat as match
	}

	bits := uint32(len(filter)-1) * 8
	h := Hash(key)
	delta := (h >> 17) | (h << 15)
	for j := uint8(0); j < k; j++ {
		bitPos := h % bits
		if (filter[bitPos/8] & (1 << (bitPos % 8))) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// Hash uses wyhash for the bloom filter.
func Hash(data []byte) uint32 {
	// WyHash returns uint64, we cast to uint32 for bloom filter usage.
	// Seed 0 is fine for this purpose.
	return uint32(wyhash.WyHash(data, 0))
}
