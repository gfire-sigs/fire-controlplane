package marena

import "sync/atomic"

const (
	// ARENA_MAX_ALLOC_SIZE defines the maximum allocatable size.
	ARENA_MAX_ALLOC_SIZE = 1<<31 - 1

	// ARENA_PAGESIZE is the fixed page size used for alignment.
	ARENA_PAGESIZE = 1 << 16

	// ARENA_INVALID_ADDRESS represents an invalid address.
	ARENA_INVALID_ADDRESS = 0

	// ARENA_MIN_ADDRESS is the minimal valid arena address.
	ARENA_MIN_ADDRESS = 128
)

/*
64-bit address format:
High 32 bits = offset in the arena,
Low 32 bits  = requested memory size.
*/

// NewArena creates a new Arena with a buffer aligned to ARENA_PAGESIZE.
// It ensures that the requested size does not exceed ARENA_MAX_ALLOC_SIZE.
func NewArena(size int64) *Arena {
	size = ((size + (ARENA_PAGESIZE - 1)) / ARENA_PAGESIZE) * ARENA_PAGESIZE
	if size > ARENA_MAX_ALLOC_SIZE {
		size = 1 << 31
	}

	return &Arena{
		buffer: make([]byte, size),
		size:   size,
		cursor: ARENA_MIN_ADDRESS, // allocate zero value
	}
}

// Arena represents a memory arena with a buffer, total size, and cursor offset.
type Arena struct {
	buffer []byte
	size   int64
	cursor int64
}

// align rounds n up to an 8-byte boundary.
func align(n int64) int64 {
	return ((n + 7) >> 3) << 3
}

// Allocate reserves a block of the specified size in the arena.
// Returns a 64-bit address (offset << 32 | size) or ARENA_INVALID_ADDRESS on failure.
func (g *Arena) Allocate(size int) uint64 {
	if uint64(size) > ARENA_MAX_ALLOC_SIZE {
		return ARENA_INVALID_ADDRESS
	}

	sizeAligned := align(int64(size))
	if atomic.LoadInt64(&g.cursor)+sizeAligned > g.size {
		return ARENA_INVALID_ADDRESS
	}

	indexEnd := atomic.AddInt64(&g.cursor, sizeAligned)
	indexStart := indexEnd - sizeAligned
	if indexEnd > g.size {
		return ARENA_INVALID_ADDRESS
	}

	return uint64(indexStart)<<32 | uint64(size)
}

// AllocateMultiple reserves multiple blocks in a single operation.
// isizes_oaddrs: pointers to the sizes; on success, overwritten with final addresses.
// Returns true if all allocations succeed, false otherwise.
func (g *Arena) AllocateMultiple(isizes_oaddrs ...*uint64) bool {
	var totalSize int64
	for _, isize_oaddr := range isizes_oaddrs {
		if *isize_oaddr > ARENA_MAX_ALLOC_SIZE {
			return false
		}
		totalSize += align(int64(*isize_oaddr))
	}

	if atomic.LoadInt64(&g.cursor)+totalSize > g.size {
		return false
	}

	indexEnd := atomic.AddInt64(&g.cursor, totalSize)
	indexStart := indexEnd - totalSize
	if indexEnd > g.size {
		return false
	}

	for _, isize_oaddr := range isizes_oaddrs {
		size := int64(*isize_oaddr)
		*isize_oaddr = uint64(indexStart)<<32 | uint64(size)
		indexStart += align(size)
	}

	return true
}

// View returns a slice of the arena's buffer corresponding to the given address.
// Returns nil if the address range is invalid.
func (g *Arena) View(address uint64) []byte {
	offset := address >> 32
	size := address & (1<<32 - 1)

	if offset < ARENA_MIN_ADDRESS || offset+size > uint64(g.size) {
		return nil
	}

	return g.buffer[offset : offset+size]
}

// Reset resets the Arena cursor to the minimal valid address.
func (g *Arena) Reset() {
	atomic.StoreInt64(&g.cursor, ARENA_MIN_ADDRESS)
}

// Remaining returns the number of bytes remaining in the Arena.
func (g *Arena) Remaining() int64 {
	// We load the cursor atomically for thread safety
	return g.size - atomic.LoadInt64(&g.cursor)
}

// Size extracts the size portion from a 64-bit address.
func Size(address uint64) int {
	return int(address & (1<<32 - 1))
}

// Offset extracts the offset portion from a 64-bit address.
func Offset(address uint64) int {
	return int(address >> 32)
}
