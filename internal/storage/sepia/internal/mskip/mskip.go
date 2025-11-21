// Package mskip implements a memory-optimized skiplist data structure using a custom arena allocator.
// The skiplist provides O(log n) average time complexity for insertions and searches.
package mskip

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/splitmix64"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/iterator"
)

const (
	// MSKIP_MAX_LEVEL defines the maximum height of the skiplist.
	// A higher value allows for faster searches in larger lists but uses more memory per node.
	MSKIP_MAX_LEVEL = 24
)

// Memory layout of mskipNode in arena (variable size based on level):
// - key_ptr: uint64    // Arena offset of key bytes
// - value_ptr: uint64  // Arena offset of value bytes
// - level: int32       // Node height (1 to MSKIP_MAX_LEVEL)
// - padding: 4 bytes   // Alignment padding
// - nexts: uint32[]   // Next pointers (only 'level' entries used)

type mskipNode struct {
	keyPtr   uint64                  // Arena offset of key
	valuePtr uint64                  // Arena offset of value
	level    int32                   // Node height
	nexts    [MSKIP_MAX_LEVEL]uint32 // Next pointers
}

// Memory layout constants for mskipNode struct.
// These are used to ensure consistent memory layout and for size calculations.
const (
	nodeKeyPtrOffset   = unsafe.Offsetof(mskipNode{}.keyPtr)   // Offset of keyPtr field (0 bytes)
	nodeValuePtrOffset = unsafe.Offsetof(mskipNode{}.valuePtr) // Offset of valuePtr field (8 bytes)
	nodeLevelOffset    = unsafe.Offsetof(mskipNode{}.level)    // Offset of level field (16 bytes)
	nodeNextsOffset    = unsafe.Offsetof(mskipNode{}.nexts)    // Offset of nexts array (20 bytes)
	nodeMaxSize        = unsafe.Sizeof(mskipNode{})            // Total size with max level nexts array
)

// Compile-time assertions to verify memory layout.
// These will fail at compile time if the memory layout changes unexpectedly.
var _ = [1]struct{}{}[nodeKeyPtrOffset-0]   // keyPtr must be at offset 0
var _ = [1]struct{}{}[nodeValuePtrOffset-8] // valuePtr must be at offset 8
var _ = [1]struct{}{}[nodeLevelOffset-16]   // level must be at offset 16
var _ = [1]struct{}{}[nodeNextsOffset-20]   // nexts must be at offset 20
var _ = [1]struct{}{}[nodeMaxSize-120]      // total size must be 120 bytes

// sizeNode calculates the actual size needed for a node with the given level.
// Since most nodes won't use all MSKIP_MAX_LEVEL next pointers, we can save memory
// by allocating only the space needed for the actual level count.
func sizeNode(nodeLevel int32) uintptr {
	return uintptr(nodeMaxSize - MSKIP_MAX_LEVEL*4 + uintptr(nodeLevel)*4)
}

// SkipList represents a memory-optimized skiplist data structure.
// It uses a custom arena allocator for all memory allocations and
// maintains keys and values as byte slices for flexibility.
type SkipList struct {
	arena *marena.Arena // Arena allocator for all memory management

	seed uint64 // Random seed for level generation
	head uint32 // Pointer to the head node in the arena

	compare func(key1, key2 []byte) int // Key comparison function
	// Returns: -1 if key1 < key2
	//
	//	0 if key1 == key2
	//	1 if key1 > key2

	refCount int64 // Number of active references to this skiplist
}

// IncRef increments the reference count of the skiplist atomically.
// Returns the new reference count after incrementing.
func (g *SkipList) IncRef() int64 {
	return atomic.AddInt64(&g.refCount, 1)
}

// DecRef decrements the reference count of the skiplist atomically.
// Returns the new reference count after decrementing.
func (g *SkipList) DecRef() int64 {
	return atomic.AddInt64(&g.refCount, -1)
}

// RefCount returns the current reference count of the skiplist.
// This is an atomic operation that returns the current value without modifying it.
func (g *SkipList) RefCount() int64 {
	return atomic.LoadInt64(&g.refCount)
}

// NewSkipList creates a new skiplist with the given arena allocator and comparison function.
// The seed parameter is used for randomizing node levels.
// Returns an error if the initial head node allocation fails.
func NewSkipList(arena *marena.Arena, compare func(key1, key2 []byte) int, seed uint64) (*SkipList, error) {
	g := &SkipList{
		arena:    arena,
		seed:     seed,
		head:     0,
		compare:  compare,
		refCount: 1,
	}

	// Create head node with maximum level
	headSize := sizeNode(MSKIP_MAX_LEVEL)
	headPtr := arena.Allocate(int(headSize))
	if headPtr == marena.ARENA_INVALID_ADDRESS {
		return nil, marena.ErrAllocationFailed
	}

	// Initialize head node with no key/value and invalid next pointers
	head := g.getNode(marena.Offset(headPtr))
	head.level = MSKIP_MAX_LEVEL
	head.keyPtr = marena.ARENA_INVALID_ADDRESS
	head.valuePtr = marena.ARENA_INVALID_ADDRESS
	for i := int32(0); i < MSKIP_MAX_LEVEL; i++ {
		head.nexts[i] = marena.ARENA_INVALID_ADDRESS
	}
	g.head = marena.Offset(headPtr)

	return g, nil
}

// randLevel generates a random level for a new node using a geometric distribution.
// This ensures a proper probabilistic balance in the skiplist structure where:
// - Level 1 has probability 1
// - Level 2 has probability 1/2
// - Level 3 has probability 1/4
// And so on...
func (g *SkipList) randLevel() int32 {
	level := int32(1)
	for level < MSKIP_MAX_LEVEL && splitmix64.Splitmix64(&g.seed)%2 == 0 {
		level++
	}
	return level
}

// getNode converts an arena offset into a pointer to a mskipNode.
// This is an unsafe operation that relies on the arena's memory management.
func (g *SkipList) getNode(ptr uint32) *mskipNode {
	return (*mskipNode)(unsafe.Pointer(g.arena.Index(ptr)))
}

// seeklt finds the largest node with key < target, recording search path.
// Used by insert operations to locate insertion point.
func (g *SkipList) seeklt(key []byte, log *[MSKIP_MAX_LEVEL]uint32) uint32 {
	ptr := g.head

	// Use dummy log if none provided
	var dummyLog [MSKIP_MAX_LEVEL]uint32
	if log == nil {
		log = &dummyLog
	}

	// Search from top to bottom, recording insertion path
	for i := MSKIP_MAX_LEVEL - 1; i >= 0; i-- {
		for {
			next := g.getNode(ptr).nexts[i]
			if next == marena.ARENA_INVALID_ADDRESS {
				break
			}
			if g.compare(key, g.arena.View(g.getNode(next).keyPtr)) <= 0 {
				break // Found key >= target
			}
			ptr = next // Move forward
		}
		log[i] = ptr // Record path for insertion
	}

	return ptr
}

// insertNext inserts key-value after nodes in log, updating if key exists.
func (g *SkipList) insertNext(log *[MSKIP_MAX_LEVEL]uint32, key []byte, value []byte) uint32 {
	// Check for existing key and update if found
	next := g.getNode(log[0]).nexts[0]
	if next != marena.ARENA_INVALID_ADDRESS && g.compare(key, g.arena.View(g.getNode(next).keyPtr)) == 0 {
		if value == nil {
			g.getNode(next).valuePtr = marena.ARENA_INVALID_ADDRESS // Delete
		} else {
			newValueAddr := g.arena.Allocate(len(value))
			if newValueAddr == marena.ARENA_INVALID_ADDRESS {
				return marena.ARENA_INVALID_ADDRESS
			}
			copy(g.arena.View(newValueAddr), value)
			g.getNode(next).valuePtr = newValueAddr
		}
		return next
	}

	// Create new node with random level
	level := g.randLevel()
	newNodeSize := uint64(sizeNode(level))
	newKeySize := uint64(len(key))

	// Allocate memory for node and key
	if !g.arena.AllocateMultiple(&newNodeSize, &newKeySize) {
		return marena.ARENA_INVALID_ADDRESS
	}

	// Initialize new node
	node := g.getNode(marena.Offset(newNodeSize))
	node.level = level
	node.keyPtr = newKeySize
	copy(g.arena.View(node.keyPtr), key)

	if value == nil {
		node.valuePtr = marena.ARENA_INVALID_ADDRESS
	} else {
		newValueAddr := g.arena.Allocate(len(value))
		if newValueAddr == marena.ARENA_INVALID_ADDRESS {
			return marena.ARENA_INVALID_ADDRESS
		}
		copy(g.arena.View(newValueAddr), value)
		node.valuePtr = newValueAddr
	}

	// Link node into skiplist at each level
	for i := int32(0); i < level; i++ {
		node.nexts[i] = g.getNode(log[i]).nexts[i]
		g.getNode(log[i]).nexts[i] = marena.Offset(newNodeSize)
	}

	return marena.Offset(newNodeSize)
}

// Insert adds a new key-value pair to the skiplist or updates an existing one.
// Returns true if the operation was successful, false if memory allocation failed.
// The key and value are stored as byte slices in the arena allocator.
func (g *SkipList) Insert(key []byte, value []byte) bool {
	var log [MSKIP_MAX_LEVEL]uint32
	g.seeklt(key, &log)
	res := g.insertNext(&log, key, value)
	if res == marena.ARENA_INVALID_ADDRESS {
		return false
	}
	return true
}

var iteratorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &SkipListIterator{}
	},
}

// SkipListIterator provides forward/backward iteration over skiplist entries.
type SkipListIterator struct {
	skl     *SkipList
	current uint32 // Current node offset
}

var _ iterator.Iterator = (*SkipListIterator)(nil)

// Iterator returns a new iterator for traversing the skiplist.
// Iterator must be positioned using First(), Seek(), etc. before use.
func (g *SkipList) Iterator() *SkipListIterator {
	g.IncRef()
	iter := iteratorPool.Get().(*SkipListIterator)
	iter.skl = g
	iter.current = marena.ARENA_INVALID_ADDRESS
	return iter
}

// First positions the iterator at the first key in the skiplist.
// After this call, the iterator is positioned at the smallest key
// if the skiplist is not empty.
func (g *SkipListIterator) First() {
	g.current = g.skl.head
	g.Next()
}

// SeekLT positions iterator at largest key < target.
func (g *SkipListIterator) SeekLT(key []byte) {
	var log [MSKIP_MAX_LEVEL]uint32
	g.current = g.skl.seeklt(key, &log)
}

// SeekLE positions iterator at largest key <= target.
func (g *SkipListIterator) SeekLE(key []byte) {
	var log [MSKIP_MAX_LEVEL]uint32
	prevNodePtr := g.skl.seeklt(key, &log)

	// Handle case where prev is head node
	if prevNodePtr == g.skl.head {
		firstNodePtr := g.skl.getNode(g.skl.head).nexts[0]
		if firstNodePtr != marena.ARENA_INVALID_ADDRESS {
			cmp := g.skl.compare(key, g.skl.arena.View(g.skl.getNode(firstNodePtr).keyPtr))
			if cmp == 0 {
				g.current = firstNodePtr
			} else {
				g.current = marena.ARENA_INVALID_ADDRESS
			}
		} else {
			g.current = marena.ARENA_INVALID_ADDRESS
		}
		return
	}

	// Check if next node matches exactly
	g.current = prevNodePtr
	nextNodePtr := g.skl.getNode(g.current).nexts[0]
	if nextNodePtr != marena.ARENA_INVALID_ADDRESS && g.skl.compare(key, g.skl.arena.View(g.skl.getNode(nextNodePtr).keyPtr)) == 0 {
		g.current = nextNodePtr
	}
}

// Valid returns true if the iterator is positioned at a valid node in the skiplist.
// Returns false if the iterator has moved past the end or is not initialized.
func (g *SkipListIterator) Valid() bool {
	return g.current != marena.ARENA_INVALID_ADDRESS
}

// Next advances the iterator to the next key in the skiplist.
// If the iterator is invalid or at the last key, it remains invalid.
func (g *SkipListIterator) Next() {
	if !g.Valid() {
		return
	}
	node := g.skl.getNode(g.current)
	g.current = node.nexts[0]
}

// Prev moves the iterator to the previous key in the skiplist.
// If the iterator is invalid or at the first key, it becomes invalid.
func (g *SkipListIterator) Prev() {
	if !g.Valid() {
		return
	}

	current := g.current
	last := g.skl.seeklt(g.skl.arena.View(g.skl.getNode(current).keyPtr), nil)
	if last == g.skl.head {
		g.current = marena.ARENA_INVALID_ADDRESS
		return
	}
	g.current = last
}

// Key returns the key at the current iterator position.
// Returns nil if the iterator is not valid.
func (g *SkipListIterator) Key() []byte {
	if !g.Valid() {
		return nil
	}

	node := g.skl.getNode(g.current)
	return g.skl.arena.View(node.keyPtr)
}

// Value returns the value associated with the key at the current iterator position.
// Returns nil if the iterator is not valid.
func (g *SkipListIterator) Value() []byte {
	if !g.Valid() {
		return nil
	}

	node := g.skl.getNode(g.current)
	if node.valuePtr == marena.ARENA_INVALID_ADDRESS {
		return nil
	}
	return g.skl.arena.View(node.valuePtr)
}

// Seek moves the iterator to the first key that is greater than or equal to the given key.
func (g *SkipListIterator) Seek(key []byte) {
	g.SeekLE(key)
	if !g.Valid() {
		g.First()
	} else if g.skl.compare(g.Key(), key) < 0 {
		g.Next()
	}
}

// Close releases the iterator's resources and returns it to the pool.
// The iterator becomes invalid after this call and must not be used.
// This method decrements the reference count of the associated skiplist.
func (g *SkipListIterator) Close() {
	if g == nil {
		return
	}
	g.skl.DecRef()
	g.skl = nil
	g.current = marena.ARENA_INVALID_ADDRESS
	iteratorPool.Put(g)
}
