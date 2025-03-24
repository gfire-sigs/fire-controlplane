// Package mskip implements a memory-optimized skiplist data structure using a custom arena allocator.
// The skiplist provides O(log n) average time complexity for insertions and searches.
package mskip

import (
	"unsafe"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/splitmix64"
)

const (
	// MSKIP_MAX_LEVEL defines the maximum height of the skiplist.
	// A higher value allows for faster searches in larger lists but uses more memory per node.
	MSKIP_MAX_LEVEL = 24
)

// Memory layout of mskipNode in arena:
//
// key_ptr: uint64      // Pointer to key bytes in arena
// value_ptr: uint64    // Pointer to value bytes in arena
// level: int32        // Current level of the node (1 to MSKIP_MAX_LEVEL)
// reserved: 4 bytes   // Padding for alignment
// nexts: uint32[level] // Array of pointers to next nodes at each level

// mskipNode represents a node in the skiplist.
// The actual size allocated depends on the node's level.
type mskipNode struct {
	keyPtr   uint64                  // Arena offset of the key bytes
	valuePtr uint64                  // Arena offset of the value bytes
	level    int32                   // Height of this node
	nexts    [MSKIP_MAX_LEVEL]uint32 // Next pointers at each level (only level entries are used)
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
}

// NewSkipList creates a new skiplist with the given arena allocator and comparison function.
// The seed parameter is used for randomizing node levels.
// Returns an error if the initial head node allocation fails.
func NewSkipList(arena *marena.Arena, compare func(key1, key2 []byte) int, seed uint64) (*SkipList, error) {
	g := &SkipList{
		arena:   arena,
		seed:    seed,
		head:    0,
		compare: compare,
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

// seeklt finds the node with the largest key less than the given key.
// It traverses the skiplist from top to bottom, recording the path if log is provided.
// The path recording is essential for insertion operations.
//
// Parameters:
//   - key: The key to search for
//   - log: Optional array to record the search path (required for insertions)
//
// Returns the arena offset of the found node.
func (g *SkipList) seeklt(key []byte, log *[MSKIP_MAX_LEVEL]uint32) uint32 {
	ptr := g.head

	// Create dummy log if none provided
	var dummyLog [MSKIP_MAX_LEVEL]uint32
	if log == nil {
		log = &dummyLog
	}

	// Search from top level to bottom, recording path
	for i := MSKIP_MAX_LEVEL - 1; i >= 0; i-- {
		for {
			next := g.getNode(ptr).nexts[i]
			if next == marena.ARENA_INVALID_ADDRESS {
				break // No more nodes at this level
			}
			if g.compare(key, g.arena.View(g.getNode(next).keyPtr)) <= 0 {
				break // Found a key >= target key
			}

			ptr = next // Move forward at current level
		}
		log[i] = ptr // Record path at this level
	}

	return ptr
}

// insertNext inserts a new key-value pair into the skiplist after the nodes specified in log.
// If the key already exists, it updates the value and returns the existing node.
//
// Parameters:
//   - log: Array containing the path to the insertion point (from seeklt)
//   - key: Key to insert
//   - value: Value to associate with the key
//
// Returns:
//   - The arena offset of the inserted/updated node
//   - ARENA_INVALID_ADDRESS if memory allocation fails
func (g *SkipList) insertNext(log *[MSKIP_MAX_LEVEL]uint32, key []byte, value []byte) uint32 {
	// Check if key already exists
	next := g.getNode(log[0]).nexts[0]
	if next != marena.ARENA_INVALID_ADDRESS && g.compare(key, g.arena.View(g.getNode(next).keyPtr)) == 0 {
		// Key exists - update value
		newValue := g.arena.Allocate(len(value))
		if newValue == marena.ARENA_INVALID_ADDRESS {
			return marena.ARENA_INVALID_ADDRESS
		}
		copy(g.arena.View(newValue), value)
		g.getNode(next).valuePtr = newValue
		return next
	}

	// Generate random level and allocate memory for new node
	level := g.randLevel()
	var newNode, newValue, newKey uint64 = uint64(sizeNode(level)), uint64(len(value)), uint64(len(key))
	if !g.arena.AllocateMultiple(&newNode, &newValue, &newKey) {
		return marena.ARENA_INVALID_ADDRESS
	}

	// Initialize new node
	node := g.getNode(marena.Offset(newNode))
	node.level = level
	node.keyPtr = newKey
	node.valuePtr = newValue
	copy(g.arena.View(node.keyPtr), key)
	copy(g.arena.View(node.valuePtr), value)

	// Update next pointers at each level
	for i := int32(0); i < level; i++ {
		node.nexts[i] = g.getNode(log[i]).nexts[i]          // Set new node's next pointer
		g.getNode(log[i]).nexts[i] = marena.Offset(newNode) // Update previous node's next pointer
	}

	return marena.Offset(newNode)
}
