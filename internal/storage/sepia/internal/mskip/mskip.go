package mskip

import (
	"unsafe"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/splitmix64"
)

const (
	MSKIP_MAX_LEVEL = 24
)

// mskip in-memory format:
//
// key_ptr: uint64
// value_ptr: uint64
// level: int32
// reserved: 4 bytes
// nexts: (8 * level) bytes

type mskipNode struct {
	keyPtr   uint64
	valuePtr uint64
	level    int32
	nexts    [MSKIP_MAX_LEVEL]uint32
}

const (
	nodeKeyPtrOffset   = unsafe.Offsetof(mskipNode{}.keyPtr)
	nodeValuePtrOffset = unsafe.Offsetof(mskipNode{}.valuePtr)
	nodeLevelOffset    = unsafe.Offsetof(mskipNode{}.level)
	nodeNextsOffset    = unsafe.Offsetof(mskipNode{}.nexts)
	nodeMaxSize        = unsafe.Sizeof(mskipNode{})
)

var _ = [1]struct{}{}[nodeKeyPtrOffset-0]
var _ = [1]struct{}{}[nodeValuePtrOffset-8]
var _ = [1]struct{}{}[nodeLevelOffset-16]
var _ = [1]struct{}{}[nodeNextsOffset-20]
var _ = [1]struct{}{}[nodeMaxSize-120]

func sizeNode(nodeLevel int32) uintptr {
	return uintptr(nodeMaxSize - MSKIP_MAX_LEVEL*4 + uintptr(nodeLevel)*4)
}

type SkipList struct {
	arena *marena.Arena

	seed uint64
	head uint32

	compare func(key1, key2 []byte) int // -1 if key1 < key2, 0 if key1 == key2, 1 if key1 > key2
}

func NewSkipList(arena *marena.Arena, compare func(key1, key2 []byte) int, seed uint64) (*SkipList, error) {
	g := &SkipList{
		arena:   arena,
		seed:    seed,
		head:    0,
		compare: compare,
	}

	headSize := sizeNode(MSKIP_MAX_LEVEL)
	headPtr := arena.Allocate(int(headSize))
	if headPtr == marena.ARENA_INVALID_ADDRESS {
		return nil, marena.ErrAllocationFailed
	}
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

func (g *SkipList) randLevel() int32 {
	level := int32(1)
	for level < MSKIP_MAX_LEVEL && splitmix64.Splitmix64(&g.seed)%2 == 0 {
		level++
	}
	return level
}

func (g *SkipList) getNode(ptr uint32) *mskipNode {
	return (*mskipNode)(unsafe.Pointer(g.arena.Index(ptr)))
}

func (g *SkipList) seeklt(key []byte, log *[MSKIP_MAX_LEVEL]uint32) uint32 {
	ptr := g.head

	var dummyLog [MSKIP_MAX_LEVEL]uint32
	if log == nil {
		log = &dummyLog
	}

	for i := MSKIP_MAX_LEVEL - 1; i >= 0; i-- {
		for {
			next := g.getNode(ptr).nexts[i]
			if next == marena.ARENA_INVALID_ADDRESS {
				break
			}
			if g.compare(key, g.arena.View(g.getNode(next).keyPtr)) <= 0 {
				break
			}

			ptr = next
		}
		log[i] = ptr
	}

	return ptr
}

func (g *SkipList) insertNext(log *[MSKIP_MAX_LEVEL]uint32, key []byte, value []byte) uint32 {
	next := g.getNode(log[0]).nexts[0]

	if next != marena.ARENA_INVALID_ADDRESS && g.compare(key, g.arena.View(g.getNode(next).keyPtr)) == 0 {
		newValue := g.arena.Allocate(len(value))
		if newValue == marena.ARENA_INVALID_ADDRESS {
			return marena.ARENA_INVALID_ADDRESS
		}
		copy(g.arena.View(newValue), value)
		g.getNode(next).valuePtr = newValue
		return next
	}

	level := g.randLevel()
	var newNode, newValue, newKey uint64 = uint64(sizeNode(level)), uint64(len(value)), uint64(len(key))
	if !g.arena.AllocateMultiple(&newNode, &newValue, &newKey) {
		return marena.ARENA_INVALID_ADDRESS
	}

	node := g.getNode(marena.Offset(newNode))
	node.level = level
	node.keyPtr = newKey
	node.valuePtr = newValue
	copy(g.arena.View(node.keyPtr), key)
	copy(g.arena.View(node.valuePtr), value)

	for i := int32(0); i < level; i++ {
		node.nexts[i] = g.getNode(log[i]).nexts[i]
		g.getNode(log[i]).nexts[i] = marena.Offset(newNode)
	}

	return marena.Offset(newNode)
}
