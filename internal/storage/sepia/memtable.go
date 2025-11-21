package sepia

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/mskip"
)

// MemTable is an in-memory key-value store.
type MemTable struct {
	skiplist *mskip.SkipList
	arena    *marena.Arena
	ref      int32
}

// NewMemTable creates a new MemTable.
func NewMemTable(cmp Comparator) *MemTable {
	internalCmp := InternalKeyComparator{UserComparator: cmp}
	arena := marena.NewArena(4 << 20)                         // 4MB initial arena
	sl, _ := mskip.NewSkipList(arena, internalCmp.Compare, 0) // 0 seed
	return &MemTable{
		skiplist: sl,
		arena:    arena,
		ref:      1,
	}
}

// Add adds a key-value pair to the memtable.
func (m *MemTable) Add(seq uint64, t ValueType, key, value []byte) error {
	internalKey := NewInternalKey(key, seq, t)
	if !m.skiplist.Insert(internalKey, value) {
		return errors.New("memtable insert failed") // Allocation failed
	}
	return nil
}

// Get looks up a key in the memtable.
func (m *MemTable) Get(key []byte) (value []byte, err error) {
	// We want the latest version of the key.
	// Our comparator sorts UserKey asc, SeqNum desc.
	// So "key + MaxSeq" is the first entry for "key".
	lookupKey := NewInternalKey(key, ^uint64(0)>>8, TypeValue) // Max seq

	iter := m.skiplist.Iterator()
	defer iter.Close()

	iter.Seek(lookupKey)
	if iter.Valid() {
		// Check if user key matches
		internalKey := InternalKey(iter.Key())
		fmt.Printf("Get(%s): Found key %s seq %d type %d trailer=%x\n", key, internalKey.UserKey(), internalKey.SeqNum(), internalKey.Type(), internalKey.Trailer())
		if bytes.Equal(internalKey.UserKey(), key) {
			// Found it. Check type.
			if internalKey.Type() == TypeValue {
				return iter.Value(), nil
			}
			// Deleted
			return nil, nil // or ErrNotFound
		}
	} else {
		fmt.Printf("Get(%s): Seek found nothing\n", key)
	}
	return nil, nil // Not found
}

// Iterator returns an iterator over the memtable.
func (m *MemTable) Iterator() *mskip.SkipListIterator {
	return m.skiplist.Iterator()
}

// Size returns the approximate size of the memtable.
func (m *MemTable) Size() int {
	return 0
}

// Ref increments the reference count.
func (m *MemTable) Ref() {
	atomic.AddInt32(&m.ref, 1)
}

// Unref decrements the reference count and frees memory if 0.
func (m *MemTable) Unref() {
	if atomic.AddInt32(&m.ref, -1) == 0 {
		m.arena.Reset()
	}
}
