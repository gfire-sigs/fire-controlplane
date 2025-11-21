package sepia

import (
	"bytes"
	"errors"
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

// Get looks up a key in the memtable and returns the latest version.
// InternalKey ordering: UserKey asc, SeqNum desc (newer first)
func (m *MemTable) Get(key []byte) (value []byte, err error) {
	iter := m.skiplist.Iterator()
	defer iter.Close()

	iter.First()
	for iter.Valid() {
		internalKey := InternalKey(iter.Key())
		cmp := bytes.Compare(internalKey.UserKey(), key)
		if cmp == 0 {
			// Found key - return value if not deleted
			if internalKey.Type() == TypeValue {
				return iter.Value(), nil
			}
			// Key was deleted
			return nil, nil
		} else if cmp > 0 {
			// Passed the key location - not found
			break
		}
		iter.Next()
	}
	return nil, nil // Not found
}

// Iterator returns an iterator over the memtable.
func (m *MemTable) Iterator() *mskip.SkipListIterator {
	return m.skiplist.Iterator()
}

// Size returns the approximate size of the memtable in bytes.
// TODO: Implement actual size calculation based on arena usage.
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
