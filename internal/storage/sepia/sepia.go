package sepia

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dsst"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/mskip"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/iterator"
	"pkg.gfire.dev/controlplane/internal/vfs"
)

const (
	// defaultArenaSize specifies the default memory allocation size for a memtable.
	defaultArenaSize = 64 * 1024 * 1024 // 64MB
)

// DB represents the Sepia key-value database.
// It holds the current active memtable and manages memory using an arena allocator.
type DB struct {
	mu            sync.RWMutex
	memtable      *mskip.SkipList
	arena         *marena.Arena
	sstManager    *sstManager
	vfs           vfs.VFS
	opts          Options
	encryptionKey []byte
}

// Options allows configuring the Sepia database.
type Options struct {
	// ArenaSize specifies the size of the memory arena for the memtable.
	// If zero, defaultArenaSize is used.
	ArenaSize int64
	// VFS is the virtual file system to use.
	VFS vfs.VFS
	// EncryptionKey is the key used for SSTable encryption.
	EncryptionKey []byte
	// DataDir is the directory where SSTables will be stored.
	DataDir string
}

// NewDB creates a new Sepia database instance.
// It initializes a memtable with the specified or default arena size.
func NewDB(opts Options) (*DB, error) {
	if opts.ArenaSize == 0 {
		opts.ArenaSize = defaultArenaSize
	}
	if opts.VFS == nil {
		opts.VFS = vfs.NewOSVFS()
	}
	if opts.EncryptionKey == nil {
		opts.EncryptionKey = []byte(dsst.DefaultEncryptionKeyStr)
	}
	// If DataDir is not specified, use a default.
	if opts.DataDir == "" {
		opts.DataDir = "./database"
	}

	arena := marena.NewArena(opts.ArenaSize)
	// The seed for the skip list is based on the current time for probabilistic balancing.
	memtable, err := mskip.NewSkipList(arena, bytes.Compare, uint64(time.Now().UnixNano()))
	if err != nil {
		return nil, fmt.Errorf("failed to create memtable: %w", err)
	}

	sstManager, err := newSSTManager(opts.VFS, opts.DataDir, opts.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create sst manager: %w", err)
	}

	db := &DB{
		arena:         arena,
		memtable:      memtable,
		sstManager:    sstManager,
		vfs:           opts.VFS,
		opts:          opts,
		encryptionKey: opts.EncryptionKey,
	}
	return db, nil
}

// Put inserts a key-value pair into the database.
// The operation is performed on the in-memory memtable.
// In a full implementation, this would trigger a flush to disk if the memtable is full.
var Tombstone = []byte("sepia-tombstone")

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry := dsst.KVEntry{
		EntryType: dsst.EntryTypeKeyValue,
		Key:       key,
		Value:     value,
	}

	if !db.memtable.Insert(entry.Key, entry.Value) {
		if err := db.flushMemtable(); err != nil {
			return err
		}
		// After flushing, try inserting again into the new memtable
		if !db.memtable.Insert(entry.Key, entry.Value) {
			return fmt.Errorf("memtable insertion failed, arena is likely full")
		}
	}
	return nil
}

// Delete removes a key-value pair from the database by writing a tombstone.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry := dsst.KVEntry{
		EntryType: dsst.EntryTypeTombstone,
		Key:       key,
		Value:     nil, // Tombstones have no value
	}

	// Insert a tombstone to mark the key as deleted.
	// This will be handled during compaction (future work).
	if !db.memtable.Insert(entry.Key, entry.Value) {
		if err := db.flushMemtable(); err != nil {
			return err
		}
		// After flushing, try inserting the tombstone again into the new memtable
		if !db.memtable.Insert(entry.Key, entry.Value) {
			return fmt.Errorf("memtable insertion of tombstone failed, arena is likely full")
		}
	}
	return nil
}

func (db *DB) flushMemtable() error {
	f, configs, num, err := db.sstManager.create()
	if err != nil {
		return err
	}

	w := dsst.NewWriter(f, configs, db.encryptionKey)

	iter := db.memtable.Iterator()
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		// The value stored in memtable is the actual value for KeyValue entries,
		// or nil for Tombstone entries. We need to reconstruct the KVEntry.
		entryType := dsst.EntryTypeKeyValue
		value := iter.Value()
		// In mskip.SkipList, a tombstone is represented by a nil value.
		if value == nil {
			entryType = dsst.EntryTypeTombstone
		}

		kvEntry := dsst.KVEntry{
			EntryType: entryType,
			Key:       iter.Key(),
			Value:     value,
		}

		if err := w.Add(kvEntry); err != nil {
			return err
		}
	}

	if err := w.Finish(); err != nil {
		return err
	}

	// Add the newly created SST file to the sstManager's files map
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	r, _, err := dsst.NewReader(f, stat.Size(), db.encryptionKey)
	if err != nil {
		return err
	}
	db.sstManager.addReader(num, r, f)

	db.arena = marena.NewArena(db.opts.ArenaSize)
	db.memtable, err = mskip.NewSkipList(db.arena, bytes.Compare, uint64(time.Now().UnixNano()))
	if err != nil {
		return fmt.Errorf("failed to create new memtable: %w", err)
	}
	return nil
}

// Get retrieves the value for a given key from the memtable.
// It returns the value, a boolean indicating if the key was found, and an error.
// In a full implementation, this would also search SSTables on disk if not in memtable.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iter := db.memtable.Iterator()
	defer iter.Close()

	iter.SeekLE(key)

	if iter.Valid() && bytes.Equal(iter.Key(), key) {
		// If the value is nil, it's a tombstone, meaning the key is deleted.
		if iter.Value() == nil {
			return nil, false, nil
		}
		val := make([]byte, len(iter.Value()))
		copy(val, iter.Value())
		return val, true, nil
	}

	// Check SSTables if not found in memtable or if it was a tombstone in memtable
	val, ok, err := db.sstManager.get(key)
	if err != nil {
		return nil, false, err
	}
	// If found in SSTable, check if it's a tombstone (nil value)
	if ok && val == nil {
		return nil, false, nil
	}
	return val, ok, nil
}

// Iterator returns a new iterator for traversing the key-value pairs in the database.
// It merges the memtable and SSTable iterators to provide a unified view.
func (db *DB) Iterator() iterator.Iterator {
	memIter := db.memtable.Iterator()
	sstIter := db.sstManager.Iterator()
	iters := []iterator.Iterator{memIter, sstIter}
	return iterator.NewMergeIterator(iters)
}

// Close releases resources used by the database.
// In a full implementation, this would ensure all data is flushed to disk.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.flushMemtable(); err != nil {
		return err
	}
	return db.sstManager.close()
}
