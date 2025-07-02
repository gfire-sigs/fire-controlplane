package sepia

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dsst"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dwal"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/mskip"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/iterator"
	"pkg.gfire.dev/controlplane/internal/vfs"
)

const (
	// defaultArenaSize specifies the default memory allocation size for a memtable.
	defaultArenaSize      = 64 * 1024 * 1024 // 64MB
	defaultWALSegmentSize = 64 * 1024 * 1024 // 64MB
	metadataFileName      = "METADATA"
)

// metadata represents the persistent metadata for the Sepia database.
type metadata struct {
	LastWALSequence uint64 `json:"last_wal_sequence"`
	SSTables        []struct {
		Name  string `json:"name"`  // SSTable file name
		Level int    `json:"level"` // Level of the SSTable
	} `json:"sstables"`
}

// readMetadata reads the metadata from the METADATA file.
func (db *DB) readMetadata() (*metadata, error) {
	metaPath := filepath.Join(db.opts.DataDir, metadataFileName)
	exists, err := db.vfs.Exists(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check metadata file existence: %w", err)
	}
	if !exists {
		return &metadata{}, nil // Return empty metadata if file doesn't exist
	}

	file, err := db.vfs.Open(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	var meta metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	return &meta, nil
}

// writeMetadata writes the metadata to the METADATA file.
func (db *DB) writeMetadata(meta *metadata) error {
	metaPath := filepath.Join(db.opts.DataDir, metadataFileName)
	file, err := db.vfs.Create(metaPath)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %w", err)
	}
	defer file.Close()

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write metadata to file: %w", err)
	}
	return nil
}

// DB represents the Sepia key-value database.
// It holds the current active memtable and manages memory using an arena allocator.
type DB struct {
	memtable      *mskip.SkipList
	arena         *marena.Arena
	sstManager    *sstManager
	wal           *dwal.WAL // Write-Ahead Log
	vfs           vfs.VFS
	opts          Options
	encryptionKey []byte
}

// Options allows configuring the Sepia database.
type Options struct {
	// ArenaSize specifies the size of the memory arena for the memtable.
	// If zero, defaultArenaSize is used.
	ArenaSize int64
	// WALSegmentSize specifies the size of each WAL segment file.
	// If zero, defaultWALSegmentSize is used.
	WALSegmentSize int64
	// VFS is the virtual file system to use.
	VFS vfs.VFS
	// EncryptionKey is the key used for SSTable encryption.
	EncryptionKey []byte
	// DataDir is the directory where SSTables will be stored.
	DataDir string
	// Compare is the key comparison function used for ordering keys.
	// If nil, bytes.Compare is used.
	Compare func(key1, key2 []byte) int
}

// NewDB creates a new Sepia database instance.
// It initializes a memtable with the specified or default arena size.
func NewDB(opts Options) (*DB, error) {
	if opts.ArenaSize == 0 {
		opts.ArenaSize = defaultArenaSize
	}
	if opts.WALSegmentSize == 0 {
		opts.WALSegmentSize = defaultWALSegmentSize
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

	// Ensure data directory exists
	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	arena := marena.NewArena(opts.ArenaSize)
	// The seed for the skip list is based on the current time for probabilistic balancing.
	compare := opts.Compare
	if compare == nil {
		compare = TimestampCompare
	}
	memtable, err := mskip.NewSkipList(arena, compare, uint64(time.Now().UnixNano()))
	if err != nil {
		return nil, fmt.Errorf("failed to create memtable: %w", err)
	}

	sstManager, err := newSSTManager(opts.VFS, opts.DataDir, opts.EncryptionKey, compare)
	if err != nil {
		return nil, fmt.Errorf("failed to create sst manager: %w", err)
	}

	wal, err := dwal.NewWAL(opts.VFS, opts.DataDir, opts.WALSegmentSize, opts.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	db := &DB{
		arena:         arena,
		memtable:      memtable,
		sstManager:    sstManager,
		wal:           wal,
		vfs:           opts.VFS,
		opts:          opts,
		encryptionKey: opts.EncryptionKey,
	}

	// Read metadata and recover SSTables
	meta, err := db.readMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	for _, sst := range meta.SSTables {
		// Reopen SSTables based on metadata
		sstPath := filepath.Join(db.opts.DataDir, sst.Name)
		f, err := db.vfs.Open(sstPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open SSTable from metadata %s: %w", sstPath, err)
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to stat SSTable from metadata %s: %w", sstPath, err)
		}
		r, _, err := dsst.NewReader(f, info.Size(), db.encryptionKey, db.opts.Compare)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to create SST reader from metadata %s: %w", sstPath, err)
		}
		// Extract SST number from filename (e.g., sst-000001.sst -> 1)
		sstNumStr := strings.TrimPrefix(strings.TrimSuffix(sst.Name, ".sst"), "sst-")
		sstNum, err := strconv.ParseUint(sstNumStr, 10, 64)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to parse SST number from filename %s: %w", sst.Name, err)
		}
		db.sstManager.addReader(int(sstNum), r, f)
	}

	// Recover from WAL
	if err := db.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	return db, nil
}

func (db *DB) recoverFromWAL() error {
	for {
		entryData, _, err := db.wal.ReadEntry()
		if err != nil {
			if err == io.EOF {
				break // No more entries
			}
			// Handle corrupted or partial entries
			if errors.Is(err, dwal.ErrPartialEntry) || errors.Is(err, dwal.ErrCorruptedEntry) || errors.Is(err, dwal.ErrChecksumMismatch) {
				fmt.Printf("Warning: Skipping corrupted or partial WAL entry during recovery: %v\n", err)
				continue // Skip to next entry
			}
			return fmt.Errorf("failed to read WAL entry during recovery: %w", err)
		}

		entry := dsst.AcquireKVEntry()
		if err := entry.UnmarshalBinary(entryData); err != nil {
			dsst.ReleaseKVEntry(entry)
			return fmt.Errorf("failed to unmarshal WAL entry during recovery: %w", err)
		}

		// Apply entry to memtable
		if entry.EntryType == dsst.EntryTypeKeyValue {
			if !db.memtable.Insert(entry.Key, entry.Value) {
				// Memtable full during recovery, flush it
				if err := db.flushMemtable(); err != nil {
					dsst.ReleaseKVEntry(entry)
					return fmt.Errorf("failed to flush memtable during WAL recovery: %w", err)
				}
				// Try inserting again
				if !db.memtable.Insert(entry.Key, entry.Value) {
					dsst.ReleaseKVEntry(entry)
					return fmt.Errorf("memtable insertion failed during WAL recovery, arena is likely full")
				}
			}
		} else if entry.EntryType == dsst.EntryTypeTombstone {
			if !db.memtable.Insert(entry.Key, nil) { // Tombstones have nil value in memtable
				// Memtable full during recovery, flush it
				if err := db.flushMemtable(); err != nil {
					dsst.ReleaseKVEntry(entry)
					return fmt.Errorf("failed to flush memtable during WAL recovery: %w", err)
				}
				// Try inserting again
				if !db.memtable.Insert(entry.Key, nil) {
					dsst.ReleaseKVEntry(entry)
					return fmt.Errorf("memtable insertion of tombstone failed during WAL recovery, arena is likely full")
				}
			}
		}
		dsst.ReleaseKVEntry(entry)
	}
	return nil
}

// Put inserts a key-value pair into the database.
// The operation is performed on the in-memory memtable.
// In a full implementation, this would trigger a flush to disk if the memtable is full.
var Tombstone = []byte("sepia-tombstone")

// Put inserts a key-value pair into the database.
// The operation is performed on the in-memory memtable.
func (db *DB) Put(key, value []byte) error {
	// First, write to WAL
	walEntry := dsst.AcquireKVEntry()
	walEntry.EntryType = dsst.EntryTypeKeyValue
	walEntry.Key = key
	walEntry.Value = value

	walData, err := walEntry.MarshalBinary()
	if err != nil {
		dsst.ReleaseKVEntry(walEntry)
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	_, err = db.wal.WriteEntry(walData)
	if err != nil {
		dsst.ReleaseKVEntry(walEntry)
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Then, insert into memtable
	if !db.memtable.Insert(key, value) {
		if err := db.flushMemtable(); err != nil {
			dsst.ReleaseKVEntry(walEntry)
			return err
		}
		// After flushing, try inserting again into the new memtable
		if !db.memtable.Insert(key, value) {
			dsst.ReleaseKVEntry(walEntry)
			return fmt.Errorf("memtable insertion failed, arena is likely full")
		}
	}
	dsst.ReleaseKVEntry(walEntry)
	return nil
}

// Delete removes a key-value pair from the database by writing a tombstone.
// Delete removes a key-value pair from the database by writing a tombstone.
func (db *DB) Delete(key []byte) error {
	// First, write to WAL
	walEntry := dsst.AcquireKVEntry()
	walEntry.EntryType = dsst.EntryTypeTombstone
	walEntry.Key = key
	walEntry.Value = nil // Tombstones have no value

	walData, err := walEntry.MarshalBinary()
	if err != nil {
		dsst.ReleaseKVEntry(walEntry)
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	_, err = db.wal.WriteEntry(walData)
	if err != nil {
		dsst.ReleaseKVEntry(walEntry)
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Then, insert a tombstone to mark the key as deleted.
	// This will be handled during compaction (future work).
	if !db.memtable.Insert(key, nil) {
		if err := db.flushMemtable(); err != nil {
			dsst.ReleaseKVEntry(walEntry)
			return err
		}
		// After flushing, try inserting the tombstone again into the new memtable
		if !db.memtable.Insert(key, nil) {
			dsst.ReleaseKVEntry(walEntry)
			return fmt.Errorf("memtable insertion of tombstone failed, arena is likely full")
		}
	}
	dsst.ReleaseKVEntry(walEntry)
	return nil
}

func (db *DB) flushMemtable() error {
	f, _, num, err := db.sstManager.create()
	if err != nil {
		return err
	}

	w := dsst.NewWriter(f, db.sstManager.configs, db.encryptionKey, db.opts.Compare)

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

		entry := dsst.AcquireKVEntry()
		entry.EntryType = entryType
		entry.Key = iter.Key()
		entry.Value = value

		if err := w.Add(entry); err != nil {
			dsst.ReleaseKVEntry(entry)
			return err
		}
		dsst.ReleaseKVEntry(entry)
	}

	if err := w.Finish(); err != nil {
		return err
	}

	// Ensure the file is synced to disk
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync SST file to disk: %w", err)
	}

	// Add the newly created SST file to the sstManager's files map
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	fmt.Printf("Created SST file with number: %d\n", num)
	r, _, err := dsst.NewReader(f, stat.Size(), db.encryptionKey, db.opts.Compare)
	if err != nil {
		return err
	}
	db.sstManager.addReader(num, r, f)

	// Update metadata with new SSTable at level 0
	meta, err := db.readMetadata()
	if err != nil {
		return fmt.Errorf("failed to read metadata before updating SSTables: %w", err)
	}
	meta.SSTables = append(meta.SSTables, struct {
		Name  string `json:"name"`
		Level int    `json:"level"`
	}{
		Name:  fmt.Sprintf("sst-%06d.sst", num),
		Level: 0,
	})
	if err := db.writeMetadata(meta); err != nil {
		return fmt.Errorf("failed to write metadata after flushing memtable: %w", err)
	}

	// Trigger compaction for level 0 after adding new SSTable
	if err := db.sstManager.compact(0); err != nil {
		return fmt.Errorf("failed to perform compaction after memtable flush: %w", err)
	}

	// Update metadata after compaction
	meta, err = db.readMetadata()
	if err != nil {
		return fmt.Errorf("failed to read metadata after compaction: %w", err)
	}
	// TODO: Update metadata levels based on sstManager levels after compaction
	// For now, this is a placeholder as the compaction logic in sstManager does not yet update metadata

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

// TimestampCompare compares two keys assuming the last 8 bytes of each key represent a 64-bit timestamp.
// It prioritizes higher timestamps (descending order), and for equal timestamps, it compares the key prefix using bytes.Compare.
func TimestampCompare(key1, key2 []byte) int {
	if len(key1) < 8 || len(key2) < 8 {
		return bytes.Compare(key1, key2) // Fallback if key is too short to have a timestamp
	}

	// Extract timestamps (last 8 bytes)
	ts1 := int64(key1[len(key1)-8])<<56 | int64(key1[len(key1)-7])<<48 | int64(key1[len(key1)-6])<<40 | int64(key1[len(key1)-5])<<32 |
		int64(key1[len(key1)-4])<<24 | int64(key1[len(key1)-3])<<16 | int64(key1[len(key1)-2])<<8 | int64(key1[len(key1)-1])
	ts2 := int64(key2[len(key2)-8])<<56 | int64(key2[len(key2)-7])<<48 | int64(key2[len(key2)-6])<<40 | int64(key2[len(key2)-5])<<32 |
		int64(key2[len(key2)-4])<<24 | int64(key2[len(key2)-3])<<16 | int64(key2[len(key2)-2])<<8 | int64(key2[len(key2)-1])

	// Compare timestamps in descending order (higher timestamp first)
	if ts1 > ts2 {
		return -1
	} else if ts1 < ts2 {
		return 1
	}

	// If timestamps are equal, compare the key prefix
	return bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8])
}

// Close releases resources used by the database.
// In a full implementation, this would ensure all data is flushed to disk.
func (db *DB) Close() error {
	if err := db.flushMemtable(); err != nil {
		return err
	}
	// Update metadata with last WAL sequence before closing
	meta, err := db.readMetadata()
	if err != nil {
		return fmt.Errorf("failed to read metadata before closing: %w", err)
	}
	// TODO: Get actual last committed WAL sequence from WAL
	meta.LastWALSequence = 0 // Placeholder for now
	if err := db.writeMetadata(meta); err != nil {
		return fmt.Errorf("failed to write metadata before closing: %w", err)
	}

	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}
	return db.sstManager.close()
}
