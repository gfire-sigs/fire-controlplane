package sepia

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dsst"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
)

// Options for the DB.
type Options struct {
	Comparator  Comparator
	MergePolicy MergePolicy
	FileSystem  vfs.FileSystem
	Dir         string
}

// DB is the Sepia Key-Value Database.
type DB struct {
	mu       sync.RWMutex
	mem      *MemTable
	imm      *MemTable // Immutable memtable (being flushed)
	wal      *LogWriter
	versions *VersionSet
	opts     *Options

	// Sequence number
	seq uint64
}

// Open opens the database.
func Open(opts *Options) (*DB, error) {
	if opts.FileSystem == nil {
		opts.FileSystem = vfs.NewDirFileSystem(opts.Dir)
	}
	if opts.Comparator == nil {
		opts.Comparator = DefaultComparator{}
	}
	if opts.MergePolicy == nil {
		opts.MergePolicy = DefaultMergePolicy{}
	}

	vs := NewVersionSet(opts.Dir, opts.FileSystem)
	// TODO: Recover from Manifest/WAL

	// Create new MemTable and WAL
	mem := NewMemTable(opts.Comparator)
	walFile, err := opts.FileSystem.Create(fmt.Sprintf("%s/wal-%d.log", opts.Dir, time.Now().UnixNano()))
	if err != nil {
		return nil, err
	}
	wal := NewLogWriter(walFile)

	return &DB{
		mem:      mem,
		wal:      wal,
		versions: vs,
		opts:     opts,
		seq:      0, // Should recover from WAL
	}, nil
}

// Put adds a key-value pair.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Assign sequence number
	db.seq++
	seq := db.seq

	// Write to WAL
	// Format: Seq(8) + Type(1) + KeyLen(varint) + Key + ValueLen(varint) + Value
	// For simplicity, just write raw data for now or use a proper batch format.
	// Let's just write key/value to WAL for this task.
	// In real DB, we write a batch.

	// Add to MemTable
	if err := db.mem.Add(seq, TypeValue, key, value); err != nil {
		return err
	}

	// Check for compaction/flush
	if db.mem.Size() > 4*1024*1024 { // 4MB limit
		// Trigger flush
		// For now, just rotate synchronously (blocking)
		if err := db.makeRoomForWrite(); err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves a value.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 1. Check MemTable
	val, err := db.mem.Get(key)
	if err != nil {
		return nil, err
	}
	if val != nil {
		return val, nil
	}

	// 2. Check Immutable MemTable
	if db.imm != nil {
		val, err := db.imm.Get(key)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
	}

	// 3. Check SSTs (VersionSet)
	// Need to implement Version.Get()
	// For now, return not found
	return nil, errors.New("not found")
}

// Delete deletes a key.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.seq++
	seq := db.seq

	if err := db.mem.Add(seq, TypeDeletion, key, nil); err != nil {
		return err
	}
	return nil
}

// Close closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.wal != nil {
		db.wal.Close()
	}
	return nil
}

func (db *DB) makeRoomForWrite() error {
	// Rotate MemTable
	if db.imm != nil {
		// Wait for background flush to finish
		// For this task, we just error or block.
		return errors.New("memtable full and flush in progress")
	}

	db.imm = db.mem
	db.mem = NewMemTable(db.opts.Comparator)

	// Create new WAL
	if db.wal != nil {
		db.wal.Close()
	}
	walFile, err := db.opts.FileSystem.Create(fmt.Sprintf("%s/wal-%d.log", db.opts.Dir, time.Now().UnixNano()))
	if err != nil {
		return err
	}
	db.wal = NewLogWriter(walFile)

	// Trigger Flush in background
	go func() {
		db.flushMemTable(db.imm)
		db.mu.Lock()
		db.imm = nil
		db.mu.Unlock()
	}()

	return nil
}

func (db *DB) flushMemTable(mem *MemTable) {
	// Write MemTable to SST
	// ...
	// Create CompactionJob for L0?
	// Or just write L0 file.

	fileNum := db.versions.NewFileNumber()
	filename := fmt.Sprintf("%s/%06d.sst", db.opts.Dir, fileNum)
	f, err := db.opts.FileSystem.Create(filename)
	if err != nil {
		// Log error
		return
	}

	sstOpts := dsst.DefaultOptions()
	// Configure sstOpts from db.opts

	w, err := dsst.NewWriter(f, sstOpts)
	if err != nil {
		return
	}

	iter := mem.Iterator()
	iter.First() // Start
	// mskip iterator iterates sorted.
	// We need to iterate and add to writer.
	// But mskip iterator returns InternalKey bytes directly?
	// Yes, Insert took []byte.

	for iter.Valid() {
		// Key is InternalKey
		// Value is user value
		// Writer.Add expects UserKey?
		// Wait, Writer.Add(key, value).
		// If we pass InternalKey as key, Writer will treat it as UserKey and add footer?
		// No, Writer is generic.
		// But Writer has KeyTSExtractor.
		// If we pass InternalKey, we need KeyTSExtractor to extract TS from it.
		// And Writer writes key as is.
		// So yes, we pass InternalKey as "key" to Writer.

		w.Add(iter.Key(), iter.Value())
		iter.Next()
	}

	w.Close()

	// Update Version
	edit := NewVersionEdit()
	edit.AddFile(0, &FileMetadata{
		FileNum: fileNum,
		// Size, Min/Max Key/TS
	})
	db.versions.LogAndApply(edit)
}
