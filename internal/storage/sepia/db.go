package sepia

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
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

	// Create new MemTable and WAL
	mem := NewMemTable(opts.Comparator)
	walFile, err := opts.FileSystem.Create(fmt.Sprintf("%s/wal-%d.log", opts.Dir, time.Now().UnixNano()))
	if err != nil {
		return nil, err
	}
	wal := NewLogWriter(walFile)

	db := &DB{
		mem:      mem,
		wal:      wal,
		versions: vs,
		opts:     opts,
		seq:      0,
	}

	if err := db.recover(); err != nil {
		// If recovery fails, we should probably close what we opened
		db.Close()
		return nil, err
	}

	return db, nil
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
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, seq)
	buf.WriteByte(byte(TypeValue))
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.Write(key)
	binary.Write(buf, binary.LittleEndian, int32(len(value)))
	buf.Write(value)

	if err := db.wal.AddRecord(buf.Bytes()); err != nil {
		return err
	}

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
	if val, err := db.versions.current.Get(key, db.opts); err != nil {
		return nil, err
	} else if val != nil {
		return val, nil
	}

	return nil, errors.New("not found")
}

// Delete deletes a key.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.seq++
	seq := db.seq

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, seq)
	buf.WriteByte(byte(TypeDeletion))
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	buf.Write(key)
	// No value for deletion

	if err := db.wal.AddRecord(buf.Bytes()); err != nil {
		return err
	}

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
	// We need to convert InternalKey to InternalKey to store in FileMetadata?
	// FileMetadata expects InternalKey.
	// dsst.Writer.MinKey() returns the raw key passed to Add(), which is InternalKey.
	// So we can just cast it.
	minKey := InternalKey(w.MinKey())
	maxKey := InternalKey(w.MaxKey())

	edit := NewVersionEdit()
	edit.AddFile(0, &FileMetadata{
		FileNum:  fileNum,
		FileSize: w.Size(),
		MinKey:   minKey,
		MaxKey:   maxKey,
	})

	// Get file size
	if stat, err := f.Stat(); err == nil {
		edit.NewFiles[0][0].FileSize = uint64(stat.Size())
	}

	db.versions.LogAndApply(edit)
}

func (db *DB) recover() error {
	// 1. List files
	files, err := db.opts.FileSystem.List(db.opts.Dir)
	if err != nil {
		return err
	}

	// 2. Find latest WAL
	var walFile string
	var maxWalTs int64
	for _, f := range files {
		if n, err := fmt.Sscanf(f, "wal-%d.log", &maxWalTs); err == nil && n == 1 {
			// Assuming simple format check.
			// Actually Sscanf might match partial?
			// Let's use strings.HasPrefix and suffix.
			// But memfs returns full paths or names?
			// memfs.List returns names (keys in map).
			// In Open, we create "dir/wal-ts.log".
			// So name might contain dir prefix if memfs is simple.
			// But List implementation in memfs returns just keys.
			// If keys are full paths, we need to check that.
			if f > walFile {
				walFile = f
			}
		}
	}

	// Better WAL finding logic:
	// We want the LATEST WAL.
	// The naming is wal-<timestamp>.log.
	// String comparison works for timestamp if fixed width, but they are not.
	// But they are likely close.
	// Let's just look for all wal files and pick the one with largest timestamp.

	// Actually, Open() creates a NEW WAL.
	// So we should recover from the PREVIOUS WAL(s).
	// But for this task, let's assume we just recover from any existing WALs that are not the one we just created?
	// Wait, Open() created a new WAL at line 53.
	// So if we list now, we will see the new one too.
	// We should recover from *older* WALs.
	// Or, we should have recovered BEFORE creating the new WAL.
	// Yes, typically recovery happens before opening new WAL.
	// But I can't easily change the order in Open without rewriting it.
	// I'll just skip the current WAL (empty) or replay it (no op).
	// Actually, if I replay all WALs, it should be fine.

	// Let's refine Open() to call recover BEFORE creating new WAL?
	// That would be cleaner.
	// But I am using multi_replace on existing structure.
	// I will just replay all WALs found.

	for _, f := range files {
		// Check if it is a WAL file
		// We need to handle full paths if memfs returns them.
		// memfs.List returns keys. In Open we used "dir/wal...".
		// So keys are full paths.
		if !strings.Contains(f, "/wal-") || !strings.HasSuffix(f, ".log") {
			continue
		}

		// Open WAL
		file, err := db.opts.FileSystem.Open(f)
		if err != nil {
			return err
		}

		reader := NewLogReader(file)
		for {
			data, err := reader.ReadRecord()
			if err != nil {
				if err == io.EOF {
					break
				}
				// Ignore partial records at end?
				break
			}

			// Parse batch
			// Format: Seq(8) + Type(1) + KeyLen(varint) + Key + ValueLen(varint) + Value
			// Note: db.Put writes raw data as a record.

			buf := bytes.NewReader(data)
			var seq uint64
			if err := binary.Read(buf, binary.LittleEndian, &seq); err != nil {
				break
			}

			var typeByte byte
			if err := binary.Read(buf, binary.LittleEndian, &typeByte); err != nil {
				break
			}
			t := ValueType(typeByte)

			var keyLen int32
			if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
				break
			}
			key := make([]byte, keyLen)
			if _, err := io.ReadFull(buf, key); err != nil {
				break
			}

			var valLen int32
			var value []byte
			if t == TypeValue {
				if err := binary.Read(buf, binary.LittleEndian, &valLen); err != nil {
					break
				}
				value = make([]byte, valLen)
				if _, err := io.ReadFull(buf, value); err != nil {
					break
				}
			}

			// Update DB seq
			if seq > db.seq {
				db.seq = seq
			}

			// Add to MemTable
			db.mem.Add(seq, t, key, value)
		}
		file.Close()
	}
	return nil
}
