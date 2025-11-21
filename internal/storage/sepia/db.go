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

	// Write to WAL with format: Seq(8) + Type(1) + KeyLen(varint) + Key + ValueLen(varint) + Value
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

	// Trigger flush when memtable exceeds 4MB limit
	if db.mem.Size() > 4*1024*1024 {
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

	// Search order: MemTable -> Immutable MemTable -> SST files
	if val, err := db.mem.Get(key); err != nil {
		return nil, err
	} else if val != nil {
		return val, nil
	}

	if db.imm != nil {
		if val, err := db.imm.Get(key); err != nil {
			return nil, err
		} else if val != nil {
			return val, nil
		}
	}

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
	// Rotate memtable: current becomes immutable, new one created
	if db.imm != nil {
		return errors.New("memtable full and flush in progress")
	}

	db.imm = db.mem
	db.mem = NewMemTable(db.opts.Comparator)

	// Create new WAL file
	if db.wal != nil {
		db.wal.Close()
	}
	walFile, err := db.opts.FileSystem.Create(fmt.Sprintf("%s/wal-%d.log", db.opts.Dir, time.Now().UnixNano()))
	if err != nil {
		return err
	}
	db.wal = NewLogWriter(walFile)

	// Flush immutable memtable to SST in background
	go func() {
		db.flushMemTable(db.imm)
		db.mu.Lock()
		db.imm = nil
		db.mu.Unlock()
	}()

	return nil
}

func (db *DB) flushMemTable(mem *MemTable) {
	// Convert memtable to SST file and update version
	fileNum := db.versions.NewFileNumber()
	filename := fmt.Sprintf("%s/%06d.sst", db.opts.Dir, fileNum)
	f, err := db.opts.FileSystem.Create(filename)
	if err != nil {
		return
	}

	sstOpts := dsst.DefaultOptions()
	w, err := dsst.NewWriter(f, sstOpts)
	if err != nil {
		return
	}

	// Iterate through memtable entries and write to SST
	iter := mem.Iterator()
	iter.First()
	for iter.Valid() {
		// InternalKey contains user key + sequence + type
		// Writer stores it as-is for proper ordering
		w.Add(iter.Key(), iter.Value())
		iter.Next()
	}

	w.Close()

	// Update version with new SST file metadata
	minKey := InternalKey(w.MinKey())
	maxKey := InternalKey(w.MaxKey())

	edit := NewVersionEdit()
	edit.AddFile(0, &FileMetadata{
		FileNum:  fileNum,
		FileSize: w.Size(),
		MinKey:   minKey,
		MaxKey:   maxKey,
	})

	if stat, err := f.Stat(); err == nil {
		edit.NewFiles[0][0].FileSize = uint64(stat.Size())
	}

	db.versions.LogAndApply(edit)
}

func (db *DB) recover() error {
	// Recover from WAL files to restore database state
	files, err := db.opts.FileSystem.List(db.opts.Dir)
	if err != nil {
		return err
	}

	// Find and replay all WAL files
	for _, f := range files {
		// Check if file is a WAL (format: wal-<timestamp>.log)
		if !strings.Contains(f, "/wal-") || !strings.HasSuffix(f, ".log") {
			continue
		}

		// Open and replay WAL file
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
				break
			}

			// Parse WAL record: Seq(8) + Type(1) + KeyLen(varint) + Key + ValueLen(varint) + Value
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

			// Update sequence number and replay to memtable
			if seq > db.seq {
				db.seq = seq
			}
			db.mem.Add(seq, t, key, value)
		}
		file.Close()
	}
	return nil
}
