package sepia

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dsst"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
)

const (
	NumLevels = 7
)

// FileMetadata contains information about an SST file.
type FileMetadata struct {
	FileNum  uint64
	FileSize uint64
	MinKey   InternalKey
	MaxKey   InternalKey
	MinTS    uint64
	MaxTS    uint64
}

// Version represents a snapshot of the database at a point in time.
type Version struct {
	Files [NumLevels][]*FileMetadata
	ref   int
}

func NewVersion() *Version {
	return &Version{
		ref: 1,
	}
}

func (v *Version) Ref() {
	v.ref++
}

func (v *Version) Unref() {
	v.ref--
	if v.ref == 0 {
		// cleanup
	}
}

// VersionEdit describes changes to apply to a Version.
type VersionEdit struct {
	ComparatorName string
	LogNumber      uint64
	NextFileNumber uint64
	LastSequence   uint64
	DeletedFiles   map[int][]uint64        // Level -> FileNum
	NewFiles       map[int][]*FileMetadata // Level -> FileMetadata
}

func NewVersionEdit() *VersionEdit {
	return &VersionEdit{
		DeletedFiles: make(map[int][]uint64),
		NewFiles:     make(map[int][]*FileMetadata),
	}
}

func (e *VersionEdit) AddFile(level int, f *FileMetadata) {
	e.NewFiles[level] = append(e.NewFiles[level], f)
}

func (e *VersionEdit) DeleteFile(level int, fileNum uint64) {
	e.DeletedFiles[level] = append(e.DeletedFiles[level], fileNum)
}

// VersionSet manages the collection of versions.
type VersionSet struct {
	basePath        string
	fs              vfs.FileSystem
	current         *Version
	manifestFileNum uint64
	nextFileNum     uint64
	logNumber       uint64
	lastSequence    uint64
	mu              sync.Mutex
}

func NewVersionSet(basePath string, fs vfs.FileSystem) *VersionSet {
	return &VersionSet{
		basePath: basePath,
		fs:       fs,
		current:  NewVersion(),
	}
}

// LogAndApply applies a VersionEdit to create new version and updates manifest.
func (vs *VersionSet) LogAndApply(edit *VersionEdit) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Update metadata from edit
	if edit.LogNumber != 0 {
		vs.logNumber = edit.LogNumber
	}
	if edit.NextFileNumber != 0 {
		vs.nextFileNum = edit.NextFileNumber
	}
	if edit.LastSequence != 0 {
		vs.lastSequence = edit.LastSequence
	}

	// Create new version by applying edits to current
	newVersion := NewVersion()
	for level := 0; level < NumLevels; level++ {
		// Keep existing files that are not deleted
		deleted := make(map[uint64]bool)
		for _, f := range edit.DeletedFiles[level] {
			deleted[f] = true
		}

		for _, f := range vs.current.Files[level] {
			if !deleted[f.FileNum] {
				newVersion.Files[level] = append(newVersion.Files[level], f)
			}
		}

		// Add new files and sort by MinKey
		newVersion.Files[level] = append(newVersion.Files[level], edit.NewFiles[level]...)
		sort.Slice(newVersion.Files[level], func(i, j int) bool {
			return bytes.Compare(newVersion.Files[level][i].MinKey, newVersion.Files[level][j].MinKey) < 0
		})
	}

	// TODO: Implement persistent manifest writing
	// For now, maintain in-memory version only

	vs.current.Unref()
	vs.current = newVersion
	return nil
}

// NewFileNumber generates a new file number.
func (vs *VersionSet) NewFileNumber() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.nextFileNum++
	return vs.nextFileNum
}

// Level0Files returns the files in level 0.
func (vs *VersionSet) Level0Files() []*FileMetadata {
	return vs.current.Files[0]
}

// PickCompaction selects a level and files for compaction based on:
// - Level 0: compact when file count >= 4 (overlapping keys)
// - Level > 0: compact when size exceeds threshold (10MB * 10^level)
func (vs *VersionSet) PickCompaction() (int, []*FileMetadata) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Check Level 0 for file count trigger
	if len(vs.current.Files[0]) >= 4 {
		return 0, vs.current.Files[0]
	}

	// Check other levels for size trigger
	for level := 1; level < NumLevels-1; level++ {
		totalSize := uint64(0)
		for _, f := range vs.current.Files[level] {
			totalSize += f.FileSize
		}

		// Calculate target size: 10MB * 10^level
		targetSize := uint64(10 * 1024 * 1024)
		for i := 0; i < level; i++ {
			targetSize *= 10
		}

		if totalSize > targetSize {
			// TODO: Implement smarter file selection (overlap-based)
			return level, []*FileMetadata{vs.current.Files[level][0]}
		}
	}

	return -1, nil // No compaction needed
}

// Get searches for a key across all levels, returning the value or nil if not found.
// Search order: Level 0 (newest to oldest) -> Level 1+ (binary search)
func (v *Version) Get(key []byte, opts *Options) ([]byte, error) {
	// Level 0: files may overlap, search from newest to oldest
	for i := len(v.Files[0]) - 1; i >= 0; i-- {
		f := v.Files[0][i]
		if bytes.Compare(key, f.MinKey.UserKey()) >= 0 && bytes.Compare(key, f.MaxKey.UserKey()) <= 0 {
			if val, err := v.getFromFile(f, key, opts); err != nil {
				return nil, err
			} else if val != nil {
				return val, nil
			}
		}
	}

	// Level 1+: files are sorted and non-overlapping, use binary search
	for level := 1; level < NumLevels; level++ {
		files := v.Files[level]
		if len(files) == 0 {
			continue
		}

		// Find file with MaxKey >= target key
		idx := sort.Search(len(files), func(i int) bool {
			return bytes.Compare(files[i].MaxKey.UserKey(), key) >= 0
		})

		if idx < len(files) {
			f := files[idx]
			if bytes.Compare(key, f.MinKey.UserKey()) >= 0 {
				if val, err := v.getFromFile(f, key, opts); err != nil {
					return nil, err
				} else if val != nil {
					return val, nil
				}
			}
		}
	}

	return nil, nil // Key not found
}

func (v *Version) getFromFile(f *FileMetadata, key []byte, opts *Options) ([]byte, error) {
	// Open SST file and search for key
	filename := fmt.Sprintf("%s/%06d.sst", opts.Dir, f.FileNum)
	file, err := opts.FileSystem.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// TODO: Copy encryption key and other options from opts to sstOpts
	sstOpts := dsst.DefaultOptions()
	reader, err := dsst.NewReader(file, sstOpts)
	if err != nil {
		return nil, err
	}

	iter := reader.NewIterator()
	defer iter.Close()

	// Seek to key and check if found
	iter.Seek(key)
	if iter.Valid() && bytes.Equal(iter.Key(), key) {
		if ValueType(iter.Kind()) == TypeDeletion {
			return nil, nil // Key was deleted
		}

		// Return copy of value (iterator buffer may be reused)
		val := iter.Value()
		ret := make([]byte, len(val))
		copy(ret, val)
		return ret, nil
	}

	return nil, nil // Key not found in this file
}
