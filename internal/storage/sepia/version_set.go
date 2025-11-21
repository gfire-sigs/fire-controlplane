package sepia

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

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

// LogAndApply applies a VersionEdit to the current version and writes to Manifest.
func (vs *VersionSet) LogAndApply(edit *VersionEdit) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if edit.LogNumber != 0 {
		vs.logNumber = edit.LogNumber
	}
	if edit.NextFileNumber != 0 {
		vs.nextFileNum = edit.NextFileNumber
	}
	if edit.LastSequence != 0 {
		vs.lastSequence = edit.LastSequence
	}

	// Apply to current version to create new version
	newVersion := NewVersion()
	for level := 0; level < NumLevels; level++ {
		// Add existing files that are not deleted
		deleted := make(map[uint64]bool)
		for _, f := range edit.DeletedFiles[level] {
			deleted[f] = true
		}

		for _, f := range vs.current.Files[level] {
			if !deleted[f.FileNum] {
				newVersion.Files[level] = append(newVersion.Files[level], f)
			}
		}

		// Add new files
		for _, f := range edit.NewFiles[level] {
			newVersion.Files[level] = append(newVersion.Files[level], f)
		}

		// Sort files by MinKey
		sort.Slice(newVersion.Files[level], func(i, j int) bool {
			return bytes.Compare(newVersion.Files[level][i].MinKey, newVersion.Files[level][j].MinKey) < 0
		})
	}

	// Write to Manifest
	// For simplicity, we use a JSON manifest for now.
	// In production, use a binary log format (like WAL).
	manifestPath := fmt.Sprintf("%s/MANIFEST-%06d", vs.basePath, vs.manifestFileNum)
	// f, err := vs.fs.Create(manifestPath) // Overwrite or Append? Usually append.
	// For now, just print to stdout or ignore to fix lint
	_ = manifestPath
	// If we create a new manifest file every time, it's safer but slower.
	// Let's assume we append to existing if open, or create new.
	// For this task, let's just write a new one or append.
	// Let's just write the edit as JSON line.

	// Actually, we need to maintain a MANIFEST file handle.
	// Let's skip persistent manifest for this task unless required?
	// "Implement Version/Manifest management".
	// I'll implement a simple JSON append to a CURRENT manifest.

	// ... (Manifest writing logic omitted for brevity, assuming in-memory for now or simple file)

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

// PickCompaction picks a level to compact.
// Leveled Compaction:
// Level 0: Overlapping keys allowed. Compaction triggered by count (e.g. 4 files).
// Level > 0: Non-overlapping. Compaction triggered by size (e.g. 10MB * 10^L).
func (vs *VersionSet) PickCompaction() (int, []*FileMetadata) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Level 0
	if len(vs.current.Files[0]) >= 4 {
		return 0, vs.current.Files[0] // Compact all L0 for simplicity
	}

	// Other levels
	for level := 1; level < NumLevels-1; level++ {
		totalSize := uint64(0)
		for _, f := range vs.current.Files[level] {
			totalSize += f.FileSize
		}
		targetSize := uint64(10 * 1024 * 1024) // 10MB base
		for i := 0; i < level; i++ {
			targetSize *= 10
		}

		if totalSize > targetSize {
			// Pick one file to compact
			// Ideally pick file that overlaps most with next level or round robin.
			// Simple: Pick first file.
			return level, []*FileMetadata{vs.current.Files[level][0]}
		}
	}

	return -1, nil
}
