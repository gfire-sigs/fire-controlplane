package sepia

import (
	"fmt"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dsst"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/iterator"
)

// MergePolicy decides whether to keep or drop a key during compaction.
type MergePolicy interface {
	// ShouldDrop returns true if the key should be dropped.
	ShouldDrop(key []byte, ts uint64) bool
}

// DefaultMergePolicy keeps everything.
type DefaultMergePolicy struct{}

func (p DefaultMergePolicy) ShouldDrop(key []byte, ts uint64) bool {
	return false
}

// CompactionJob represents a compaction execution.
type CompactionJob struct {
	vs          *VersionSet
	fs          vfs.FileSystem
	level       int
	inputs      []*FileMetadata
	mergePolicy MergePolicy
	options     *dsst.Options
}

func NewCompactionJob(vs *VersionSet, fs vfs.FileSystem, level int, inputs []*FileMetadata, policy MergePolicy, opts *dsst.Options) *CompactionJob {
	return &CompactionJob{
		vs:          vs,
		fs:          fs,
		level:       level,
		inputs:      inputs,
		mergePolicy: policy,
		options:     opts,
	}
}

// Run executes compaction: merges input files into a new output file.
// Returns VersionEdit describing the changes made.
func (c *CompactionJob) Run() (*VersionEdit, error) {
	// Create iterators for all input SST files
	var iters []iterator.Iterator
	for _, f := range c.inputs {
		file, err := c.fs.Open(fmt.Sprintf("%s/%06d.sst", c.vs.basePath, f.FileNum))
		if err != nil {
			return nil, err
		}
		r, err := dsst.NewReader(file, c.options)
		if err != nil {
			file.Close()
			return nil, err
		}
		iters = append(iters, r.NewIterator())
	}

	// Merge all input iterators (handles overlapping keys)
	mergedIter := iterator.NewMergeIterator(iters)
	defer mergedIter.Close()

	// Create output SST file
	fileNum := c.vs.NewFileNumber()
	filename := fmt.Sprintf("%s/%06d.sst", c.vs.basePath, fileNum)
	f, err := c.fs.Create(filename)
	if err != nil {
		return nil, err
	}

	w, err := dsst.NewWriter(f, c.options)
	if err != nil {
		f.Close()
		return nil, err
	}

	// Process merged entries and write to output
	mergedIter.First()
	for mergedIter.Valid() {
		key := mergedIter.Key()
		val := mergedIter.Value()

		// Apply merge policy to filter old entries
		ts := ExtractTS(key)
		if c.mergePolicy.ShouldDrop(key, ts) {
			mergedIter.Next()
			continue
		}

		if err := w.Add(key, val); err != nil {
			w.Close()
			return nil, err
		}
		mergedIter.Next()
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	// Create version edit to remove input files and add output
	edit := NewVersionEdit()
	for _, input := range c.inputs {
		edit.DeleteFile(c.level, input.FileNum)
	}

	// TODO: Get actual file size from writer
	meta := &FileMetadata{
		FileNum:  fileNum,
		FileSize: 0,
	}
	edit.AddFile(c.level+1, meta)

	return edit, nil
}
