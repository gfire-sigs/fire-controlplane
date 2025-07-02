package sepia

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dsst"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/iterator"
	"pkg.gfire.dev/controlplane/internal/vfs"
)

// sstManager manages SST files organized into levels for compaction.
type sstManager struct {
	mu      sync.Mutex
	vfs     vfs.VFS
	dir     string
	nextNum int
	levels  []map[int]struct {
		reader *dsst.Reader
		file   vfs.File
	} // SSTables organized by levels
	configs       dsst.SSTableConfigs // Default configs for new SSTables
	encryptionKey []byte
	compare       func(key1, key2 []byte) int
}

// newSSTManager creates a new sstManager.
func newSSTManager(fs vfs.VFS, dir string, encryptionKey []byte, compare func(key1, key2 []byte) int) (*sstManager, error) {
	m := &sstManager{
		vfs: fs,
		dir: dir,
		levels: []map[int]struct {
			reader *dsst.Reader
			file   vfs.File
		}{
			make(map[int]struct {
				reader *dsst.Reader
				file   vfs.File
			}), // Level 0
			make(map[int]struct {
				reader *dsst.Reader
				file   vfs.File
			}), // Level 1
			make(map[int]struct {
				reader *dsst.Reader
				file   vfs.File
			}), // Level 2
		},
		configs: dsst.SSTableConfigs{
			BlockSize:               32 * 1024, // 32KB
			RestartInterval:         16,
			BloomFilterBitsPerKey:   10, // Default bloom filter bits per key
			BloomFilterNumHashFuncs: 6,  // Default bloom filter hash functions
		},
		encryptionKey: encryptionKey,
		compare:       compare,
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

// load loads existing SST files into the appropriate levels based on metadata.
func (m *sstManager) load() error {
	names, err := m.vfs.List(m.dir)
	if err != nil {
		return fmt.Errorf("failed to list files in %s: %w", m.dir, err)
	}

	// Initially, place all SSTables in level 0 if metadata is not available or during initial load
	for _, name := range names {
		if !strings.HasSuffix(name, ".sst") {
			continue
		}
		baseName := filepath.Base(name)
		num, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSuffix(baseName, ".sst"), "sst-"))
		if err != nil {
			return fmt.Errorf("failed to parse sst file number from %s: %w", name, err)
		}
		if num >= m.nextNum {
			m.nextNum = num + 1
		}
		filePath := filepath.Join(m.dir, name) // Use full path
		f, err := m.vfs.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", filePath, err)
		}
		stat, err := f.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file %s: %w", filePath, err)
		}
		r, _, err := dsst.NewReader(f, stat.Size(), m.encryptionKey, m.compare)
		if err != nil {
			return fmt.Errorf("failed to create sst reader for %s: %w", filePath, err)
		}
		// Default to level 0 for now; level will be updated based on metadata in DB initialization
		m.levels[0][num] = struct {
			reader *dsst.Reader
			file   vfs.File
		}{reader: r, file: f}
	}
	return nil
}

// create creates a new SST file and returns the vfs.File and its configs.
func (m *sstManager) create() (vfs.File, dsst.SSTableConfigs, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	num := m.nextNum
	m.nextNum++
	name := fmt.Sprintf("sst-%06d.sst", num) // Use sst- prefix for consistency
	fullPath := filepath.Join(m.dir, name)
	f, err := m.vfs.Create(fullPath)
	if err != nil {
		return nil, dsst.SSTableConfigs{}, 0, err
	}
	return f, m.configs, num, nil
}

// addReader adds a new dsst.Reader to the manager's levels map at the specified level (default 0 for new SSTables).
func (m *sstManager) addReader(num int, reader *dsst.Reader, file vfs.File) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// When adding a reader from a flush, the file is already open and managed by the DB.Put/Delete flow.
	// New SSTables from memtable flush start at level 0
	m.levels[0][num] = struct {
		reader *dsst.Reader
		file   vfs.File
	}{reader: reader, file: file}
}

// get returns the value for the given key, searching through levels from newest to oldest.
func (m *sstManager) get(key []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Search through levels, starting from level 0 (newest)
	for level := 0; level < len(m.levels); level++ {
		var nums []int
		for num := range m.levels[level] {
			nums = append(nums, num)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(nums))) // Newest SSTables first within a level
		for _, num := range nums {
			r := m.levels[level][num].reader
			val, ok, err := r.Get(key)
			if err != nil {
				return nil, false, err
			}
			if ok {
				// If the key is found, check if it's a tombstone.
				// dsst.Reader.Get already returns nil for value if it's a tombstone.
				if val == nil {
					return nil, false, nil // Found a tombstone, key is deleted
				}
				return val, true, nil
			}
		}
	}
	return nil, false, nil
}

// Iterator returns a new iterator that iterates over all SSTables managed by this sstManager, level by level.
func (m *sstManager) Iterator() iterator.Iterator {
	m.mu.Lock()
	defer m.mu.Unlock()

	var iters []iterator.Iterator
	// Iterate through levels, starting from level 0 (newest)
	for level := 0; level < len(m.levels); level++ {
		var nums []int
		for num := range m.levels[level] {
			nums = append(nums, num)
		}
		// Iterate in reverse order within a level (newest first) to ensure correct key precedence.
		sort.Sort(sort.Reverse(sort.IntSlice(nums)))
		for _, num := range nums {
			iters = append(iters, m.levels[level][num].reader.Iterator())
		}
	}
	return iterator.NewMergeIterator(iters)
}

// compact performs leveled compaction on SSTables, moving them between levels or merging them as needed.
func (m *sstManager) compact(level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if level >= len(m.levels)-1 {
		return nil // Cannot compact beyond the last level
	}

	// Simple compaction strategy: if level 0 has more than 4 SSTables, compact to level 1
	if level == 0 && len(m.levels[0]) > 4 {
		// Select the oldest SSTables for compaction (lowest numbers)
		var toCompact []int
		for num := range m.levels[0] {
			toCompact = append(toCompact, num)
		}
		sort.Ints(toCompact) // Sort ascending to get oldest first
		if len(toCompact) > 4 {
			toCompact = toCompact[:4] // Limit to 4 SSTables for this compaction
		}

		// Move selected SSTables to level 1 (for simplicity, no merging yet)
		for _, num := range toCompact {
			entry := m.levels[0][num]
			delete(m.levels[0], num)
			m.levels[1][num] = entry
			fmt.Printf("Moved SST %d from level 0 to level 1 during compaction\n", num)
		}
	} else if level > 0 && len(m.levels[level]) > 8 {
		// For higher levels, if more than 8 SSTables, compact to next level
		var toCompact []int
		for num := range m.levels[level] {
			toCompact = append(toCompact, num)
		}
		sort.Ints(toCompact)
		if len(toCompact) > 8 {
			toCompact = toCompact[:8]
		}

		for _, num := range toCompact {
			entry := m.levels[level][num]
			delete(m.levels[level], num)
			m.levels[level+1][num] = entry
			fmt.Printf("Moved SST %d from level %d to level %d during compaction\n", num, level, level+1)
		}
	}
	return nil
}

// close closes the sstManager, closing all files in all levels.
func (m *sstManager) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for level := 0; level < len(m.levels); level++ {
		for num, entry := range m.levels[level] {
			if entry.file != nil { // Close all files that were opened by sstManager
				if err := entry.file.Close(); err != nil {
					return err
				}
			}
			// The reader itself doesn't need explicit closing if its underlying io.ReaderAt is closed.
			delete(m.levels[level], num)
		}
	}
	return nil
}
