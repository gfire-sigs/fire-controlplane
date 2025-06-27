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

// sstManager manages SST files.
type sstManager struct {
	mu      sync.Mutex
	vfs     vfs.VFS
	dir     string
	nextNum int
	files   map[int]struct {
		reader *dsst.Reader
		file   vfs.File
	}
	encryptionKey []byte
}

// newSSTManager creates a new sstManager.
func newSSTManager(fs vfs.VFS, dir string, encryptionKey []byte) (*sstManager, error) {
	m := &sstManager{
		vfs: fs,
		dir: dir,
		files: make(map[int]struct {
			reader *dsst.Reader
			file   vfs.File
		}),
		encryptionKey: encryptionKey,
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *sstManager) load() error {
	names, err := m.vfs.List(m.dir)
	if err != nil {
		return fmt.Errorf("failed to list files in %s: %w", m.dir, err)
	}

	for _, name := range names {
		if !strings.HasSuffix(name, ".sst") {
			continue
		}
		baseName := filepath.Base(name)
		num, err := strconv.Atoi(strings.TrimSuffix(baseName, ".sst"))
		if err != nil {
			return fmt.Errorf("failed to parse sst file number from %s: %w", name, err)
		}
		if num >= m.nextNum {
			m.nextNum = num + 1
		}
		filePath := name
		f, err := m.vfs.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", filePath, err)
		}
		stat, err := f.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file %s: %w", filePath, err)
		}
		r, _, err := dsst.NewReader(f, stat.Size(), m.encryptionKey)
		if err != nil {
			return fmt.Errorf("failed to create sst reader for %s: %w", filePath, err)
		}
		m.files[num] = struct {
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
	name := fmt.Sprintf("%06d.sst", num)
	fullPath := filepath.Join(m.dir, name)
	fmt.Printf("sstManager.create: creating file %s\n", fullPath)
	f, err := m.vfs.Create(fullPath)
	if err != nil {
		fmt.Printf("sstManager.create: error creating file: %v\n", err)
		return nil, dsst.SSTableConfigs{}, 0, err
	}
	fmt.Printf("sstManager.create: file %s created successfully\n", fullPath)
	// Default SSTable configs for now
	configs := dsst.SSTableConfigs{
		BlockSize:       32 * 1024, // 32KB
		RestartInterval: 16,
	}
	return f, configs, num, nil
}

// addReader adds a new dsst.Reader to the manager's files map.
func (m *sstManager) addReader(num int, reader *dsst.Reader, file vfs.File) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// When adding a reader from a flush, the file is already open and managed by the DB.Put/Delete flow.
	// We need to ensure we don't close it prematurely.
	// Store the file so it can be closed later.
	m.files[num] = struct {
		reader *dsst.Reader
		file   vfs.File
	}{reader: reader, file: file}
}

// get returns the value for the given key.
func (m *sstManager) get(key []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var files []int
	for num := range m.files {
		files = append(files, num)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(files)))
	for _, num := range files {
		r := m.files[num].reader
		val, ok, err := r.Get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			// If the key is found, check if it's a tombstone.
			// dsst.Reader.Get already returns nil for value if it's a tombstone.
			// So, if val is nil, it means it's a tombstone.
			if val == nil {
				return nil, false, nil // Found a tombstone, key is deleted
			}
			return val, true, nil
		}
	}
	return nil, false, nil
}

// Iterator returns a new iterator that iterates over all SSTables managed by this sstManager.
func (m *sstManager) Iterator() iterator.Iterator {
	m.mu.Lock()
	defer m.mu.Unlock()

	var readers []*dsst.Reader
	var nums []int
	for num := range m.files {
		nums = append(nums, num)
	}
	// Iterate in reverse order (newest first) to ensure correct key precedence.
	sort.Sort(sort.Reverse(sort.IntSlice(nums)))

	for _, num := range nums {
		readers = append(readers, m.files[num].reader)
	}

	var iters []iterator.Iterator
	for _, reader := range readers {
		iters = append(iters, reader.Iterator())
	}
	return iterator.NewMergeIterator(iters)
}

// close closes the sstManager.
func (m *sstManager) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for num, entry := range m.files {
		if entry.file != nil { // Close all files that were opened by sstManager
			if err := entry.file.Close(); err != nil {
				return err
			}
		}
		// The reader itself doesn't need explicit closing if its underlying io.ReaderAt is closed.
		delete(m.files, num)
	}
	return nil
}
