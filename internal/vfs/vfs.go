package vfs

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// File represents a file in the virtual file system.
type File interface {
	io.ReadWriteCloser
	io.ReaderAt
	io.WriterAt
	Sync() error
	Stat() (os.FileInfo, error)
}

// VFS is a virtual file system interface.
type VFS interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	Remove(name string) error
	Rename(oldpath, newpath string) error
	Exists(name string) (bool, error)
	List(dir string) ([]string, error)
}

// osVFS is a VFS implementation that uses the OS file system.
type osVFS struct{}

// NewOSVFS creates a new osVFS.
func NewOSVFS() VFS {
	return &osVFS{}
}

func (fs *osVFS) Create(name string) (File, error) {
	return os.Create(name)
}

func (fs *osVFS) Open(name string) (File, error) {
	return os.Open(name)
}

func (fs *osVFS) Remove(name string) error {
	return os.Remove(name)
}

func (fs *osVFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (fs *osVFS) Exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (fs *osVFS) List(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdirnames(-1)
}

// memVFS is a VFS implementation that uses memory.
type memVFS struct {
	mu    sync.Mutex
	files map[string]*memFile
}

// NewMemVFS creates a new memVFS.
func NewMemVFS() VFS {
	return &memVFS{
		files: make(map[string]*memFile),
	}
}
func (fs *memVFS) Create(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f := newMemFile(name)
	fs.files[name] = f
	return f, nil
}

func (fs *memVFS) Open(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	existingFile, ok := fs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	// Return a new memFile instance with a copy of the data
	// This simulates opening a file and getting a new file descriptor
	newFile := newMemFile(name)
	newFile.data = make([]byte, len(existingFile.data))
	copy(newFile.data, existingFile.data)
	newFile.modTime = existingFile.modTime
	return newFile, nil
}

func (fs *memVFS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.files[name]; !ok {
		return os.ErrNotExist
	}
	delete(fs.files, name)
	return nil
}

func (fs *memVFS) Rename(oldpath, newpath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f, ok := fs.files[oldpath]
	if !ok {
		return os.ErrNotExist
	}
	delete(fs.files, oldpath)
	f.name = newpath
	fs.files[newpath] = f
	return nil
}

func (fs *memVFS) Exists(name string) (bool, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_, ok := fs.files[name]
	return ok, nil
}

func (fs *memVFS) List(dir string) ([]string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var names []string

	// Normalize directory path
	cleanDir := filepath.Clean(dir)
	if cleanDir == "." || cleanDir == "./" {
		cleanDir = ""
	} else if cleanDir != "" && !strings.HasSuffix(cleanDir, "/") {
		cleanDir += "/"
	}

	for name := range fs.files {
		if cleanDir == "" {
			// For root directory, include all top-level files
			if !strings.Contains(name, "/") {
				names = append(names, name)
			}
		} else if strings.HasPrefix(name, cleanDir) {
			// Check if file is directly in directory (no nested paths)
			remaining := strings.TrimPrefix(name, cleanDir)
			if !strings.Contains(remaining, "/") {
				names = append(names, name)
			}
		}
	}

	sort.Strings(names)
	return names, nil
}

// memFile is a file implementation that uses memory.
type memFile struct {
	name    string
	mu      sync.Mutex
	data    []byte
	modTime time.Time
	closed  bool
}

func newMemFile(name string) *memFile {
	return &memFile{
		name:    name,
		modTime: time.Now(),
	}
}

func (f *memFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if len(f.data) == 0 {
		return 0, io.EOF
	}
	n = copy(p, f.data)
	f.data = f.data[n:]
	return n, nil
}

func (f *memFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	f.data = append(f.data, p...)
	f.modTime = time.Now()
	return len(p), nil
}

func (f *memFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *memFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n = copy(p, f.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (f *memFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if off+int64(len(p)) > int64(len(f.data)) {
		newData := make([]byte, off+int64(len(p)))
		copy(newData, f.data)
		f.data = newData
	}
	n = copy(f.data[off:], p)
	f.modTime = time.Now()
	return n, nil
}

func (f *memFile) Sync() error {
	return nil
}

func (f *memFile) Stat() (os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &memFileInfo{
		name:    f.name,
		size:    int64(len(f.data)),
		modTime: f.modTime,
	}, nil
}

// memFileInfo is a file info implementation that uses memory.
type memFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (fi *memFileInfo) Name() string {
	return fi.name
}

func (fi *memFileInfo) Size() int64 {
	return fi.size
}

func (fi *memFileInfo) Mode() os.FileMode {
	return 0644
}

func (fi *memFileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi *memFileInfo) IsDir() bool {
	return false
}

func (fi *memFileInfo) Sys() interface{} {
	return nil
}
