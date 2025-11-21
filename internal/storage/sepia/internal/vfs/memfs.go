package vfs

import (
	"io"
	"os"
	"sync"
	"time"
)

// MemFileSystem implements FileSystem in memory.
type MemFileSystem struct {
	mu    sync.Mutex
	files map[string]*memFile
}

// NewMemFileSystem creates a new in-memory file system.
func NewMemFileSystem() *MemFileSystem {
	return &MemFileSystem{
		files: make(map[string]*memFile),
	}
}

func (fs *MemFileSystem) Create(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f := &memFile{name: name, fs: fs}
	fs.files[name] = f
	return &memFileHandle{f: f}, nil
}

func (fs *MemFileSystem) Open(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f, ok := fs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	// Return a new reader/writer pointing to the same data but independent pos
	return &memFileHandle{f: f}, nil
}

func (fs *MemFileSystem) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.files, name)
	return nil
}

func (fs *MemFileSystem) Rename(oldpath, newpath string) error {
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

func (fs *MemFileSystem) Stat(name string) (os.FileInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f, ok := fs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return &memFileInfo{name: f.name, size: int64(len(f.data))}, nil
}

type memFile struct {
	name string
	data []byte
	mu   sync.Mutex
	fs   *MemFileSystem
}

type memFileHandle struct {
	f   *memFile
	pos int64
}

func (h *memFileHandle) Close() error { return nil }

func (h *memFileHandle) Read(p []byte) (n int, err error) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	if h.pos >= int64(len(h.f.data)) {
		return 0, io.EOF
	}
	n = copy(p, h.f.data[h.pos:])
	h.pos += int64(n)
	return n, nil
}

func (h *memFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	if off >= int64(len(h.f.data)) {
		return 0, io.EOF
	}
	n = copy(p, h.f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (h *memFileHandle) Write(p []byte) (n int, err error) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	// Expand if needed
	if h.pos+int64(len(p)) > int64(len(h.f.data)) {
		newData := make([]byte, h.pos+int64(len(p)))
		copy(newData, h.f.data)
		h.f.data = newData
	}
	n = copy(h.f.data[h.pos:], p)
	h.pos += int64(n)
	return n, nil
}

func (h *memFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	if off+int64(len(p)) > int64(len(h.f.data)) {
		newData := make([]byte, off+int64(len(p)))
		copy(newData, h.f.data)
		h.f.data = newData
	}
	n = copy(h.f.data[off:], p)
	return n, nil
}

func (h *memFileHandle) Seek(offset int64, whence int) (int64, error) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	switch whence {
	case io.SeekStart:
		h.pos = offset
	case io.SeekCurrent:
		h.pos += offset
	case io.SeekEnd:
		h.pos = int64(len(h.f.data)) + offset
	}
	return h.pos, nil
}

func (h *memFileHandle) Stat() (os.FileInfo, error) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	return &memFileInfo{name: h.f.name, size: int64(len(h.f.data))}, nil
}

func (h *memFileHandle) Sync() error { return nil }

type memFileInfo struct {
	name string
	size int64
}

func (fi *memFileInfo) Name() string       { return fi.name }
func (fi *memFileInfo) Size() int64        { return fi.size }
func (fi *memFileInfo) Mode() os.FileMode  { return 0644 }
func (fi *memFileInfo) ModTime() time.Time { return time.Time{} }
func (fi *memFileInfo) IsDir() bool        { return false }
func (fi *memFileInfo) Sys() interface{}   { return nil }

func (fs *MemFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return nil
}

func (fs *MemFileSystem) List(dir string) ([]string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var names []string
	for name := range fs.files {
		// Simple prefix check for now, assuming flat structure or full paths
		// In this test usage, we just want to find the WAL file.
		names = append(names, name)
	}
	return names, nil
}
