package vfs

import (
	"io"
	"os"
)

// File is the interface for a file in the VFS.
type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Seeker
	Stat() (os.FileInfo, error)
	Sync() error
}

// FileSystem is the interface for a virtual file system.
type FileSystem interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	Remove(name string) error
	Rename(oldpath, newpath string) error
	Stat(name string) (os.FileInfo, error)
	MkdirAll(path string, perm os.FileMode) error
}
