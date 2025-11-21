package vfs

import (
	"os"
)

// DirFileSystem implements FileSystem using the local file system.
type DirFileSystem struct {
	// root is the root directory for this file system.
	// If empty, it uses the current working directory.
	root string
}

// NewDirFileSystem creates a new DirFileSystem.
func NewDirFileSystem(root string) *DirFileSystem {
	return &DirFileSystem{root: root}
}

func (fs *DirFileSystem) Create(name string) (File, error) {
	return os.Create(name)
}

func (fs *DirFileSystem) Open(name string) (File, error) {
	return os.OpenFile(name, os.O_RDWR, 0644)
}

func (fs *DirFileSystem) Remove(name string) error {
	return os.Remove(name)
}

func (fs *DirFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *DirFileSystem) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (fs *DirFileSystem) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *DirFileSystem) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names, nil
}
