package dwal

import (
	"crypto/rand"
	"io"
	"testing"

	"pkg.gfire.dev/controlplane/internal/vfs"
)

func BenchmarkWAL_WriteEntry(b *testing.B) {
	dir := b.TempDir()
	fs := vfs.NewOSVFS()
	key := make([]byte, 32) // AES-256 key
	_, err := io.ReadFull(rand.Reader, key)
	if err != nil {
		b.Fatalf("failed to generate key: %v", err)
	}
	wal, err := NewWAL(fs, dir, 64*1024*1024, key) // Default segment size
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	data := make([]byte, 1024) // 1KB data
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := wal.WriteEntry(data)
		if err != nil {
			b.Fatalf("failed to write entry: %v", err)
		}
	}
}

func BenchmarkWAL_WriteEntry_Small(b *testing.B) {
	dir := b.TempDir()
	fs := vfs.NewOSVFS()
	key := make([]byte, 32) // AES-256 key
	_, err := io.ReadFull(rand.Reader, key)
	if err != nil {
		b.Fatalf("failed to generate key: %v", err)
	}
	wal, err := NewWAL(fs, dir, 64*1024*1024, key) // Default segment size
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	data := make([]byte, 128) // 128 bytes data
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := wal.WriteEntry(data)
		if err != nil {
			b.Fatalf("failed to write entry: %v", err)
		}
	}
}

func BenchmarkWAL_WriteEntry_Large(b *testing.B) {
	dir := b.TempDir()
	fs := vfs.NewOSVFS()
	key := make([]byte, 32) // AES-256 key
	_, err := io.ReadFull(rand.Reader, key)
	if err != nil {
		b.Fatalf("failed to generate key: %v", err)
	}
	wal, err := NewWAL(fs, dir, 64*1024*1024, key) // Default segment size
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	data := make([]byte, 4*1024) // 4KB data
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := wal.WriteEntry(data)
		if err != nil {
			b.Fatalf("failed to write entry: %v", err)
		}
	}
}
