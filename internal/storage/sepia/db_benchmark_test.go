package sepia

import (
	"fmt"
	"testing"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
)

func BenchmarkDB_Put(b *testing.B) {
	fs := vfs.NewMemFileSystem()
	opts := &Options{
		FileSystem: fs,
		Dir:        "/tmp/sepia-bench-put",
	}
	fs.MkdirAll(opts.Dir, 0755)

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkDB_Get(b *testing.B) {
	fs := vfs.NewMemFileSystem()
	opts := &Options{
		FileSystem: fs,
		Dir:        "/tmp/sepia-bench-get",
	}
	fs.MkdirAll(opts.Dir, 0755)

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Pre-populate
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		if err := db.Put(key, val); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i%numKeys))
		if _, err := db.Get(key); err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}
