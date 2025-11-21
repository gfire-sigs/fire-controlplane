package dsst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
)

// TestSSTWriterReader tests writing and reading an SST file.
func TestSSTWriterReader(t *testing.T) {
	fs := vfs.NewMemFileSystem()
	f, err := fs.Create("test.sst")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	opts := DefaultOptions()
	opts.BlockSize = 1024 // Small block size for testing multiple blocks

	// Generate random keys
	n := 1000
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}
	// Sort keys (writer expects sorted keys)
	// Actually, fmt.Sprintf with %04d is already sorted lexicographically.

	w, err := NewWriter(f, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	for _, k := range keys {
		if err := w.Add([]byte(k), []byte("val-"+k)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file size
	stat, _ := f.Stat()
	if stat.Size() == 0 {
		t.Fatal("File is empty")
	}
	t.Logf("SST file size: %d bytes", stat.Size())

	// Read back
	// Re-open file
	f, err = fs.Open("test.sst")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	r, err := NewReader(f, opts)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	it := r.NewIterator()
	it.Seek([]byte("key-0000"))
	if !it.Valid() {
		t.Fatal("Seek failed")
	}

	count := 0
	for ; it.Valid(); it.Next() {
		k := string(it.Key())
		v := string(it.Value())
		expectedK := keys[count]
		expectedV := "val-" + expectedK
		if k != expectedK {
			t.Errorf("Key mismatch at %d: got %s, want %s", count, k, expectedK)
		}
		if v != expectedV {
			t.Errorf("Value mismatch at %d: got %s, want %s", count, v, expectedV)
		}
		count++
	}

	if count != n {
		t.Errorf("Read %d keys, want %d", count, n)
	}

	// Verify Metadata
	if string(r.MinKey) != keys[0] {
		t.Errorf("MinKey mismatch: got %s, want %s", r.MinKey, keys[0])
	}
	if string(r.MaxKey) != keys[n-1] {
		t.Errorf("MaxKey mismatch: got %s, want %s", r.MaxKey, keys[n-1])
	}
	// TS was not extracted, so should be default
	if r.MinTS != ^uint64(0) { // MaxUint64
		t.Errorf("MinTS mismatch: got %d, want MaxUint64", r.MinTS)
	}
	if r.MaxTS != 0 {
		t.Errorf("MaxTS mismatch: got %d, want 0", r.MaxTS)
	}

	// Test Seek
	it.Seek([]byte("key-0500"))
	if !it.Valid() {
		t.Fatal("Seek(key-0500) failed")
	}
	if string(it.Key()) != "key-0500" {
		t.Errorf("Seek(key-0500) got %s", string(it.Key()))
	}
}

// TestEncryption verifies that data is actually encrypted.
func TestEncryption(t *testing.T) {
	fs := vfs.NewMemFileSystem()
	f, err := fs.Create("test_enc.sst")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	opts := DefaultOptions()
	opts.BlockSize = 1024

	key, _ := GenerateKey()
	opts.EncryptionKey = key

	w, err := NewWriter(f, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	secret := "this-is-secret-data"
	if err := w.Add([]byte("key-1"), []byte(secret)); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	w.Close()

	// Check if secret appears in file data
	// We can't easily access internal buffer of MemFS from here without casting,
	// but we can read it back as a file.
	f, err = fs.Open("test_enc.sst")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	content, _ := io.ReadAll(f)
	if bytes.Contains(content, []byte(secret)) {
		t.Error("Found plaintext data in encrypted file!")
	}

	// Read back with correct key
	f.Seek(0, io.SeekStart)
	r, err := NewReader(f, opts)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	it := r.NewIterator()
	it.Seek([]byte("key-1"))
	if !it.Valid() || string(it.Value()) != secret {
		t.Error("Failed to read back encrypted data")
	}

	// Read back with wrong key
	wrongOpts := DefaultOptions()
	wrongOpts.EncryptionKey, _ = GenerateKey()
	f.Seek(0, io.SeekStart)
	_, err = NewReader(f, wrongOpts)
	// It might fail at DEK decryption (GCM auth failure)
	if err == nil {
		t.Error("NewReader should fail with wrong key")
	}
}

func TestCorruptSST(t *testing.T) {
	fs := vfs.NewMemFileSystem()
	f, err := fs.Create("test_corrupt.sst")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	opts := DefaultOptions()

	w, err := NewWriter(f, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	w.Add([]byte("key"), []byte("value"))
	w.Close()

	// Corrupt Magic
	// We need to modify the file content directly.
	// Since we are using vfs.MemFS, we can write to it.
	f, err = fs.Open("test_corrupt.sst")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Read all, modify, write back? Or just WriteAt.
	// MemFS supports WriteAt.

	f.WriteAt([]byte{0x00}, 0)
	f.Seek(0, io.SeekStart)
	if _, err := NewReader(f, opts); err == nil {
		t.Error("NewReader should fail with corrupt magic")
	}
	f.WriteAt([]byte{0x46}, 0) // Restore 'F'

	// Corrupt Footer Magic
	stat, _ := f.Stat()
	last := stat.Size() - 1

	// Read original byte
	var buf [1]byte
	f.ReadAt(buf[:], last)
	orig := buf[0]

	f.WriteAt([]byte{0x00}, last)
	f.Seek(0, io.SeekStart)
	if _, err := NewReader(f, opts); err == nil {
		t.Error("NewReader should fail with corrupt footer magic")
	}
	f.WriteAt([]byte{orig}, last)
}

func TestSSTMetadata(t *testing.T) {
	fs := vfs.NewMemFileSystem()
	f, err := fs.Create("test_meta.sst")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	opts := DefaultOptions()
	opts.KeyTSExtractor = func(key []byte) uint64 {
		if len(key) < 8 {
			return 0
		}
		return binary.BigEndian.Uint64(key[len(key)-8:])
	}

	w, err := NewWriter(f, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Add keys with timestamps
	// Key format: prefix + TS (8 bytes)
	// We want sorted keys.
	// key-1 (ts=100), key-2 (ts=200), key-3 (ts=50)
	// Sorted: key-1, key-2, key-3.
	// MinKey: key-1..., MaxKey: key-3...
	// MinTS: 50, MaxTS: 200.

	makeKey := func(prefix string, ts uint64) []byte {
		k := []byte(prefix)
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], ts)
		return append(k, buf[:]...)
	}

	w.Add(makeKey("key-1", 100), []byte("val1"))
	w.Add(makeKey("key-2", 200), []byte("val2"))
	w.Add(makeKey("key-3", 50), []byte("val3"))
	w.Close()

	// Read back
	f, _ = fs.Open("test_meta.sst")
	r, err := NewReader(f, opts)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	if r.MinTS != 50 {
		t.Errorf("MinTS mismatch: got %d, want 50", r.MinTS)
	}
	if r.MaxTS != 200 {
		t.Errorf("MaxTS mismatch: got %d, want 200", r.MaxTS)
	}

	minKey := makeKey("key-1", 100)
	if !bytes.Equal(r.MinKey, minKey) {
		t.Errorf("MinKey mismatch")
	}
	maxKey := makeKey("key-3", 50)
	if !bytes.Equal(r.MaxKey, maxKey) {
		t.Errorf("MaxKey mismatch")
	}
}
