package dsst

import (
	"io"
	"testing"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
)

func TestSSTRoundTrip(t *testing.T) {
	fs := vfs.NewMemFileSystem()
	f, err := fs.Create("test.sst")
	if err != nil {
		t.Fatal(err)
	}

	opts := DefaultOptions()
	opts.BlockSize = 1024 // Small block size to force multiple blocks
	opts.BloomBitsPerKey = 10

	w, err := NewWriter(f, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Write keys
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, k := range keys {
		if k == "c" {
			if err := w.AddDeleted([]byte(k)); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := w.Add([]byte(k), []byte("value-"+k)); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Add Range Tombstone
	if err := w.AddRangeTombstone([]byte("k"), []byte("z")); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Read back
	f, err = fs.Open("test.sst")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(f, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Check Properties
	if string(r.MinKey) != "a" {
		t.Errorf("MinKey mismatch: got %s, want a", r.MinKey)
	}
	if string(r.MaxKey) != "j" {
		t.Errorf("MaxKey mismatch: got %s, want j", r.MaxKey)
	}

	// Iterate
	it := r.NewIterator()
	defer it.Close()

	it.Seek([]byte("a"))
	if !it.Valid() {
		t.Fatal("Iterator invalid after seek")
	}
	if string(it.Key()) != "a" {
		t.Errorf("Expected key a, got %s", it.Key())
	}
	if string(it.Value()) != "value-a" {
		t.Errorf("Expected value value-a, got %s", it.Value())
	}
	if it.Kind() != 0 {
		t.Errorf("Expected kind 0 (Put), got %d", it.Kind())
	}

	it.Next() // b
	if string(it.Key()) != "b" {
		t.Errorf("Expected key b, got %s", it.Key())
	}

	it.Next() // c (deleted)
	if string(it.Key()) != "c" {
		t.Errorf("Expected key c, got %s", it.Key())
	}
	if it.Kind() != 1 {
		t.Errorf("Expected kind 1 (Delete), got %d", it.Kind())
	}

	// Check Range Tombstones
	rtIt, err := r.NewRangeTombstoneIterator()
	if err != nil {
		t.Fatal(err)
	}
	if rtIt == nil {
		t.Fatal("Expected range tombstones")
	}
	rtIt.SeekToFirst()
	if !rtIt.Next() {
		t.Fatal("Expected at least one range tombstone")
	}
	rt := rtIt.Tombstone()
	if string(rt.Start) != "k" || string(rt.End) != "z" {
		t.Errorf("Range tombstone mismatch: got [%s, %s), want [k, z)", rt.Start, rt.End)
	}
	if rtIt.Next() {
		t.Fatal("Expected only one range tombstone")
	}
}

func TestEncryptionFull(t *testing.T) {
	fs := vfs.NewMemFileSystem()
	f, err := fs.Create("encrypted.sst")
	if err != nil {
		t.Fatal(err)
	}

	key, _ := GenerateKey()
	opts := DefaultOptions()
	opts.EncryptionKey = key

	w, err := NewWriter(f, opts)
	if err != nil {
		t.Fatal(err)
	}
	w.Add([]byte("secret"), []byte("data"))
	w.Close()

	// Read with correct key
	f, _ = fs.Open("encrypted.sst")
	r, err := NewReader(f, opts)
	if err != nil {
		t.Fatal(err)
	}
	it := r.NewIterator()
	it.First()
	if !it.Valid() || string(it.Key()) != "secret" {
		t.Fatal("Failed to read encrypted data")
	}

	// Read with wrong key
	wrongKey, _ := GenerateKey()
	optsWrong := DefaultOptions()
	optsWrong.EncryptionKey = wrongKey
	f, _ = fs.Open("encrypted.sst")
	_, err = NewReader(f, optsWrong)
	if err == nil {
		t.Fatal("Expected error reading with wrong key")
	}
}

func TestCorruption(t *testing.T) {
	fs := vfs.NewMemFileSystem()
	f, err := fs.Create("corrupt.sst")
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWriter(f, DefaultOptions())
	w.Add([]byte("a"), []byte("b"))
	w.Close()

	// Corrupt file content
	f, err = fs.Open("corrupt.sst")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Flip a bit in the middle (likely data block)
	data[len(data)/2] ^= 0xFF

	// Write back
	f, err = fs.Open("corrupt.sst") // Open for writing (MemFS Open returns ReadWriter handle if it exists)
	// Wait, MemFS Open returns a handle that shares data but has independent pos.
	// But we need to overwrite. MemFS doesn't have Truncate or similar easily exposed via File interface?
	// File interface has WriterAt.
	// Let's just use WriteAt on the handle we get from Open.
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	f, _ = fs.Open("corrupt.sst")
	r, err := NewReader(f, DefaultOptions())
	if err != nil {
		// It might fail at header/footer load
		return
	}
	// If reader opened, iterator should fail
	it := r.NewIterator()
	it.First()
	if it.Valid() {
		t.Fatal("Expected iterator to be invalid due to corruption")
	}
}

func TestBloomFilter(t *testing.T) {
	fs := vfs.NewMemFileSystem()
	f, err := fs.Create("bloom.sst")
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.BloomBitsPerKey = 10
	w, _ := NewWriter(f, opts)
	w.Add([]byte("exists"), []byte("v"))
	w.Close()

	f, _ = fs.Open("bloom.sst")
	r, err := NewReader(f, opts)
	if err != nil {
		t.Fatal(err)
	}

	if !r.filter.KeyMayMatch([]byte("exists"), r.filterData) {
		t.Error("Bloom filter should contain 'exists'")
	}
	// "missing" might be false positive, but usually not with 10 bits
	// We can't assert false strictly, but we can check it runs.
}
