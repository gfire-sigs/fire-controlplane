package sepia

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"pkg.gfire.dev/controlplane/internal/vfs"
)

func TestDBSimple(t *testing.T) {
	vfs := vfs.NewMemVFS()
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024, // Small arena to trigger flushing
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	// Put and Get a simple key-value pair
	key1 := []byte("hello")
	value1 := []byte("world")
	if err := db.Put(key1, value1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrievedValue, ok, err := db.Get(key1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok {
		t.Fatal("Get failed: key not found")
	}
	if !bytes.Equal(retrievedValue, value1) {
		t.Fatalf("Get returned wrong value: got %s, want %s", retrievedValue, value1)
	}

	// Update an existing key
	value1Updated := []byte("world_updated")
	if err := db.Put(key1, value1Updated); err != nil {
		t.Fatalf("Put update failed: %v", err)
	}
	retrievedValue, ok, err = db.Get(key1)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if !ok {
		t.Fatal("Get after update failed: key not found")
	}
	if !bytes.Equal(retrievedValue, value1Updated) {
		t.Fatalf("Get after update returned wrong value: got %s, want %s", retrievedValue, value1Updated)
	}

	// Get a non-existent key
	keyNonExistent := []byte("nonexistent")
	_, ok, err = db.Get(keyNonExistent)
	if err != nil {
		t.Fatalf("Get non-existent key failed: %v", err)
	}
	if ok {
		t.Fatal("Get non-existent key found it, but shouldn't have")
	}
}

func TestDBEdgeCases(t *testing.T) {
	vfs := vfs.NewMemVFS()
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024, // Small arena to trigger flushing
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	// Test empty key
	keyEmpty := []byte("")
	valueEmpty := []byte("empty_key_value")
	if err := db.Put(keyEmpty, valueEmpty); err != nil {
		t.Fatalf("Put with empty key failed: %v", err)
	}
	retrievedValue, ok, err := db.Get(keyEmpty)
	if err != nil {
		t.Fatalf("Get with empty key failed: %v", err)
	}
	if !ok {
		t.Fatal("Get with empty key failed: key not found")
	}
	if !bytes.Equal(retrievedValue, valueEmpty) {
		t.Fatalf("Get with empty key returned wrong value: got %s, want %s", retrievedValue, valueEmpty)
	}

	// Test empty value
	keyEmptyValue := []byte("key_empty_value")
	valueEmpty2 := []byte("")
	if err := db.Put(keyEmptyValue, valueEmpty2); err != nil {
		t.Fatalf("Put with empty value failed: %v", err)
	}
	retrievedValue, ok, err = db.Get(keyEmptyValue)
	if err != nil {
		t.Fatalf("Get with empty value failed: %v", err)
	}
	if !ok {
		t.Fatal("Get with empty value failed: key not found")
	}
	if !bytes.Equal(retrievedValue, valueEmpty2) {
		t.Fatalf("Get with empty value returned wrong value: got %s, want %s", retrievedValue, valueEmpty2)
	}

	// Test very long key and value
	longKey := make([]byte, 2048)
	longValue := make([]byte, 4096)
	for i := 0; i < len(longKey); i++ {
		longKey[i] = byte('a' + (i % 26))
	}
	for i := 0; i < len(longValue); i++ {
		longValue[i] = byte('b' + (i % 26))
	}

	if err := db.Put(longKey, longValue); err != nil {
		t.Fatalf("Put with long key/value failed: %v", err)
	}
	retrievedValue, ok, err = db.Get(longKey)
	if err != nil {
		t.Fatalf("Get with long key/value failed: %v", err)
	}
	if !ok {
		t.Fatal("Get with long key/value failed: key not found")
	}
	if !bytes.Equal(retrievedValue, longValue) {
		t.Fatalf("Get with long key/value returned wrong value: got len %d, want len %d", len(retrievedValue), len(longValue))
	}

	// Test special characters in key/value
	specialKey := []byte("!@#$%^&*()_+{}[]|\\;:'\"<>,./?`~")
	specialValue := []byte("한글漢字日本語")
	if err := db.Put(specialKey, specialValue); err != nil {
		t.Fatalf("Put with special characters failed: %v", err)
	}
	retrievedValue, ok, err = db.Get(specialKey)
	if err != nil {
		t.Fatalf("Get with special characters failed: %v", err)
	}
	if !ok {
		t.Fatal("Get with special characters failed: key not found")
	}
	if !bytes.Equal(retrievedValue, specialValue) {
		t.Fatalf("Get with special characters returned wrong value: got %s, want %s", retrievedValue, specialValue)
	}
}

func TestDBFlush(t *testing.T) {
	vfs := vfs.NewMemVFS()
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024, // Small arena to trigger flushing
		DataDir:   "database",
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	// Insert more data than the arena can hold to trigger a flush.
	numInsertions := 500
	for i := 0; i < numInsertions; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed for key %s: %v", key, err)
		}
	}

	// Explicitly close the DB to force a flush of the memtable to disk.
	if err := db.Close(); err != nil {
		t.Fatalf("DB Close failed: %v", err)
	}

	// Reopen the DB to read from the flushed SSTables

	db, err = NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed on reopen: %v", err)
	}

	defer db.Close()

	// Verify that we can still get all the data (from both SST and memtable).
	for i := 0; i < numInsertions; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		retrievedValue, ok, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get failed for key %s: %v", key, err)
		}

		if !ok {
			t.Fatalf("Get failed: key %s not found after flush", key)
		}
		if !bytes.Equal(retrievedValue, value) {
			t.Fatalf("Get returned wrong value for key %s: got %s, want %s", key, retrievedValue, value)
		}
	}

	// Update some keys after flush
	for i := 0; i < numInsertions/2; i += 2 { // Update every other key
		key := []byte(fmt.Sprintf("key-%03d", i))
		newValue := []byte(fmt.Sprintf("updated-value-%d", i))
		if err := db.Put(key, newValue); err != nil {
			t.Fatalf("Put update failed for key %s: %v", key, err)
		}
	}

	// Verify updated keys
	for i := 0; i < numInsertions/2; i += 2 {
		key := []byte(fmt.Sprintf("key-%03d", i))
		expectedValue := []byte(fmt.Sprintf("updated-value-%d", i))
		retrievedValue, ok, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get failed for updated key %s: %v", key, err)
		}
		if !ok {
			t.Fatalf("Get failed: updated key %s not found", key)
		}
		if !bytes.Equal(retrievedValue, expectedValue) {
			t.Fatalf("Get returned wrong value for updated key %s: got %s, want %s", key, retrievedValue, expectedValue)
		}
	}

	// Verify non-updated keys are still correct
	for i := 1; i < numInsertions/2; i += 2 {
		key := []byte(fmt.Sprintf("key-%03d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))
		retrievedValue, ok, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get failed for non-updated key %s: %v", key, err)
		}
		if !ok {
			t.Fatalf("Get failed: non-updated key %s not found", key)
		}
		if !bytes.Equal(retrievedValue, expectedValue) {
			t.Fatalf("Get returned wrong value for non-updated key %s: got %s, want %s", key, retrievedValue, expectedValue)
		}
	}

	// Test Iterator after flush and updates
	t.Run("Iterator after flush and updates", func(t *testing.T) {
		iter := db.Iterator()
		defer iter.Close()

		count := 0
		for iter.First(); iter.Valid(); iter.Next() {
			count++
			key := iter.Key()
			value := iter.Value()

			// Verify key and value consistency
			expectedValue, ok, err := db.Get(key)
			if err != nil {
				t.Fatalf("Iterator verification: Get failed for key %s: %v", key, err)
			}
			if !ok {
				t.Fatalf("Iterator verification: Key %s not found via Get", key)
			}
			if !bytes.Equal(value, expectedValue) {
				t.Fatalf("Iterator verification: Value mismatch for key %s. Got %s, want %s", key, value, expectedValue)
			}
		}
		if count != numInsertions {
			t.Errorf("Iterator count mismatch. Expected %d, got %d", numInsertions, count)
		}
	})

	// Test deletion and persistence after flush
	t.Run("Delete after flush", func(t *testing.T) {
		keyToDelete := []byte("key-to-be-deleted")
		valueToDelete := []byte("value-to-be-deleted")

		if err := db.Put(keyToDelete, valueToDelete); err != nil {
			t.Fatalf("Put keyToDelete failed: %v", err)
		}

		// Verify it exists
		_, ok, err := db.Get(keyToDelete)
		if err != nil {
			t.Fatalf("Get keyToDelete failed: %v", err)
		}
		if !ok {
			t.Fatalf("keyToDelete not found before delete")
		}

		// Delete the key
		if err := db.Delete(keyToDelete); err != nil {
			t.Fatalf("Delete keyToDelete failed: %v", err)
		}

		// Verify it's gone from memtable
		_, ok, err = db.Get(keyToDelete)
		if err != nil {
			t.Fatalf("Get keyToDelete failed after delete: %v", err)
		}
		if ok {
			t.Fatalf("keyToDelete found after delete in memtable, but shouldn't be")
		}

		// Close and reopen to force flush and read from SSTable
		if err := db.Close(); err != nil {
			t.Fatalf("DB Close failed before re-opening for delete test: %v", err)
		}
		db, err = NewDB(opts)
		if err != nil {
			t.Fatalf("NewDB failed on reopen for delete test: %v", err)
		}
		defer db.Close() // Ensure this new DB instance is closed

		// Verify it's still gone after reopen (from SSTable)
		_, ok, err = db.Get(keyToDelete)
		if err != nil {
			t.Fatalf("Get keyToDelete failed after reopen and delete: %v", err)
		}
		if ok {
			t.Fatalf("keyToDelete found after reopen and delete from SSTable, but shouldn't be")
		}
	})
}

func TestDBDelete(t *testing.T) {
	vfs := vfs.NewMemVFS()
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024, // Small arena to trigger flushing
		DataDir:   "database",
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	key1 := []byte("key-to-delete-1")
	value1 := []byte("value-to-delete-1")
	key2 := []byte("key-to-delete-2")
	value2 := []byte("value-to-delete-2")
	keyNonExistent := []byte("non-existent-key")

	// Put some keys
	if err := db.Put(key1, value1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(key2, value2); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify keys exist
	_, ok, err := db.Get(key1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok {
		t.Fatalf("Key %s not found before delete", key1)
	}

	// Delete key1
	if err := db.Delete(key1); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify key1 is no longer found
	_, ok, err = db.Get(key1)
	if err != nil {
		t.Fatalf("Get failed after delete: %v", err)
	}
	if ok {
		t.Fatalf("Key %s found after delete, but shouldn't be", key1)
	}

	// Verify key2 still exists
	_, ok, err = db.Get(key2)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok {
		t.Fatalf("Key %s not found after deleting another key", key2)
	}

	// Delete a non-existent key (should not error)
	if err := db.Delete(keyNonExistent); err != nil {
		t.Fatalf("Delete non-existent key failed: %v", err)
	}

	// Test iterator after deletion in memtable
	t.Run("IteratorAfterMemtableDelete", func(t *testing.T) {
		iter := db.Iterator()
		defer iter.Close()

		foundKey1 := false
		foundKey2 := false
		for iter.First(); iter.Valid(); iter.Next() {
			if bytes.Equal(iter.Key(), key1) {
				foundKey1 = true
			}
			if bytes.Equal(iter.Key(), key2) {
				foundKey2 = true
			}
		}
		if foundKey1 {
			t.Fatalf("Iterator found deleted key %s in memtable", key1)
		}
		if !foundKey2 {
			t.Fatalf("Iterator did not find existing key %s in memtable", key2)
		}
	})

	// Force flush to disk and reopen DB to check persistence of tombstone
	if err := db.Close(); err != nil {
		t.Fatalf("DB Close failed: %v", err)
	}
	db, err = NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed on reopen: %v", err)
	}
	defer db.Close()

	// Verify key1 is still not found after reopen
	_, ok, err = db.Get(key1)
	if err != nil {
		t.Fatalf("Get failed after reopen and delete: %v", err)
	}
	if ok {
		t.Fatalf("Key %s found after reopen and delete, but shouldn't be", key1)
	}

	// Verify key2 still exists after reopen
	_, ok, err = db.Get(key2)
	if err != nil {
		t.Fatalf("Get failed after reopen: %v", err)
	}
	if !ok {
		t.Fatalf("Key %s not found after reopen", key2)
	}

	// Test iterator after deletion in SSTable
	t.Run("IteratorAfterSSTableDelete", func(t *testing.T) {
		iter := db.Iterator()
		defer iter.Close()

		foundKey1 := false
		foundKey2 := false
		for iter.First(); iter.Valid(); iter.Next() {
			if bytes.Equal(iter.Key(), key1) {
				foundKey1 = true
			}
			if bytes.Equal(iter.Key(), key2) {
				foundKey2 = true
			}
		}
		if foundKey1 {
			t.Fatalf("Iterator found deleted key %s in SSTable", key1)
		}
		if !foundKey2 {
			t.Fatalf("Iterator did not find existing key %s in SSTable", key2)
		}
	})
}

func BenchmarkDBPut(b *testing.B) {
	memVFS := vfs.NewMemVFS()
	opts := Options{
		VFS:       memVFS,
		ArenaSize: 64 * 1024 * 1024, // 64MB arena
		Compare:   bytes.Compare,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := NewDB(opts)
		if err != nil {
			b.Fatalf("NewDB failed: %v", err)
		}

		// Insert a large number of unique keys to trigger flushes
		numKeys := 10000 // Adjust based on desired benchmark duration and memtable size
		for j := 0; j < numKeys; j++ {
			key := []byte(fmt.Sprintf("key-%08d-%d", j, i)) // Unique key for each benchmark iteration
			value := []byte(fmt.Sprintf("value-%08d", j))
			if err := db.Put(key, value); err != nil {
				b.Fatalf("Put failed: %v", err)
			}
		}
		if err := db.Close(); err != nil {
			b.Fatalf("DB Close failed: %v", err)
		}
	}
}

func BenchmarkDBGet(b *testing.B) {
	memVFS := vfs.NewMemVFS()
	opts := Options{
		VFS:       memVFS,
		ArenaSize: 64 * 1024 * 1024, // 64MB arena
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		b.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	// Pre-populate DB with a large number of keys
	numKeys := 100000
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		value := []byte(fmt.Sprintf("value-%08d", i))
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put failed during pre-population: %v", err)
		}
		keys[i] = key
	}
	if err := db.Close(); err != nil { // Force flush all to SSTables
		b.Fatalf("DB Close failed during pre-population: %v", err)
	}
	db, err = NewDB(opts) // Reopen to ensure reads are from SSTables
	if err != nil {
		b.Fatalf("NewDB failed on reopen for benchmark: %v", err)
	}

	b.ResetTimer()
	b.Run("ExistingKeys", func(b *testing.B) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < b.N; i++ {
			idx := r.Intn(numKeys)
			key := keys[idx]
			_, ok, err := db.Get(key)
			if err != nil {
				b.Fatalf("Get failed for existing key %s: %v", key, err)
			}
			if !ok {
				b.Fatalf("Existing key %s not found", key)
			}
		}
	})

	b.Run("NonExistingKeys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("nonexistent-key-%08d", i))
			_, ok, err := db.Get(key)
			if err != nil {
				b.Fatalf("Get failed for non-existing key %s: %v", key, err)
			}
			if ok {
				b.Fatalf("Non-existing key %s found", key)
			}
		}
	})
}

func TestDBConcurrency(t *testing.T) {
	vfs := vfs.NewMemVFS()
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024 * 1024, // 1MB arena
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	numWorkers := 10
	numOps := 100
	done := make(chan bool)
	keyPrefix := []byte("key-")

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for j := 0; j < numOps; j++ {
				key := []byte(fmt.Sprintf("%s%d-%d", keyPrefix, workerID, j))
				value := []byte(fmt.Sprintf("value-%d-%d", workerID, j))

				// Randomly choose between Put and Get
				if rand.Intn(2) == 0 {
					if err := db.Put(key, value); err != nil {
						t.Errorf("Put failed: %v", err)
					}
				} else {
					if _, _, err := db.Get(key); err != nil {
						t.Errorf("Get failed: %v", err)
					}
				}
			}
			done <- true
		}(i)
	}

	// Wait for all workers to finish
	for i := 0; i < numWorkers; i++ {
		<-done
	}
}

func TestDBIteratorEdgeCases(t *testing.T) {
	vfs := vfs.NewMemVFS()
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024,
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	// Test empty iterator
	t.Run("Empty", func(t *testing.T) {
		iter := db.Iterator()
		defer iter.Close()
		iter.First()
		if iter.Valid() {
			t.Fatal("Valid() should return false for empty iterator")
		}
	})
}

func TestDBValueOverwrite(t *testing.T) {
	vfs := vfs.NewMemVFS()
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024,
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	key := []byte("same-key")
	values := [][]byte{
		[]byte("short-value"),
		[]byte("medium-length-value"),
		[]byte("very-long-value-that-exceeds-initial-allocation-size"),
	}

	for i, value := range values {
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed for value %d: %v", i, err)
		}

		retrieved, ok, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get failed for value %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("Key not found after Put %d", i)
		}
		if !bytes.Equal(retrieved, value) {
			t.Fatalf("Value mismatch after Put %d: got %s, want %s", i, retrieved, value)
		}
	}
}

func TestDBPrefixScan(t *testing.T) {
	vfs := vfs.NewMemVFS()
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024,
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	prefix := []byte("user-")
	keys := [][]byte{
		[]byte("user-1001"),
		[]byte("user-1002"),
		[]byte("user-1003"),
		[]byte("account-1001"),
		[]byte("data-user-1001"),
	}

	for i, key := range keys {
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed for key %s: %v", key, err)
		}
	}

	iter := db.Iterator()
	defer iter.Close()

	// Seek to prefix
	iter.Seek(prefix)
	count := 0
	for ; iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		count++
	}

	if count != 3 {
		t.Fatalf("Expected 3 keys with prefix %s, found %d", prefix, count)
	}
}

func TestDBCustomCompare(t *testing.T) {
	vfs := vfs.NewMemVFS()
	// Custom compare function for reverse order
	reverseCompare := func(key1, key2 []byte) int {
		return bytes.Compare(key2, key1) // Reverse order
	}
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024,
		Compare:   reverseCompare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}
	values := [][]byte{
		[]byte("red"),
		[]byte("yellow"),
		[]byte("pink"),
	}

	for i := 0; i < len(keys); i++ {
		if err := db.Put(keys[i], values[i]); err != nil {
			t.Fatalf("Put failed for key %s: %v", keys[i], err)
		}
	}

	// Verify keys are retrieved in reverse order
	expectedOrder := [][]byte{
		[]byte("cherry"),
		[]byte("banana"),
		[]byte("apple"),
	}

	iter := db.Iterator()
	defer iter.Close()

	var retrievedKeys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		retrievedKeys = append(retrievedKeys, iter.Key())
	}

	if len(retrievedKeys) != len(expectedOrder) {
		t.Fatalf("Retrieved key count mismatch. Expected %d, got %d", len(expectedOrder), len(retrievedKeys))
	}

	for i := 0; i < len(expectedOrder); i++ {
		if !bytes.Equal(retrievedKeys[i], expectedOrder[i]) {
			t.Fatalf("Key order mismatch at index %d. Expected %s, got %s", i, expectedOrder[i], retrievedKeys[i])
		}
	}

	// Verify Get works with custom compare
	retrievedValue, ok, err := db.Get([]byte("banana"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok {
		t.Fatal("Get failed: key not found")
	}
	if !bytes.Equal(retrievedValue, []byte("yellow")) {
		t.Fatalf("Get returned wrong value: got %s, want %s", retrievedValue, []byte("yellow"))
	}
}

func TestDBCustomCompareWithFlush(t *testing.T) {
	vfs := vfs.NewMemVFS()
	// Custom compare function for reverse order
	reverseCompare := func(key1, key2 []byte) int {
		return bytes.Compare(key2, key1) // Reverse order
	}
	opts := Options{
		VFS:       vfs,
		ArenaSize: 1024, // Small arena to trigger flushing
		DataDir:   "database_custom_flush",
		Compare:   reverseCompare,
	}
	db, err := NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	numInsertions := 500 // Enough to trigger flushes with 1KB arena
	insertedKeys := make([][]byte, numInsertions)
	for i := 0; i < numInsertions; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed for key %s: %v", key, err)
		}
		insertedKeys[i] = key
	}

	// Explicitly close the DB to force a flush of the memtable to disk.
	if err := db.Close(); err != nil {
		t.Fatalf("DB Close failed: %v", err)
	}

	// Reopen the DB to read from the flushed SSTables
	db, err = NewDB(opts)
	if err != nil {
		t.Fatalf("NewDB failed on reopen: %v", err)
	}
	defer db.Close()

	// Verify keys are retrieved in reverse order
	iter := db.Iterator()
	defer iter.Close()

	var retrievedKeys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		retrievedKeys = append(retrievedKeys, iter.Key())
	}

	// Sort the original keys in reverse order to compare
	expectedOrder := make([][]byte, numInsertions)
	copy(expectedOrder, insertedKeys)
	sort.Slice(expectedOrder, func(i, j int) bool {
		return reverseCompare(expectedOrder[i], expectedOrder[j]) < 0
	})

	if len(retrievedKeys) != len(expectedOrder) {
		t.Fatalf("Retrieved key count mismatch. Expected %d, got %d", len(expectedOrder), len(retrievedKeys))
	}

	for i := 0; i < len(expectedOrder); i++ {
		if !bytes.Equal(retrievedKeys[i], expectedOrder[i]) {
			t.Fatalf("Key order mismatch at index %d. Expected %s, got %s", i, expectedOrder[i], retrievedKeys[i])
		}
	}

	// Verify Get works with custom compare after flush
	testKey := []byte("key-000")
	testValue := []byte("value-0")
	retrievedValue, ok, err := db.Get(testKey)
	if err != nil {
		t.Fatalf("Get failed for %s: %v", testKey, err)
	}
	if !ok {
		t.Fatalf("Get failed: key %s not found", testKey)
	}
	if !bytes.Equal(retrievedValue, testValue) {
		t.Fatalf("Get returned wrong value for %s: got %s, want %s", testKey, retrievedValue, testValue)
	}
}

func BenchmarkDBIterator(b *testing.B) {
	memVFS := vfs.NewMemVFS()
	opts := Options{
		VFS:       memVFS,
		ArenaSize: 64 * 1024 * 1024, // 64MB arena
		Compare:   bytes.Compare,
	}
	db, err := NewDB(opts)
	if err != nil {
		b.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	// Pre-populate DB with a large number of keys
	numKeys := 100000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		value := []byte(fmt.Sprintf("value-%08d", i))
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put failed during pre-population: %v", err)
		}
	}
	if err := db.Close(); err != nil { // Force flush all to SSTables
		b.Fatalf("DB Close failed during pre-population: %v", err)
	}
	db, err = NewDB(opts) // Reopen to ensure reads are from SSTables
	if err != nil {
		b.Fatalf("NewDB failed on reopen for benchmark: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := db.Iterator()
		count := 0
		for iter.First(); iter.Valid(); iter.Next() {
			count++
			_ = iter.Key()
			_ = iter.Value()
		}
		iter.Close()
		if count != numKeys {
			b.Fatalf("Iterator count mismatch. Expected %d, got %d", numKeys, count)
		}
	}
}
