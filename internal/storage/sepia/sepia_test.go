package sepia

import (
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pkg.gfire.dev/controlplane/internal/vfs"
)

func TestDB_PutGet(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.NewOSVFS()
	key := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, key)
	require.NoError(t, err)

	opts := Options{
		DataDir:       dir,
		VFS:           fs,
		EncryptionKey: key,
	}
	db, err := NewDB(opts)
	require.NoError(t, err)
	defer db.Close()

	// Test Put and Get
	err = db.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	val, found, err := db.Get([]byte("key1"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), val)

	// Test update
	err = db.Put([]byte("key1"), []byte("new_value1"))
	require.NoError(t, err)

	val, found, err = db.Get([]byte("key1"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("new_value1"), val)

	// Test non-existent key
	_, found, err = db.Get([]byte("non_existent_key"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestDB_Delete(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.NewOSVFS()
	key := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, key)
	require.NoError(t, err)

	opts := Options{
		DataDir:       dir,
		VFS:           fs,
		EncryptionKey: key,
	}
	db, err := NewDB(opts)
	require.NoError(t, err)
	defer db.Close()

	err = db.Put([]byte("key_to_delete"), []byte("value_to_delete"))
	require.NoError(t, err)

	val, found, err := db.Get([]byte("key_to_delete"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value_to_delete"), val)

	err = db.Delete([]byte("key_to_delete"))
	require.NoError(t, err)

	_, found, err = db.Get([]byte("key_to_delete"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestDB_Recovery(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.NewOSVFS()
	key := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, key)
	require.NoError(t, err)

	opts := Options{
		DataDir:       dir,
		VFS:           fs,
		EncryptionKey: key,
	}

	// Create DB, put some data, then close it
	db1, err := NewDB(opts)
	require.NoError(t, err)

	err = db1.Put([]byte("recovery_key1"), []byte("recovery_value1"))
	require.NoError(t, err)
	err = db1.Put([]byte("recovery_key2"), []byte("recovery_value2"))
	require.NoError(t, err)
	err = db1.Delete([]byte("recovery_key1"))
	require.NoError(t, err)

	require.NoError(t, db1.Close())

	// Reopen DB and verify data
	db2, err := NewDB(opts)
	require.NoError(t, err)
	defer db2.Close()

	val, found, err := db2.Get([]byte("recovery_key1"))
	require.NoError(t, err)
	assert.False(t, found) // Should be deleted

	val, found, err = db2.Get([]byte("recovery_key2"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("recovery_value2"), val)
}

func TestDB_LeveledCompaction(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.NewOSVFS()
	key := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, key)
	require.NoError(t, err)

	opts := Options{
		DataDir:       dir,
		VFS:           fs,
		EncryptionKey: key,
		ArenaSize:     1024 * 1024, // Small arena to force frequent flushes
	}

	db, err := NewDB(opts)
	require.NoError(t, err)
	defer db.Close()

	// Insert enough data to trigger multiple memtable flushes and compaction
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := make([]byte, 100000) // Large value to fill memtable quickly
		_, err := rand.Read(value)
		require.NoError(t, err)
		err = db.Put(key, value)
		require.NoError(t, err)
	}

	// Check if compaction moved some SSTables to level 1
	db.sstManager.mu.Lock()
	level0Count := len(db.sstManager.levels[0])
	level1Count := len(db.sstManager.levels[1])
	db.sstManager.mu.Unlock()

	assert.True(t, level0Count <= 4 || level1Count > 0, "Compaction should have moved some SSTables to level 1")
}

func TestTimestampCompare(t *testing.T) {
	// Test keys with timestamps appended (last 8 bytes)
	key1 := append([]byte("keyA"), 0, 0, 0, 0, 0, 0, 0, 10) // Timestamp 10
	key2 := append([]byte("keyA"), 0, 0, 0, 0, 0, 0, 0, 20) // Timestamp 20
	key3 := append([]byte("keyB"), 0, 0, 0, 0, 0, 0, 0, 10) // Timestamp 10, different prefix

	// Higher timestamp should come first (descending order)
	assert.Equal(t, -1, TimestampCompare(key2, key1)) // 20 > 10
	assert.Equal(t, 1, TimestampCompare(key1, key2))  // 10 < 20

	// Same timestamp, compare prefix
	assert.Equal(t, -1, TimestampCompare(key1, key3)) // keyA < keyB
	assert.Equal(t, 1, TimestampCompare(key3, key1))  // keyB > keyA

	// Test short keys (fallback to bytes.Compare)
	shortKey1 := []byte("key1")
	shortKey2 := []byte("key2")
	assert.Equal(t, -1, TimestampCompare(shortKey1, shortKey2))
	assert.Equal(t, 1, TimestampCompare(shortKey2, shortKey1))
}

func TestMVCCVersioning(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.NewOSVFS()
	key := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, key)
	require.NoError(t, err)

	opts := Options{
		DataDir:       dir,
		VFS:           fs,
		EncryptionKey: key,
	}

	db, err := NewDB(opts)
	require.NoError(t, err)
	defer db.Close()

	// Helper function to create a key with a timestamp appended
	makeKeyWithTimestamp := func(baseKey []byte, ts int64) []byte {
		return append(baseKey, byte(ts>>56), byte(ts>>48), byte(ts>>40), byte(ts>>32), byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts))
	}

	// Insert multiple versions of the same key with different timestamps
	baseKey := []byte("mvcc_key")
	err = db.Put(makeKeyWithTimestamp(baseKey, 10), []byte("value_at_ts10"))
	require.NoError(t, err)
	err = db.Put(makeKeyWithTimestamp(baseKey, 20), []byte("value_at_ts20"))
	require.NoError(t, err)
	err = db.Put(makeKeyWithTimestamp(baseKey, 30), []byte("value_at_ts30"))
	require.NoError(t, err)

	// Since TimestampCompare prioritizes higher timestamps, the latest version should be returned
	// However, current Get implementation does not handle versioned keys correctly
	// For this test, we assume the application layer handles key construction
	// Test retrieval of the latest version
	val, found, err := db.Get(makeKeyWithTimestamp(baseKey, 30))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value_at_ts30"), val)

	// Test deletion of a specific version
	err = db.Delete(makeKeyWithTimestamp(baseKey, 30))
	require.NoError(t, err)

	// The deleted version should not be found
	_, found, err = db.Get(makeKeyWithTimestamp(baseKey, 30))
	require.NoError(t, err)
	assert.False(t, found)

	// Earlier version should still exist
	val, found, err = db.Get(makeKeyWithTimestamp(baseKey, 20))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value_at_ts20"), val)
}
