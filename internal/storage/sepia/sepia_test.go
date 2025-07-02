package sepia

import (
	"crypto/rand"
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
