package dsst

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dbloom"
)

//go:generate stringer -type=CompressionType
type CompressionType byte

const (
	CompressionTypeNone CompressionType = iota
	CompressionTypeSnappy
	CompressionTypeZstd
)

//go:generate stringer -type=EntryType
type EntryType byte

const (
	EntryTypeKeyValue EntryType = iota
	EntryTypeTombstone
)

// Constants defining the SST file format structure and flags based on the RFC.
const (
	// SST_V1_MAGIC is used in the file footer to identify the file format and version.
	SST_V1_MAGIC = "SEPIASSTMAGICV01"

	// SST_FOOTER_SIZE is the fixed size of the file footer.
	// 2*blockHandle(16) + 8-byte wyhash_seed + 16-byte magic + 8-byte version + 1*blockHandle(16) for bloom filter
	SST_FOOTER_SIZE = 80
)

// SSTableConfigs holds the configuration parameters for an SSTable.
type SSTableConfigs struct {
	CompressionType         CompressionType
	BlockSize               uint32 // Target size for data blocks, e.g., 64KB
	RestartInterval         uint32 // Number of keys between restart points within a data block
	WyhashSeed              uint64 // Seed for wyhash checksums
	BloomFilterBitsPerKey   int    // Bits per key for the bloom filter
	BloomFilterNumHashFuncs int    // Number of hash functions for the bloom filter
}

// BlockHeader contains metadata for each data block.
type BlockHeader struct {
	CompressionType      CompressionType
	InitializationVector [12]byte // 12-byte unique IV for AES-GCM
	AuthenticationTag    [16]byte // 16-byte GCM tag
}

// blockHandle stores the location (offset) and size of a block within the SST file.
type blockHandle struct {
	offset uint64
	size   uint64
}

// SSTFooter is the footer written at the end of the SST file.
type SSTFooter struct {
	MetaindexHandle   blockHandle
	IndexHandle       blockHandle
	BloomFilterHandle blockHandle // Handle for the bloom filter block
	WyhashSeed        uint64
	Magic             [16]byte
	Version           uint64
}

// indexEntry represents an entry in the index block.
type indexEntry struct {
	firstKey    []byte
	blockHandle blockHandle
}

// blockBloomFilterEntry represents an entry in the bloom filter block.
type blockBloomFilterEntry struct {
	firstKey    []byte
	bloomFilter *dbloom.Bloom
	blockHandle blockHandle
}

// encodeEntry encodes a single KV entry according to the RFC 5.1 specification.
func encodeEntry(buf *bytes.Buffer, prevKey []byte, entry KVEntry) {
	buf.WriteByte(byte(entry.EntryType))

	sharedPrefixLen := dsstCommonPrefix(prevKey, entry.Key)
	unsharedKey := entry.Key[sharedPrefixLen:]

	var uint64Buf [8]byte
	binary.LittleEndian.PutUint64(uint64Buf[:], uint64(sharedPrefixLen))
	buf.Write(uint64Buf[:])

	binary.LittleEndian.PutUint64(uint64Buf[:], uint64(len(unsharedKey)))
	buf.Write(uint64Buf[:])

	if entry.EntryType == EntryTypeKeyValue {
		binary.LittleEndian.PutUint64(uint64Buf[:], uint64(len(entry.Value)))
		buf.Write(uint64Buf[:])
	}

	buf.Write(unsharedKey)
	if entry.EntryType == EntryTypeKeyValue {
		buf.Write(entry.Value)
	}
}

// dsstCommonPrefix finds the length of the common prefix between two byte slices.
func dsstCommonPrefix(a, b []byte) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

// KVEntry represents a key-value pair for internal use.
type KVEntry struct {
	EntryType EntryType
	Key       []byte
	Value     []byte
}

// kvEntryPool is a sync.Pool for reusing KVEntry instances to reduce memory allocations.
var kvEntryPool = sync.Pool{
	New: func() interface{} {
		return &KVEntry{}
	},
}

// AcquireKVEntry retrieves a KVEntry from the pool.
func AcquireKVEntry() *KVEntry {
	return kvEntryPool.Get().(*KVEntry)
}

// ReleaseKVEntry returns a KVEntry to the pool after resetting it to prevent data leakage.
func ReleaseKVEntry(entry *KVEntry) {
	entry.EntryType = 0
	entry.Key = nil
	entry.Value = nil
	kvEntryPool.Put(entry)
}

// dsstDecodeEntry decodes a key-value entry from the given reader.
func dsstDecodeEntry(r *bytes.Reader, prevKey []byte) (KVEntry, error) {
	entryTypeByte, err := r.ReadByte()
	if err != nil {
		return KVEntry{}, err
	}
	entryType := EntryType(entryTypeByte)

	var uint64Buf [8]byte
	_, err = io.ReadFull(r, uint64Buf[:])
	if err != nil {
		return KVEntry{}, err
	}
	sharedPrefixLen := binary.LittleEndian.Uint64(uint64Buf[:])

	_, err = io.ReadFull(r, uint64Buf[:])
	if err != nil {
		return KVEntry{}, err
	}
	unsharedKeyLen := binary.LittleEndian.Uint64(uint64Buf[:])

	var valueLen uint64
	if entryType == EntryTypeKeyValue {
		_, err = io.ReadFull(r, uint64Buf[:])
		if err != nil {
			return KVEntry{}, err
		}
		valueLen = binary.LittleEndian.Uint64(uint64Buf[:])
	}

	// Allocate a byte slice for the key.
	requiredLen := sharedPrefixLen + unsharedKeyLen
	key := make([]byte, requiredLen)
	copy(key, prevKey[:sharedPrefixLen])

	// Allocate a byte slice for the unshared key part.
	unsharedKey := make([]byte, unsharedKeyLen)
	if _, err := io.ReadFull(r, unsharedKey); err != nil {
		return KVEntry{}, err
	}
	copy(key[sharedPrefixLen:], unsharedKey)

	var value []byte
	if entryType == EntryTypeKeyValue {
		// Allocate a byte slice for the value.
		value = make([]byte, valueLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return KVEntry{}, err
		}
	}

	// Get a pooled entry.
	entry := AcquireKVEntry()
	entry.EntryType = entryType
	entry.Key = key
	entry.Value = value
	// Note: The caller is responsible for returning the entry to the pool using ReleaseKVEntry if needed.
	return *entry, nil
}
