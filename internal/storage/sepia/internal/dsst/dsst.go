package dsst

import (
	"bytes"
	"encoding/binary"
	"io"
)

//go:generate stringer -type=CompressionType
type CompressionType byte

const (
	CompressionTypeNone CompressionType = iota
	CompressionTypeSnappy
	CompressionTypeZstd
)

// Constants defining the SST file format structure and flags based on the RFC.
const (
	// SST_V1_MAGIC is used in the file footer to identify the file format and version.
	SST_V1_MAGIC = "SEPIASSTMAGICV01"

	// SST_FOOTER_SIZE is the fixed size of the file footer.
	// 2*blockHandle(16) + 8-byte wyhash_seed + 16-byte magic + 8-byte version
	SST_FOOTER_SIZE = 64
)

// SSTableConfigs holds the configuration parameters for an SSTable.
type SSTableConfigs struct {
	CompressionType CompressionType
	BlockSize       uint32 // Target size for data blocks, e.g., 64KB
	RestartInterval uint32 // Number of keys between restart points within a data block
	WyhashSeed      uint64 // Seed for wyhash checksums
	EncryptionKey   []byte // 32-byte AES-256 key
}

// BlockHeader contains metadata for each data block.
type BlockHeader struct {
	CompressionType   CompressionType
	InitializationVector [12]byte // 12-byte unique IV for AES-GCM
	AuthenticationTag    [16]byte // 16-byte GCM tag
}

// KVEntry represents a single key-value pair.
type KVEntry struct {
	Key   []byte
	Value []byte
}

// blockHandle stores the location (offset) and size of a block within the SST file.
type blockHandle struct {
	offset uint64
	size   uint64
}

// SSTFooter is the footer written at the end of the SST file.
type SSTFooter struct {
	MetaindexHandle blockHandle
	IndexHandle     blockHandle
	WyhashSeed      uint64
	Magic           [16]byte
	Version         uint64
}

// indexEntry represents an entry in the index block.
type indexEntry struct {
	firstKey    []byte
	blockHandle blockHandle
}

// encodeEntry encodes a single KV entry according to the RFC 5.1 specification.
func encodeEntry(buf *bytes.Buffer, prevKey []byte, entry KVEntry) {
	sharedPrefixLen := commonPrefix(prevKey, entry.Key)
	unsharedKey := entry.Key[sharedPrefixLen:]

	var uvarintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(uvarintBuf[:], uint64(sharedPrefixLen))
	buf.Write(uvarintBuf[:n])

	n = binary.PutUvarint(uvarintBuf[:], uint64(len(unsharedKey)))
	buf.Write(uvarintBuf[:n])

	n = binary.PutUvarint(uvarintBuf[:], uint64(len(entry.Value)))
	buf.Write(uvarintBuf[:n])

	buf.Write(unsharedKey)
	buf.Write(entry.Value)
}

// commonPrefix finds the length of the common prefix between two byte slices.
func commonPrefix(a, b []byte) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

func decodeEntry(r *bytes.Reader, prevKey []byte) (KVEntry, error) {
	sharedPrefixLen, err := binary.ReadUvarint(r)
	if err != nil {
		return KVEntry{}, err
	}

	unsharedKeyLen, err := binary.ReadUvarint(r)
	if err != nil {
		return KVEntry{}, err
	}

	valueLen, err := binary.ReadUvarint(r)
	if err != nil {
		return KVEntry{}, err
	}

	key := make([]byte, sharedPrefixLen+unsharedKeyLen)
	copy(key, prevKey[:sharedPrefixLen])

	unsharedKey := make([]byte, unsharedKeyLen)
	if _, err := io.ReadFull(r, unsharedKey); err != nil {
		return KVEntry{}, err
	}
	copy(key[sharedPrefixLen:], unsharedKey)

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return KVEntry{}, err
	}

	return KVEntry{Key: key, Value: value}, nil
}