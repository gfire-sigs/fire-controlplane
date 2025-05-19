package sepia

// MagicSepiaSSTable is the magic number for Sepia SSTable files.
// "SEPIASST" in ASCII, followed by a version number (e.g., 0001).
const MagicSepiaSSTable = uint64(0x53455049415353540001) // SEPIASST0001

// DefaultBlockSize is the default size for data blocks (e.g., 4KB).
const DefaultBlockSize = 4 * 1024

// DefaultBloomFilterFPRate is the default false positive rate for the Bloom filter.
const DefaultBloomFilterFPRate = 0.01

// CurrentSSTableFormatVersion indicates the version of the SSTable format being written.
// This allows for future format changes and backward/forward compatibility checks.
const CurrentSSTableFormatVersion = uint32(1)

// CompressionTypeNone indicates that a block is not compressed.
const CompressionTypeNone = byte(0x00)

// CompressionTypeSnappy indicates that a block is compressed using Snappy.
const CompressionTypeSnappy = byte(0x01)

// SSTableFooterSize is the fixed size of the SSTable footer in bytes.
// The footer contains metadata like block offsets, magic number, and checksum.
const SSTableFooterSize = 48
