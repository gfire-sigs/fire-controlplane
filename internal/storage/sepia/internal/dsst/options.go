package dsst

import (
	"crypto/rand"
	"errors"
)

// CompressionType represents the compression algorithm used.
type CompressionType uint8

const (
	// NoCompression indicates no compression.
	NoCompression CompressionType = 0
	// SnappyCompression indicates Snappy compression (not yet implemented).
	SnappyCompression CompressionType = 1
)

// Options holds the configuration for SST writing and reading.
type Options struct {
	// Compression specifies the compression type.
	Compression CompressionType

	// EncryptionKey is the master key (KEK) used to encrypt the Data Encryption Key (DEK).
	// If nil, encryption is disabled, but a default key is used for format compatibility.
	EncryptionKey []byte

	// BlockSize is the target size of data blocks.
	BlockSize int

	// MaxFileSize is the maximum size of the SST file.
	MaxFileSize int64

	// BloomBitsPerKey is the number of bits per key for the bloom filter.
	// If 0, bloom filter is disabled.
	BloomBitsPerKey int

	// BlockCipherSize is the size of the encryption block (e.g. 16KB for CTR).
	// Default is 16KB.
	BlockCipherSize int

	// KeyTSExtractor extracts the timestamp from a key.
	// If nil, timestamp range is not tracked.
	KeyTSExtractor func([]byte) uint64
}

// DefaultOptions returns the default options.
func DefaultOptions() *Options {
	return &Options{
		EncryptionKey:   nil, // Use default key
		BlockSize:       16 * 1024,
		MaxFileSize:     1 << 30, // 1GB
		BloomBitsPerKey: 10,
		Compression:     NoCompression,
		BlockCipherSize: 16 * 1024,
	}
}

// GenerateKey generates a random 32-byte key.
func GenerateKey() ([]byte, error) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// Validate checks if the options are valid.
func (o *Options) Validate() error {
	if o.BlockSize <= 0 {
		return errors.New("BlockSize must be positive")
	}
	if o.BlockCipherSize <= 0 {
		return errors.New("BlockCipherSize must be positive")
	}
	if len(o.EncryptionKey) > 0 && len(o.EncryptionKey) != 32 {
		return errors.New("EncryptionKey must be 32 bytes (AES-256)")
	}
	return nil
}
