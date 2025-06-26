package dsst

import (
	"bytes"
	"fmt"
	"testing"
)

func TestWriterAndReader(t *testing.T) {
	tests := []struct {
		name        string
		compression CompressionType
	}{
		{name: "No Compression", compression: CompressionTypeNone},
		{name: "Snappy Compression", compression: CompressionTypeSnappy},
		{name: "Zstd Compression", compression: CompressionTypeZstd},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			encryptionKey := []byte(DefaultEncryptionKeyStr) // Use the default key for testing

			configs := SSTableConfigs{
				CompressionType: tt.compression,
				BlockSize:       64 << 10, // 64KB
				RestartInterval: 16,
				WyhashSeed:      0, // Fixed seed for deterministic tests
			}

			// Test Writer
			writer := NewWriter(buf, configs, encryptionKey)
			var kvs []KVEntry
			// Generate a larger set of keys to ensure multiple blocks and restart points
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key%03d", i))
				value := []byte(fmt.Sprintf("value%03d", i*10))
				kvs = append(kvs, KVEntry{Key: key, Value: value})
			}

			for _, kv := range kvs {
				if err := writer.Add(kv.Key, kv.Value); err != nil {
					t.Fatalf("Writer.Add() error = %v", err)
				}
			}

			if err := writer.Finish(); err != nil {
				t.Fatalf("Writer.Finish() error = %v", err)
			}

			// Test Reader
			reader, _, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), encryptionKey)
			if err != nil {
				t.Fatalf("NewReader() error = %v", err)
			}

			// Check for existing keys
			for _, kv := range kvs {
				value, found, err := reader.Get(kv.Key)
				if err != nil {
					t.Fatalf("Reader.Get(%q) error = %v", kv.Key, err)
				}
				if !found {
					t.Errorf("Reader.Get(%q) found = false, want true", kv.Key)
				}
				if !bytes.Equal(value, kv.Value) {
					t.Errorf("Reader.Get(%q) value = %q, want %q", kv.Key, value, kv.Value)
				}
			}

			// Check for non-existent keys
			nonExistentKeys := []string{
				"key000a", // Between existing keys
				"key050a",
				"key100",  // After all keys
				"akey",    // Before all keys
				"key0000", // Shorter than existing keys
				"key00000", // Longer than existing keys
			}
			for _, nk := range nonExistentKeys {
				_, found, err := reader.Get([]byte(nk))
				if err != nil {
					t.Fatalf("Reader.Get(%q) error = %v", nk, err)
				}
				if found {
					t.Errorf("Reader.Get(%q) found = true, want false", nk)
				}
			}
		})
	}
}
