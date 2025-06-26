package dsst

import (
	"bytes"
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

			configs := SSTableConfigs{
				CompressionType: tt.compression,
				BlockSize:       64 << 10, // 64KB
				RestartInterval: 16,
				WyhashSeed:      0, // Fixed seed for deterministic tests
				EncryptionKey:   make([]byte, 32), // Dummy 32-byte key for testing
			}

			// Test Writer
			writer := NewWriter(buf, configs)
			kvs := []KVEntry{
				{Key: []byte("apple"), Value: []byte("red")},
				{Key: []byte("banana"), Value: []byte("yellow")},
				{Key: []byte("cherry"), Value: []byte("red")},
				{Key: []byte("date"), Value: []byte("brown")},
				{Key: []byte("elderberry"), Value: []byte("dark purple")},
				{Key: []byte("fig"), Value: []byte("purple")},
				{Key: []byte("grape"), Value: []byte("purple")},
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
			reader, _, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
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

			// Check for non-existent key
			_, found, err := reader.Get([]byte("guava"))
			if err != nil {
				t.Fatalf("Reader.Get(\"guava\") error = %v", err)
			}
			if found {
				t.Errorf("Reader.Get(\"guava\") found = true, want false")
			}
		})
	}
}