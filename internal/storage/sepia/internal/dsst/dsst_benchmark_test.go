package dsst

import (
	"bytes"
	"fmt"
	"testing"
)

// generateKVs generates a slice of KVEntry for benchmarking.
func generateKVs(count int) []KVEntry {
	kvs := make([]KVEntry, count)
	for i := 0; i < count; i++ {
		kvs[i] = KVEntry{
			Key:   []byte(fmt.Sprintf("key%07d", i)),
			Value: []byte(fmt.Sprintf("value%07d", i*10)),
		}
	}
	return kvs
}

func BenchmarkWriterAdd(b *testing.B) {
	kvs := generateKVs(10000) // Use a larger set for writer benchmark

	configs := SSTableConfigs{
		CompressionType: CompressionTypeNone,
		BlockSize:       64 << 10,
		RestartInterval: 16,
		WyhashSeed:      0,
	}
	encryptionKey := []byte(DefaultEncryptionKeyStr)

	// Prepare a buffer for writing outside the benchmark loop
	var finalBuf *bytes.Buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		writer := NewWriter(buf, configs, encryptionKey, bytes.Compare)
		for _, kv := range kvs {
			if err := writer.Add(kv); err != nil {
				b.Fatalf("Writer.Add() error = %v", err)
			}
		}
		if err := writer.Finish(); err != nil {
			b.Fatalf("Writer.Finish() error = %v", err)
		}
		finalBuf = buf // Keep the last buffer for correctness check
	}

	// Correctness check after all benchmark iterations
	b.StopTimer() // Stop timer before correctness check
	reader, _, err := NewReader(bytes.NewReader(finalBuf.Bytes()), int64(finalBuf.Len()), encryptionKey, bytes.Compare)
	if err != nil {
		b.Fatalf("Failed to create reader for correctness check: %v", err)
	}
	for _, kv := range kvs {
		value, found, err := reader.Get(kv.Key)
		if err != nil {
			b.Fatalf("Correctness check: Reader.Get(%q) error = %v", kv.Key, err)
		}
		if !found {
			b.Fatalf("Correctness check: Reader.Get(%q) found = false, want true", kv.Key)
		}
		if !bytes.Equal(value, kv.Value) {
			b.Fatalf("Correctness check: Reader.Get(%q) value = %q, want %q", kv.Key, value, kv.Value)
		}
	}
	b.StartTimer() // Restart timer if there are more operations after correctness check (though not in this case)
}

func BenchmarkReaderGet(b *testing.B) {
	kvs := generateKVs(10000)

	// Prepare the SSTable once outside the benchmark loop
	buf := new(bytes.Buffer)
	configs := SSTableConfigs{
		CompressionType: CompressionTypeNone,
		BlockSize:       64 << 10,
		RestartInterval: 16,
		WyhashSeed:      0,
	}
	encryptionKey := []byte(DefaultEncryptionKeyStr)

	writer := NewWriter(buf, configs, encryptionKey, bytes.Compare)
	for _, kv := range kvs {
		if err := writer.Add(kv); err != nil {
			b.Fatalf("Writer.Add() error = %v", err)
		}
	}
	if err := writer.Finish(); err != nil {
		b.Fatalf("Writer.Finish() error = %v", err)
	}

	reader, _, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), encryptionKey, bytes.Compare)
	if err != nil {
		b.Fatalf("Failed to create reader: %v", err)
	}

	b.ResetTimer()
	b.Run("ExistingKeys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := kvs[i%len(kvs)].Key

			value, found, err := reader.Get(key)

			// Correctness check
			if err != nil {
				b.Fatalf("Correctness check: Reader.Get(%q) error = %v", key, err)
			}
			if !found {
				b.Fatalf("Correctness check: Reader.Get(%q) found = false, want true", key)
			}
			if !bytes.Equal(value, kvs[i%len(kvs)].Value) {
				b.Fatalf("Correctness check: Reader.Get(%q) value = %q, want %q", key, value, kvs[i%len(kvs)].Value)
			}
		}
	})

	b.Run("NonExistingKeys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("nonexistent%07d", i))

			_, found, err := reader.Get(key)

			// Correctness check
			if err != nil {
				b.Fatalf("Correctness check: Reader.Get(%q) error = %v", key, err)
			}
			if found {
				b.Fatalf("Correctness check: Reader.Get(%q) found = true, want false", key)
			}
		}
	})
}
