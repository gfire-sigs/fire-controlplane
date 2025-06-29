package dsst

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkWriterAdd(b *testing.B) {
	configs := SSTableConfigs{
		CompressionType:         CompressionTypeNone,
		BlockSize:               4096,
		RestartInterval:         16,
		WyhashSeed:              123456789,
		BloomFilterBitsPerKey:   10,
		BloomFilterNumHashFuncs: 5,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := NewWriter(&buf, configs, []byte(DefaultEncryptionKeyStr), bytes.Compare)
		entry := KVEntry{
			EntryType: EntryTypeKeyValue,
			Key:       []byte(fmt.Sprintf("key%d", i)),
			Value:     []byte(fmt.Sprintf("value%d", i)),
		}
		err := writer.Add(entry)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkWriterFinish(b *testing.B) {
	configs := SSTableConfigs{
		CompressionType:         CompressionTypeNone,
		BlockSize:               4096,
		RestartInterval:         16,
		WyhashSeed:              123456789,
		BloomFilterBitsPerKey:   10,
		BloomFilterNumHashFuncs: 5,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := NewWriter(&buf, configs, []byte(DefaultEncryptionKeyStr), bytes.Compare)
		// Generate test data with ascending keys
		for j := 0; j < 100; j++ { // Reduced number of entries for faster benchmark
			entry := KVEntry{
				EntryType: EntryTypeKeyValue,
				Key:       []byte(fmt.Sprintf("key%09d", j)), // Use padded number to ensure ascending order
				Value:     []byte(fmt.Sprintf("value%d", j)),
			}
			err := writer.Add(entry)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
		err := writer.Finish()
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkReaderGet(b *testing.B) {
	configs := SSTableConfigs{
		CompressionType:         CompressionTypeNone,
		BlockSize:               4096,
		RestartInterval:         16,
		WyhashSeed:              123456789,
		BloomFilterBitsPerKey:   10,
		BloomFilterNumHashFuncs: 5,
	}

	// Generate test data and write to buffer once before the benchmark
	var buf bytes.Buffer
	writer := NewWriter(&buf, configs, []byte(DefaultEncryptionKeyStr), bytes.Compare)
	for i := 0; i < 100; i++ { // Reduced number of entries for faster benchmark
		entry := KVEntry{
			EntryType: EntryTypeKeyValue,
			Key:       []byte(fmt.Sprintf("key%09d", i)), // Use padded number to ensure ascending order
			Value:     []byte(fmt.Sprintf("value%d", i)),
		}
		err := writer.Add(entry)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
	err := writer.Finish()
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	// Create reader once before the benchmark
	reader, _, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), []byte(DefaultEncryptionKeyStr), bytes.Compare)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%09d", i%100))
		_, _, err := reader.Get(key)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkBloomFilterGet(b *testing.B) {
	configs := SSTableConfigs{
		CompressionType:         CompressionTypeNone,
		BlockSize:               4096,
		RestartInterval:         16,
		WyhashSeed:              123456789,
		BloomFilterBitsPerKey:   10,
		BloomFilterNumHashFuncs: 5,
	}

	// Generate test data and write to buffer once before the benchmark
	var buf bytes.Buffer
	writer := NewWriter(&buf, configs, []byte(DefaultEncryptionKeyStr), bytes.Compare)
	for i := 0; i < 100; i++ { // Reduced number of entries for faster benchmark
		entry := KVEntry{
			EntryType: EntryTypeKeyValue,
			Key:       []byte(fmt.Sprintf("key%09d", i)), // Use padded number to ensure ascending order
			Value:     []byte(fmt.Sprintf("value%d", i)),
		}
		err := writer.Add(entry)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
	err := writer.Finish()
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	// Create reader once before the benchmark
	reader, _, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), []byte(DefaultEncryptionKeyStr), bytes.Compare)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("non_existing_key%09d", i))
		_, found, err := reader.Get(key)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		if found {
			b.Errorf("expected non-existing key to not be found")
		}
	}
}
