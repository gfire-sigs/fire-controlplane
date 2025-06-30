package dsst

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncodeDecodeEntry(t *testing.T) {
	// Test case 1: Basic key-value pair
	entry1 := KVEntry{
		EntryType: EntryTypeKeyValue,
		Key:       []byte("key1"),
		Value:     []byte("value1"),
	}
	buf := new(bytes.Buffer)
	encodeEntry(buf, nil, entry1)

	reader := bytes.NewReader(buf.Bytes())
	decodedEntry, err := dsstDecodeEntry(reader, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry1.EntryType != decodedEntry.EntryType {
		t.Errorf("expected EntryType %v, got %v", entry1.EntryType, decodedEntry.EntryType)
	}
	if !bytes.Equal(entry1.Key, decodedEntry.Key) {
		t.Errorf("expected Key %v, got %v", entry1.Key, decodedEntry.Key)
	}
	if !bytes.Equal(entry1.Value, decodedEntry.Value) {
		t.Errorf("expected Value %v, got %v", entry1.Value, decodedEntry.Value)
	}

	// Test case 2: Key with shared prefix
	entry2 := KVEntry{
		EntryType: EntryTypeKeyValue,
		Key:       []byte("key2"),
		Value:     []byte("value2"),
	}
	buf.Reset()
	encodeEntry(buf, entry1.Key, entry2)

	reader = bytes.NewReader(buf.Bytes())
	decodedEntry, err = dsstDecodeEntry(reader, entry1.Key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry2.EntryType != decodedEntry.EntryType {
		t.Errorf("expected EntryType %v, got %v", entry2.EntryType, decodedEntry.EntryType)
	}
	if !bytes.Equal(entry2.Key, decodedEntry.Key) {
		t.Errorf("expected Key %v, got %v", entry2.Key, decodedEntry.Key)
	}
	if !bytes.Equal(entry2.Value, decodedEntry.Value) {
		t.Errorf("expected Value %v, got %v", entry2.Value, decodedEntry.Value)
	}

	// Test case 3: Tombstone entry
	entry3 := KVEntry{
		EntryType: EntryTypeTombstone,
		Key:       []byte("key3"),
		Value:     nil,
	}
	buf.Reset()
	encodeEntry(buf, nil, entry3)

	reader = bytes.NewReader(buf.Bytes())
	decodedEntry, err = dsstDecodeEntry(reader, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry3.EntryType != decodedEntry.EntryType {
		t.Errorf("expected EntryType %v, got %v", entry3.EntryType, decodedEntry.EntryType)
	}
	if !bytes.Equal(entry3.Key, decodedEntry.Key) {
		t.Errorf("expected Key %v, got %v", entry3.Key, decodedEntry.Key)
	}
	if decodedEntry.Value != nil {
		t.Errorf("expected Value to be nil, got %v", decodedEntry.Value)
	}
}

func TestCommonPrefix(t *testing.T) {
	// Test case 1: Identical keys
	key1 := []byte("prefix_key")
	key2 := []byte("prefix_key")
	length := dsstCommonPrefix(key1, key2)
	if length != len(key1) {
		t.Errorf("expected length %v, got %v", len(key1), length)
	}

	// Test case 2: Partial prefix
	key3 := []byte("prefix_1")
	key4 := []byte("prefix_2")
	length = dsstCommonPrefix(key3, key4)
	if length != 7 {
		t.Errorf("expected length 7, got %v", length)
	}

	// Test case 3: No common prefix
	key5 := []byte("a_key")
	key6 := []byte("b_key")
	length = dsstCommonPrefix(key5, key6)
	if length != 0 {
		t.Errorf("expected length 0, got %v", length)
	}

	// Test case 4: Empty keys
	key7 := []byte("")
	key8 := []byte("")
	length = dsstCommonPrefix(key7, key8)
	if length != 0 {
		t.Errorf("expected length 0, got %v", length)
	}

	// Test case 5: One empty key
	key9 := []byte("key")
	key10 := []byte("")
	length = dsstCommonPrefix(key9, key10)
	if length != 0 {
		t.Errorf("expected length 0, got %v", length)
	}
}

func TestFooterReadWrite(t *testing.T) {
	footer := SSTFooter{
		MetaindexHandle:   blockHandle{offset: 100, size: 50},
		IndexHandle:       blockHandle{offset: 150, size: 30},
		BloomFilterHandle: blockHandle{offset: 180, size: 20},
		WyhashSeed:        123456789,
		Version:           1,
	}
	copy(footer.Magic[:], SST_V1_MAGIC)

	buf := make([]byte, SST_FOOTER_SIZE)
	binary.LittleEndian.PutUint64(buf[0:8], footer.MetaindexHandle.offset)
	binary.LittleEndian.PutUint64(buf[8:16], footer.MetaindexHandle.size)
	binary.LittleEndian.PutUint64(buf[16:24], footer.IndexHandle.offset)
	binary.LittleEndian.PutUint64(buf[24:32], footer.IndexHandle.size)
	binary.LittleEndian.PutUint64(buf[32:40], footer.BloomFilterHandle.offset)
	binary.LittleEndian.PutUint64(buf[40:48], footer.BloomFilterHandle.size)
	binary.LittleEndian.PutUint64(buf[48:56], footer.WyhashSeed)
	copy(buf[56:72], footer.Magic[:])
	binary.LittleEndian.PutUint64(buf[72:80], footer.Version)

	reader := bytes.NewReader(buf)
	readFooter, err := readFooter(reader, int64(len(buf)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if footer.MetaindexHandle != readFooter.MetaindexHandle {
		t.Errorf("expected MetaindexHandle %v, got %v", footer.MetaindexHandle, readFooter.MetaindexHandle)
	}
	if footer.IndexHandle != readFooter.IndexHandle {
		t.Errorf("expected IndexHandle %v, got %v", footer.IndexHandle, readFooter.IndexHandle)
	}
	if footer.BloomFilterHandle != readFooter.BloomFilterHandle {
		t.Errorf("expected BloomFilterHandle %v, got %v", footer.BloomFilterHandle, readFooter.BloomFilterHandle)
	}
	if footer.WyhashSeed != readFooter.WyhashSeed {
		t.Errorf("expected WyhashSeed %v, got %v", footer.WyhashSeed, readFooter.WyhashSeed)
	}
	if !bytes.Equal(footer.Magic[:], readFooter.Magic[:]) {
		t.Errorf("expected Magic %v, got %v", footer.Magic, readFooter.Magic)
	}
	if footer.Version != readFooter.Version {
		t.Errorf("expected Version %v, got %v", footer.Version, readFooter.Version)
	}
}

func TestBloomFilterIntegration(t *testing.T) {
	configs := SSTableConfigs{
		CompressionType:         CompressionTypeNone,
		BlockSize:               4096,
		RestartInterval:         16,
		WyhashSeed:              123456789,
		BloomFilterBitsPerKey:   10,
		BloomFilterNumHashFuncs: 5,
	}

	// Create a writer
	var buf bytes.Buffer
	writer := NewWriter(&buf, configs, []byte(DefaultEncryptionKeyStr), bytes.Compare)

	// Add some entries
	entries := []KVEntry{
		{EntryType: EntryTypeKeyValue, Key: []byte("key1"), Value: []byte("value1")},
		{EntryType: EntryTypeKeyValue, Key: []byte("key2"), Value: []byte("value2")},
		{EntryType: EntryTypeKeyValue, Key: []byte("key3"), Value: []byte("value3")},
	}

	for _, entry := range entries {
		err := writer.Add(&entry)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Finish writing
	err := writer.Finish()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Create a reader
	reader, _, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), []byte(DefaultEncryptionKeyStr), bytes.Compare)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check if bloom filters are loaded
	if len(reader.blockBloomFilters) == 0 {
		t.Error("expected bloom filters to be loaded, but slice is empty")
	}

	// Test bloom filter for existing keys (should return true)
	for _, entry := range entries {
		value, found, err := reader.Get(entry.Key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Errorf("expected key %v to be found", entry.Key)
		}
		if !bytes.Equal(entry.Value, value) {
			t.Errorf("expected value %v for key %v, got %v", entry.Value, entry.Key, value)
		}
	}

	// Test bloom filter for non-existing key (should return false or true, but Get should return not found)
	nonExistingKey := []byte("non_existing_key")
	value, found, err := reader.Get(nonExistingKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Errorf("expected non-existing key %v to not be found", nonExistingKey)
	}
	if value != nil {
		t.Errorf("expected value to be nil for non-existing key, got %v", value)
	}
}

func TestCompressionIntegrationZstd(t *testing.T) {
	configs := SSTableConfigs{
		CompressionType:         CompressionTypeZstd,
		BlockSize:               4096,
		RestartInterval:         16,
		WyhashSeed:              123456789,
		BloomFilterBitsPerKey:   10,
		BloomFilterNumHashFuncs: 5,
	}

	// Create a writer
	var buf bytes.Buffer
	writer := NewWriter(&buf, configs, []byte(DefaultEncryptionKeyStr), bytes.Compare)

	// Add some entries
	entries := []KVEntry{
		{EntryType: EntryTypeKeyValue, Key: []byte("key1"), Value: []byte("value1")},
		{EntryType: EntryTypeKeyValue, Key: []byte("key2"), Value: []byte("value2")},
		{EntryType: EntryTypeKeyValue, Key: []byte("key3"), Value: []byte("value3")},
	}

	for _, entry := range entries {
		err := writer.Add(&entry)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Finish writing
	err := writer.Finish()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Create a reader
	reader, _, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), []byte(DefaultEncryptionKeyStr), bytes.Compare)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test reading compressed data for existing keys (should return true)
	for _, entry := range entries {
		value, found, err := reader.Get(entry.Key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Errorf("expected key %v to be found", entry.Key)
		}
		if !bytes.Equal(entry.Value, value) {
			t.Errorf("expected value %v for key %v, got %v", entry.Value, entry.Key, value)
		}
	}

	// Test reading for non-existing key (should return not found)
	nonExistingKey := []byte("non_existing_key")
	value, found, err := reader.Get(nonExistingKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Errorf("expected non-existing key %v to not be found", nonExistingKey)
	}
	if value != nil {
		t.Errorf("expected value to be nil for non-existing key, got %v", value)
	}
}

func TestCompressionIntegrationSnappy(t *testing.T) {
	configs := SSTableConfigs{
		CompressionType:         CompressionTypeSnappy,
		BlockSize:               4096,
		RestartInterval:         16,
		WyhashSeed:              123456789,
		BloomFilterBitsPerKey:   10,
		BloomFilterNumHashFuncs: 5,
	}

	// Create a writer
	var buf bytes.Buffer
	writer := NewWriter(&buf, configs, []byte(DefaultEncryptionKeyStr), bytes.Compare)

	// Add some entries
	entries := []KVEntry{
		{EntryType: EntryTypeKeyValue, Key: []byte("key1"), Value: []byte("value1")},
		{EntryType: EntryTypeKeyValue, Key: []byte("key2"), Value: []byte("value2")},
		{EntryType: EntryTypeKeyValue, Key: []byte("key3"), Value: []byte("value3")},
	}

	for _, entry := range entries {
		err := writer.Add(&entry)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Finish writing
	err := writer.Finish()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Create a reader
	reader, _, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), []byte(DefaultEncryptionKeyStr), bytes.Compare)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test reading compressed data for existing keys (should return true)
	for _, entry := range entries {
		value, found, err := reader.Get(entry.Key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !found {
			t.Errorf("expected key %v to be found", entry.Key)
		}
		if !bytes.Equal(entry.Value, value) {
			t.Errorf("expected value %v for key %v, got %v", entry.Value, entry.Key, value)
		}
	}

	// Test reading for non-existing key (should return not found)
	nonExistingKey := []byte("non_existing_key")
	value, found, err := reader.Get(nonExistingKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Errorf("expected non-existing key %v to not be found", nonExistingKey)
	}
	if value != nil {
		t.Errorf("expected value to be nil for non-existing key, got %v", value)
	}
}
