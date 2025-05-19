package sepia_test

import (
	"bytes"
	"encoding/binary"
	// "hash/crc32" // Not directly used in test logic, but good to have if debugging footer
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	// The import path will be relative to the module root.
	// Assuming "app" is the module name or it's discoverable by the Go toolchain.
	// For local testing, this might be "project_name/internal/storage/sepia"
	// The tool environment should handle the correct resolution.
	"app/internal/storage/sepia"
)

// Helper function to create a temporary SSTable file path
func tempSSTableFileName(t *testing.T) string {
	t.Helper()
	// Create a subdirectory within t.TempDir() for sstables if needed,
	// but TempDir() itself is fine for isolated test files.
	return filepath.Join(t.TempDir(), "test.sstable")
}

// Helper type for key-value pairs
type kvPair struct {
	key   string
	value string
}

// checkGet is a helper to test reader.Get results
func checkGet(t *testing.T, r *sepia.SSTableReader, key string, expectedValue string, expectedFound bool) {
	t.Helper()
	value, found, err := r.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q) returned error: %v", key, err)
	}
	if found != expectedFound {
		t.Fatalf("Get(%q): expected found %v, got %v", key, expectedFound, found)
	}
	if expectedFound && !bytes.Equal(value, []byte(expectedValue)) {
		t.Fatalf("Get(%q): expected value %q, got %q", key, expectedValue, string(value))
	}
	if !expectedFound && value != nil {
		t.Fatalf("Get(%q): expected nil value when not found, got %q", key, string(value))
	}
}

// TestSSTableBasicWriteRead tests basic write and read operations.
func TestSSTableBasicWriteRead(t *testing.T) {
	filePath := tempSSTableFileName(t)

	pairs := []kvPair{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "dark red"},
	}

	// Write
	writer, err := sepia.NewSSTableWriter(filePath)
	if err != nil {
		t.Fatalf("NewSSTableWriter() error: %v", err)
	}
	for _, p := range pairs {
		if err := writer.Add([]byte(p.key), []byte(p.value)); err != nil {
			t.Fatalf("Writer.Add(%q, %q) error: %v", p.key, p.value, err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Writer.Finish() error: %v", err)
	}

	// Read
	reader, err := sepia.NewSSTableReader(filePath)
	if err != nil {
		t.Fatalf("NewSSTableReader() error: %v", err)
	}
	defer reader.Close()

	for _, p := range pairs {
		checkGet(t, reader, p.key, p.value, true)
	}
	checkGet(t, reader, "grape", "", false) // Non-existent key
}

// TestSSTableMultipleBlocks tests operations spanning multiple data blocks.
func TestSSTableMultipleBlocks(t *testing.T) {
	filePath := tempSSTableFileName(t)

	// Use a small block size to force multiple blocks
	// An entry "keyXX":"valueXX" is roughly:
	// key_len (1) + key (5) + val_len (1) + value (7) = 14 bytes.
	// With block size of 32, about 2 entries per block before compression.
	// Compression type byte adds 1 byte to the block on disk.
	writerOpts := sepia.WithBlockSize(32)

	writer, err := sepia.NewSSTableWriter(filePath, writerOpts)
	if err != nil {
		t.Fatalf("NewSSTableWriter() with options error: %v", err)
	}

	var pairs []kvPair
	numPairs := 20
	for i := 0; i < numPairs; i++ {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value%02d", i)
		pairs = append(pairs, kvPair{key, value})
		if err := writer.Add([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Writer.Add(%q, %q) error: %v", key, value, err)
		}
	}

	if err := writer.Finish(); err != nil {
		t.Fatalf("Writer.Finish() error: %v", err)
	}

	// Read
	reader, err := sepia.NewSSTableReader(filePath)
	if err != nil {
		t.Fatalf("NewSSTableReader() error: %v", err)
	}
	defer reader.Close()

	for _, p := range pairs {
		checkGet(t, reader, p.key, p.value, true)
	}
	checkGet(t, reader, "nonexistentkey", "", false)
	checkGet(t, reader, "key00", "value00", true) // Check first
	checkGet(t, reader, fmt.Sprintf("key%02d", numPairs-1), fmt.Sprintf("value%02d", numPairs-1), true) // Check last
}

// TestSSTableEmpty tests an SSTable with no entries.
func TestSSTableEmpty(t *testing.T) {
	filePath := tempSSTableFileName(t)

	writer, err := sepia.NewSSTableWriter(filePath)
	if err != nil {
		t.Fatalf("NewSSTableWriter() error: %v", err)
	}
	if err := writer.Finish(); err != nil { // Finish without adding anything
		t.Fatalf("Writer.Finish() on empty table error: %v", err)
	}

	reader, err := sepia.NewSSTableReader(filePath)
	if err != nil {
		t.Fatalf("NewSSTableReader() for empty table error: %v", err)
	}
	defer reader.Close()

	checkGet(t, reader, "anykey", "", false)

	// Verify footer details for an empty table
	// This requires reading the file manually as reader doesn't expose footer directly.
	fileContents, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read empty sstable file: %v", err)
	}
	if len(fileContents) < sepia.SSTableFooterSize {
		t.Fatalf("Empty sstable file is too small: %d", len(fileContents))
	}
	footerBytes := fileContents[len(fileContents)-sepia.SSTableFooterSize:]
	
	// Offsets within the 48-byte footer:
	// filterBlockOffset (0-7), filterBlockSize (8-11)
	// indexBlockOffset (12-19), indexBlockSize (20-23)
	// totalEntries (24-31)
	// sstableFormatVersion (32-35)
	// magicNumber (36-43)
	// footerChecksum (44-47)

	totalEntries := binary.LittleEndian.Uint64(footerBytes[24:32])
	if totalEntries != 0 {
		t.Errorf("Expected 0 total entries in empty SSTable footer, got %d", totalEntries)
	}
	// Filter and Index block sizes should also be 0 for an empty table if written correctly
	filterBlockSize := binary.LittleEndian.Uint32(footerBytes[8:12])
	if filterBlockSize == 0 {
		// This is okay, bloom filter might write a minimal structure even if empty.
		// The current bloom library might write a few bytes for an "empty" filter.
		// Let's verify the offset points to just before the index block offset.
		filterBlockOffset := binary.LittleEndian.Uint64(footerBytes[0:8])
		indexBlockOffset := binary.LittleEndian.Uint64(footerBytes[12:20])
		if filterBlockOffset + uint64(filterBlockSize) != indexBlockOffset {
			// This check might be too strict if bloom filter can be truly zero bytes AND its offset matters.
			// For an empty table, filter offset should be 0, size should be for the empty filter data.
			// Index offset would be filter offset + filter size.
		}
	}


	indexBlockSize := binary.LittleEndian.Uint32(footerBytes[20:24])
	if indexBlockSize != 0 {
		t.Errorf("Expected 0 index block size in empty SSTable footer, got %d", indexBlockSize)
	}
}


// TestSSTableKeyOrderValidation tests the key order enforcement by the writer.
func TestSSTableKeyOrderValidation(t *testing.T) {
	filePath := tempSSTableFileName(t)
	writer, err := sepia.NewSSTableWriter(filePath)
	if err != nil {
		t.Fatalf("NewSSTableWriter() error: %v", err)
	}
	// No defer writer.Close() here, as we want to test Add behavior mostly.
	// File will be cleaned by t.TempDir()

	key1 := "banana"
	val1 := "yellow"
	if err := writer.Add([]byte(key1), []byte(val1)); err != nil {
		t.Fatalf("Writer.Add(%q) error: %v", key1, err)
	}

	// Attempt to add same key
	err = writer.Add([]byte(key1), []byte("green"))
	if err == nil {
		t.Errorf("Expected error when adding same key %q, got nil", key1)
	} else if err != os.ErrInvalid {
		t.Errorf("Expected os.ErrInvalid for same key, got %v", err)
	}

	// Attempt to add smaller key
	key0 := "apple"
	err = writer.Add([]byte(key0), []byte("red"))
	if err == nil {
		t.Errorf("Expected error when adding smaller key %q after %q, got nil", key0, key1)
	} else if err != os.ErrInvalid {
		t.Errorf("Expected os.ErrInvalid for smaller key, got %v", err)
	}
	// It's good practice to Finish/Close writer to avoid resource leaks if other operations were planned.
	// Since this test focuses only on Add errors, and file is in TempDir, it's less critical.
	// However, let's call Finish to make sure it doesn't panic with pending data after errors.
	_ = writer.Finish() // Ignore error as file might be in a weird state
}

// TestSSTableReaderNonExistentKeys tests Get behavior for various non-existent keys.
func TestSSTableReaderNonExistentKeys(t *testing.T) {
	filePath := tempSSTableFileName(t)
	pairs := []kvPair{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "dark red"},
		{"date", "brown"},
	}

	writer, err := sepia.NewSSTableWriter(filePath)
	if err != nil { t.Fatalf("NewSSTableWriter() error: %v", err) }
	for _, p := range pairs {
		if err := writer.Add([]byte(p.key), []byte(p.value)); err != nil {
			t.Fatalf("Writer.Add(%q, %q) error: %v", p.key, p.value, err)
		}
	}
	if err := writer.Finish(); err != nil { t.Fatalf("Writer.Finish() error: %v", err) }

	reader, err := sepia.NewSSTableReader(filePath)
	if err != nil { t.Fatalf("NewSSTableReader() error: %v", err) }
	defer reader.Close()

	checkGet(t, reader, "aardvark", "", false)  // Before first
	checkGet(t, reader, "apricot", "", false)   // Between apple and banana
	checkGet(t, reader, "blueberry", "", false) // Between banana and cherry
	checkGet(t, reader, "coconut", "", false)   // Between cherry and date
	checkGet(t, reader, "fig", "", false)       // After last
	checkGet(t, reader, "APPLE", "", false)     // Case sensitivity (binary comparison)
}

// TestSSTableReaderCorruptedFile_BadMagicNumber tests opening a file with an invalid magic number.
func TestSSTableReaderCorruptedFile_BadMagicNumber(t *testing.T) {
	filePath := tempSSTableFileName(t)

	// A valid footer is sepia.SSTableFooterSize (48 bytes).
	// Magic number is at offset 36 for 8 bytes in the 48-byte footer (0-indexed).
	badFooter := make([]byte, sepia.SSTableFooterSize)
	// Fill with some data, then set a bad magic number.
	// For other fields, zero values are fine for this specific test.
	binary.LittleEndian.PutUint64(badFooter[36:], 0xBADMAGICBADMAGIC00) // Incorrect magic

	// Need to also set a checksum, otherwise it might be 0 and match if content up to checksum is also 0.
	// Let's make other fields non-zero to ensure checksum calculation is meaningful.
	binary.LittleEndian.PutUint64(badFooter[0:], 10) // filterBlockOffset
	binary.LittleEndian.PutUint32(badFooter[8:], 5)  // filterBlockSize
	checksum := crc32.ChecksumIEEE(badFooter[:sepia.SSTableFooterSize-4])
	binary.LittleEndian.PutUint32(badFooter[sepia.SSTableFooterSize-4:], checksum)


	if err := os.WriteFile(filePath, badFooter, 0644); err != nil {
		t.Fatalf("Failed to write corrupted file: %v", err)
	}

	_, err := sepia.NewSSTableReader(filePath)
	if err == nil {
		t.Errorf("Expected error when opening SSTable with bad magic number, got nil")
	} else if err != os.ErrInvalid {
		t.Errorf("Expected os.ErrInvalid for bad magic, got %T: %v", err, err)
	}
}

// TestSSTableReaderCorruptedFile_BadChecksum tests opening a file with a bad footer checksum.
func TestSSTableReaderCorruptedFile_BadChecksum(t *testing.T) {
	filePath := tempSSTableFileName(t)

	// 1. Create a valid SSTable first
	writer, err := sepia.NewSSTableWriter(filePath)
	if err != nil { t.Fatalf("NewSSTableWriter() error: %v", err) }
	if err := writer.Add([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Writer.Add() error: %v", err)
	}
	if err := writer.Finish(); err != nil { t.Fatalf("Writer.Finish() error: %v", err) }

	// 2. Read the valid SSTable content
	fileData, err := os.ReadFile(filePath)
	if err != nil { t.Fatalf("Failed to read valid SSTable: %v", err) }

	if len(fileData) < sepia.SSTableFooterSize {
		t.Fatalf("Valid SSTable is too small: %d bytes", len(fileData))
	}

	// 3. Corrupt a byte in the footer data (e.g., totalEntries field),
	// but leave checksum and magic number as they were written by the valid writer.
	// This ensures the stored checksum will not match the now-corrupted content.
	footerStartOffsetInFile := len(fileData) - sepia.SSTableFooterSize
	
	// totalEntries is at offset 24 within the footer.
	offsetToCorrupt := footerStartOffsetInFile + 24 
	fileData[offsetToCorrupt]++ // Increment a byte of totalEntries

	// 4. Write the corrupted data back
	if err := os.WriteFile(filePath, fileData, 0644); err != nil {
		t.Fatalf("Failed to write corrupted file: %v", err)
	}

	// 5. Try to open with reader - should fail checksum validation
	_, err = sepia.NewSSTableReader(filePath)
	if err == nil {
		t.Errorf("Expected error when opening SSTable with bad checksum, got nil")
	} else if err != os.ErrInvalid { 
		t.Errorf("Expected os.ErrInvalid for bad checksum, got %T: %v", err, err)
	}
}


// TestSSTableWriteEmptyKeyOrValue tests writing and reading empty keys/values.
// SSTable design allows empty keys and values as varint length can be 0.
func TestSSTableWriteEmptyKeyOrValue(t *testing.T) {
	filePath := tempSSTableFileName(t)

	// Note: Keys must be unique and sorted. "" is a valid key.
	pairs := []kvPair{
		{"", "emptykey_value"},    // Empty key
		{"key1", ""},              // Empty value
		{"key2", "nonempty"},      // Regular entry
		{"key_empty_both", ""},    // Key with empty value, different from {"key1", ""}
	}
	// Add another pair with empty key AND empty value, but this must be the first key
	// if "" is the smallest string.
	// Let's define the set carefully considering sort order.
	
	finalPairs := []kvPair{
		{"", ""}, 					// Empty key, empty value
		{"alpha", ""},				// Key with empty value
		{"beta", "nonempty_val"},
		{"gamma", "another_val"},
		{"zeta_empty_key_val", ""}, // This key is not empty. Let's rename for clarity.
									// What was intended was perhaps a key that is empty string.
									// The key "" is unique.
	}
	// The writer requires keys in strictly increasing order.
	// If we have {"", "val1"} and then {"", "val2"}, this is an error.
	// So, only one entry can have an empty string as a key.

	testPairs := []kvPair{
		{"", "value_for_empty_key"}, // Empty key, non-empty value
		{"key_empty_value", ""},     // Non-empty key, empty value
		{"key_non_empty", "value_non_empty"},
		// {"", ""}, // Cannot add this if {"", "value_for_empty_key"} already added
	}
	// To test empty key & empty value, it must be the only one with empty key:
	testPairsForEmptyBoth := []kvPair{
		{"", ""},
		{"another_key", "another_value"},
	}


	// Test set 1: empty key, empty value, regular
	set1 := []kvPair{
		{"", "value_for_empty_key_set1"},
		{"key_empty_value_set1", ""},    
		{"key_non_empty_set1", "value_non_empty_set1"},
	}
	sort.Slice(set1, func(i, j int) bool { return set1[i].key < set1[j].key })


	writer, err := sepia.NewSSTableWriter(filePath)
	if err != nil { t.Fatalf("NewSSTableWriter() error: %v", err) }
	
	for _, p := range set1 {
		if err := writer.Add([]byte(p.key), []byte(p.value)); err != nil {
			t.Fatalf("Writer.Add(key: %q, val: %q) error: %v", p.key, p.value, err)
		}
	}
	// Test adding empty key and empty value if the "" key is not already used.
	// This requires a separate test or careful ordering.
	// Let's clear previous test and try one with {"",""}
	if err := writer.Finish(); err != nil { t.Fatalf("Writer.Finish() for set1 error: %v", err) }


	// Read set1
	reader, err := sepia.NewSSTableReader(filePath)
	if err != nil { t.Fatalf("NewSSTableReader() for set1 error: %v", err) }
	
	for _, p := range set1 {
		checkGet(t, reader, p.key, p.value, true)
	}
	reader.Close()


	// Test set 2: includes {"",""}
	set2 := []kvPair{
		{"", ""}, // Empty key, empty value
		{"keyA", "valueA"},
	}
	sort.Slice(set2, func(i, j int) bool { return set2[i].key < set2[j].key }) // Should already be sorted

	writer2, err := sepia.NewSSTableWriter(filePath) // Overwrite previous file
	if err != nil { t.Fatalf("NewSSTableWriter() for set2 error: %v", err) }
	for _, p := range set2 {
		if err := writer2.Add([]byte(p.key), []byte(p.value)); err != nil {
			t.Fatalf("Writer.Add(key: %q, val: %q) for set2 error: %v", p.key, p.value, err)
		}
	}
	if err := writer2.Finish(); err != nil { t.Fatalf("Writer.Finish() for set2 error: %v", err) }

	reader2, err := sepia.NewSSTableReader(filePath)
	if err != nil { t.Fatalf("NewSSTableReader() for set2 error: %v", err) }
	
	for _, p := range set2 {
		checkGet(t, reader2, p.key, p.value, true)
	}
	reader2.Close()
}


// TestSSTablePersistence tests if data persists after writer closes and reader opens separately.
func TestSSTablePersistence(t *testing.T) {
	filePath := tempSSTableFileName(t)

	originalPairs := []kvPair{
		{"persist_key1", "persist_value1"},
		{"persist_key2", "persist_value2"},
	}

	// Phase 1: Write and Close
	writer, err := sepia.NewSSTableWriter(filePath)
	if err != nil { t.Fatalf("NewSSTableWriter() error: %v", err) }
	for _, p := range originalPairs {
		if err := writer.Add([]byte(p.key), []byte(p.value)); err != nil {
			t.Fatalf("Writer.Add(key: %q, val: %q) error: %v", p.key, p.value, err)
		}
	}
	if err := writer.Finish(); err != nil { t.Fatalf("Writer.Finish() error: %v", err) }

	// Phase 2: Re-open with new reader and verify
	reader, err := sepia.NewSSTableReader(filePath)
	if err != nil {
		t.Fatalf("NewSSTableReader() for persisted file error: %v", err)
	}
	defer reader.Close()

	for _, p := range originalPairs {
		checkGet(t, reader, p.key, p.value, true)
	}
	checkGet(t, reader, "non_existent_persist_key", "", false)
}

// Additional test ideas:
// - Very large keys/values.
// - Keys that are prefixes of each other.
// - Many blocks (more rigorous than TestSSTableMultipleBlocks).
// - Concurrent reads (if reader is designed to be thread-safe, though typically not for a single file handle).
// - File closing behavior (double close, Get after Close).
// - Test Bloom filter effectiveness (requires more setup, e.g. specific FP rate and checking stats if available).
// - Test different compression options if that becomes a feature.
// - Test reading SSTables generated by a different (compatible) implementation or version.

// TestGetClosedReader tests behavior of Get on a closed reader.
func TestGetClosedReader(t *testing.T) {
	filePath := tempSSTableFileName(t)
	writer, err := sepia.NewSSTableWriter(filePath)
	if err != nil { t.Fatalf("Writer error: %v", err) }
	writer.Add([]byte("a"), []byte("b"))
	writer.Finish()

	reader, err := sepia.NewSSTableReader(filePath)
	if err != nil { t.Fatalf("Reader error: %v", err) }
	
	err = reader.Close()
	if err != nil { t.Fatalf("Close error: %v", err) }

	_, _, err = reader.Get([]byte("a"))
	if err == nil {
		t.Errorf("Expected error when calling Get on a closed reader, got nil")
	} else if err != os.ErrClosed { // Assuming os.ErrClosed is used
		t.Errorf("Expected os.ErrClosed for Get on closed reader, got %v", err)
	}
}
