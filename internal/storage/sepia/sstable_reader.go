package sepia

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sort"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/golang/snappy"
)

// footerData holds the parsed data from the SSTable footer.
type footerData struct {
	filterBlockOffset     uint64
	filterBlockSize       uint32
	indexBlockOffset      uint64
	indexBlockSize        uint32
	totalEntries          uint64
	sstableFormatVersion  uint32
	magic                 uint64
}

// indexEntryReader is used by the reader to store parsed index entries.
// It mirrors the structure the writer creates for the index.
type indexEntryReader struct {
	firstKey    []byte // The first key in the data block
	blockOffset uint64 // Offset of the data block in the file
	blockSize   uint32 // Size of the data block (compressed)
	numEntries  uint32 // Number of entries in this data block
}

// SSTableReader facilitates reading from an SSTable file.
type SSTableReader struct {
	filePath    string
	file        *os.File // File handle
	reader      *bufio.Reader // Buffered reader for the file
	footer      footerData
	bloomFilter *bloom.BloomFilter
	index       []indexEntryReader // Parsed index entries
	// Options can be added here if needed, e.g., for caching
}

// NewSSTableReader opens an SSTable file, reads its metadata (footer, filter, index),
// and prepares it for Get operations.
func NewSSTableReader(filePath string) (*SSTableReader, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	r := &SSTableReader{
		filePath: filePath,
		file:     file,
		reader:   bufio.NewReader(file),
	}

	// 1. Read and parse the footer
	if err := r.readFooter(); err != nil {
		file.Close()
		return nil, err
	}

	// 2. Read and parse the filter block
	if err := r.readFilterBlock(); err != nil {
		file.Close()
		return nil, err
	}

	// 3. Read and parse the index block
	if err := r.readIndexBlock(); err != nil {
		file.Close()
		return nil, err
	}
	
	return r, nil
}

// readFooter reads the footer from the end of the file, validates it, and populates r.footer.
func (r *SSTableReader) readFooter() error {
	// Footer is fixed size (48 bytes as per writer)
	footerBytes := make([]byte, 48)
	// Get file size to seek to the footer position
	fi, err := r.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()
	if fileSize < int64(len(footerBytes)) {
		return os.ErrInvalid // File too small to contain a footer
	}

	_, err = r.file.ReadAt(footerBytes, fileSize-int64(len(footerBytes)))
	if err != nil {
		return err
	}

	footerPos := 0
	r.footer.filterBlockOffset = binary.LittleEndian.Uint64(footerBytes[footerPos:])
	footerPos += 8
	r.footer.filterBlockSize = binary.LittleEndian.Uint32(footerBytes[footerPos:])
	footerPos += 4
	r.footer.indexBlockOffset = binary.LittleEndian.Uint64(footerBytes[footerPos:])
	footerPos += 8
	r.footer.indexBlockSize = binary.LittleEndian.Uint32(footerBytes[footerPos:])
	footerPos += 4
	r.footer.totalEntries = binary.LittleEndian.Uint64(footerBytes[footerPos:])
	footerPos += 8
	r.footer.sstableFormatVersion = binary.LittleEndian.Uint32(footerBytes[footerPos:])
	footerPos += 4
	r.footer.magic = binary.LittleEndian.Uint64(footerBytes[footerPos:])
	footerPos += 8 // Current position is 44

	// Verify magic number
	if r.footer.magic != MagicSepiaSSTable {
		return os.ErrInvalid // Or a custom error: ErrBadMagicNumber
	}
	
	// Verify checksum
	expectedChecksum := binary.LittleEndian.Uint32(footerBytes[footerPos:])
	actualChecksum := crc32.ChecksumIEEE(footerBytes[:footerPos]) // Checksum of bytes 0-43
	if actualChecksum != expectedChecksum {
		return os.ErrInvalid // Or a custom error: ErrBadFooterChecksum
	}

	return nil
}

// readFilterBlock reads and deserializes the Bloom filter from the file.
func (r *SSTableReader) readFilterBlock() error {
	if r.footer.filterBlockSize == 0 {
		// It's possible to have an empty filter block (e.g., if no entries or bloom filter disabled)
		// Initialize with an empty/default bloom filter.
		// Using 1 as estimatedN because bits-and-blooms/bloom/v3
		// NewWithEstimates(0, rate) results in k=0, which is not ideal.
		r.bloomFilter = bloom.NewWithEstimates(1, DefaultBloomFilterFPRate) // Default to avoid nil
		return nil
	}

	filterBytes := make([]byte, r.footer.filterBlockSize)
	_, err := r.file.ReadAt(filterBytes, int64(r.footer.filterBlockOffset))
	if err != nil {
		return err
	}

	r.bloomFilter = &bloom.BloomFilter{}
	err = r.bloomFilter.UnmarshalBinary(filterBytes)
	if err != nil {
		return err // Error deserializing bloom filter
	}
	return nil
}

// readIndexBlock reads and parses the index block from the file.
func (r *SSTableReader) readIndexBlock() error {
	if r.footer.indexBlockSize == 0 {
		r.index = []indexEntryReader{} // Empty index
		return nil
	}

	indexBytes := make([]byte, r.footer.indexBlockSize)
	_, err := r.file.ReadAt(indexBytes, int64(r.footer.indexBlockOffset))
	if err != nil {
		return err
	}

	// TODO: Decompress index block if it were compressed by the writer.
	// Currently, writer writes it uncompressed.

	buffer := bytes.NewReader(indexBytes)
	var entries []indexEntryReader

	for buffer.Len() > 0 {
		var entry indexEntryReader

		// Read key_length (varint)
		keyLen, err := binary.ReadUvarint(buffer)
		if err != nil {
			if err == io.EOF { break } // Normal end of buffer
			return err // Corrupted index data
		}

		// Read key (bytes)
		entry.firstKey = make([]byte, keyLen)
		if _, err := io.ReadFull(buffer, entry.firstKey); err != nil {
			return err // Corrupted index data
		}
		
		// Read block_offset (fixed64, LittleEndian)
		offsetBytes := make([]byte, 8)
		if _, err := io.ReadFull(buffer, offsetBytes); err != nil {
			return err
		}
		entry.blockOffset = binary.LittleEndian.Uint64(offsetBytes)

		// Read block_size (varint)
		blockSize, err := binary.ReadUvarint(buffer)
		if err != nil {
			return err
		}
		entry.blockSize = uint32(blockSize)

		// Read num_entries_in_block (varint)
		numEntries, err := binary.ReadUvarint(buffer)
		if err != nil {
			return err
		}
		entry.numEntries = uint32(numEntries)
		
		entries = append(entries, entry)
	}
	r.index = entries
	return nil
}


// Get retrieves the value for a given key from the SSTable.
// It returns (value, true, nil) if the key is found.
// It returns (nil, false, nil) if the key is not found.
// It returns (nil, false, error) if an error occurs during reading or parsing.
func (r *SSTableReader) Get(key []byte) ([]byte, bool, error) {
	if r.file == nil {
		return nil, false, os.ErrClosed
	}

	// 1. Check Bloom filter
	// Ensure bloom filter is not nil (it's initialized in NewSSTableReader)
	if r.bloomFilter != nil && !r.bloomFilter.Test(key) {
		return nil, false, nil // Key definitely not present
	}

	// 2. Find the relevant data block using the index
	blockIdx, found := r.findDataBlock(key)
	if !found {
		// This means key is smaller than the first key of any block,
		// or the index is empty.
		return nil, false, nil
	}
	
	// The key, if present, should be in the block identified by r.index[blockIdx].
	// findDataBlock ensures r.index[blockIdx].firstKey <= key.
	// (And if there's a next block i, key < r.index[i].firstKey)

	// 3. Read, decompress, and search the data block
	return r.readAndSearchDataBlock(r.index[blockIdx], key)
}

// readAndSearchDataBlock reads a specific data block, decompresses it,
// and scans for the key.
func (r *SSTableReader) readAndSearchDataBlock(blockEntry indexEntryReader, key []byte) ([]byte, bool, error) {
	// Allocate buffer for the compressed block data
	compressedBlockDataWithHeader := make([]byte, blockEntry.blockSize)
	_, err := r.file.ReadAt(compressedBlockDataWithHeader, int64(blockEntry.blockOffset))
	if err != nil {
		return nil, false, err // Error reading from file
	}

	if len(compressedBlockDataWithHeader) == 0 {
		// This shouldn't happen if blockSize > 0. If blockSize is 0, it's an empty block.
		return nil, false, nil 
	}

	// First byte is compression type, as written by SSTableWriter
	compressionType := compressedBlockDataWithHeader[0]
	actualBlockData := compressedBlockDataWithHeader[1:]
	var decompressedData []byte

	switch compressionType {
	case 0x00: // No compression
		decompressedData = actualBlockData
	case 0x01: // Snappy
		decompressedData, err = snappy.Decode(nil, actualBlockData)
		if err != nil {
			return nil, false, err // Decompression failed
		}
	default:
		// Or a more specific error like ErrUnknownCompressionType
		return nil, false, os.ErrInvalid 
	}

	// Scan the decompressed block for the key
	// Key-value pairs are stored as: key_len (varint), key, value_len (varint), value
	blockDataReader := bytes.NewReader(decompressedData)
	for blockDataReader.Len() > 0 {
		// Read key_length (varint)
		keyLen, err := binary.ReadUvarint(blockDataReader)
		if err != nil {
			if err == io.EOF { break } // Normal end of block
			return nil, false, err // Corrupted block data
		}
		if keyLen == 0 && blockDataReader.Len() == 0 { // Handle empty key at end of block if necessary
			break
		}


		// Read key (bytes)
		currentKey := make([]byte, keyLen)
		if _, err := io.ReadFull(blockDataReader, currentKey); err != nil {
			return nil, false, err // Corrupted block data
		}

		// Read value_length (varint)
		valLen, err := binary.ReadUvarint(blockDataReader)
		if err != nil {
			return nil, false, err // Corrupted block data
		}
		
		// Compare currentKey with target key
		cmp := bytes.Compare(currentKey, key)

		if cmp == 0 { // Key found
			value := make([]byte, valLen)
			if _, err := io.ReadFull(blockDataReader, value); err != nil {
				return nil, false, err // Corrupted block data
			}
			return value, true, nil
		} else if cmp > 0 {
			// Current key is greater than target key. Since keys are sorted within the block,
			// the target key is not in this block (if it were, we would have found it earlier).
			return nil, false, nil
		}
		
		// Key is not the one we're looking for (currentKey < key).
		// Skip the value bytes to move to the next entry.
		// Need to check if valLen is too large for the remaining buffer,
		// though ReadFull would catch this. Seek is more direct.
		if _, err := blockDataReader.Seek(int64(valLen), io.SeekCurrent); err != nil {
			// If seek fails (e.g. past EOF), it implies corrupted block or premature end.
			if err == io.EOF && valLen > 0 { // Trying to skip past valid EOF
				return nil, false, io.ErrUnexpectedEOF
			} else if err == io.EOF { // Normal EOF after skipping 0 bytes
				break
			}
			return nil, false, err
		}
	}

	return nil, false, nil // Key not found after scanning the entire block
}


// Close closes the SSTable reader and its underlying file.
func (r *SSTableReader) Close() error {
	if r.file != nil {
		err := r.file.Close()
		r.file = nil // Mark as closed
		return err
	}
	return nil // Already closed
}

// TODO: Helper for searching index
// TODO: Helper for reading and parsing data block
// TODO: Helper for scanning entries in a data block
// TODO: Snappy decompression helper (though `snappy.Decode` is straightforward)

// findDataBlock uses sort.Search to find the index entry for the block
// that *may* contain the given key.
// It returns the index of the entry in r.index, and a boolean indicating if a suitable block was found.
// The identified block is the one whose firstKey is the largest key less than or equal to the target key.
func (r *SSTableReader) findDataBlock(key []byte) (int, bool) {
	if len(r.index) == 0 {
		return 0, false
	}

	// sort.Search finds the smallest index i such that r.index[i].firstKey >= key.
	// We want the block whose firstKey is <= key.
	// So, if index[i].firstKey == key, block i is correct.
	// If index[i].firstKey > key, then block i-1 is the candidate (if i > 0).
	i := sort.Search(len(r.index), func(j int) bool {
		return bytes.Compare(r.index[j].firstKey, key) >= 0
	})

	if i < len(r.index) && bytes.Equal(r.index[i].firstKey, key) {
		// Exact match for the first key of a block
		return i, true
	}

	if i > 0 {
		// The key is greater than r.index[i-1].firstKey.
		// And (if i < len(r.index)) key is less than r.index[i].firstKey.
		// So, the key, if it exists, must be in block i-1.
		return i - 1, true
	}

	// If i == 0, it means the key is smaller than all firstKeys in the index.
	// In this case, the key cannot be in any block (unless the first block starts with a key
	// greater than the target key, which this logic correctly handles by returning false).
	return 0, false
}
Okay, I've created the initial structure for `sstable_reader.go`, including the `SSTableReader` struct, `footerData` struct, `indexEntryReader` struct, the `NewSSTableReader` constructor, and the helper methods `readFooter`, `readFilterBlock`, `readIndexBlock`, and a preliminary `findDataBlock`. The `Close` method is also present.

The `NewSSTableReader` now:
1.  Opens the file.
2.  Reads and parses the footer, including magic number and checksum verification.
3.  Reads and deserializes the Bloom filter using `UnmarshalBinary`.
4.  Reads and parses the index block into `[]indexEntryReader`. The index entry format matches what the writer produces: `key_len (varint), key (bytes), block_offset (fixed64), block_size (varint), num_entries_in_block (varint)`.

Next, I will implement the `Get(key []byte) ([]byte, bool, error)` method. This will involve:
1.  Using the Bloom filter.
2.  Calling `findDataBlock` to locate the relevant data block using the in-memory index.
3.  Reading the identified data block from disk.
4.  Checking the compression type byte and decompressing the block (using Snappy).
5.  Scanning the key-value pairs within the decompressed block to find the target key.
    *   This will require careful parsing of `key_len (varint), key, value_len (varint), value`.

I'll add a helper method, `readAndSearchDataBlock`, to encapsulate steps 3-5.
