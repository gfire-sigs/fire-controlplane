package sepia

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/golang/snappy"
)

// indexEntry stores information about a data block for the index.
type indexEntry struct {
	firstKey     []byte
	blockOffset  uint64
	blockSize    uint32 // Compressed size
	numEntries   uint32 // Number of entries in this block
}

// SSTableWriterOptions holds configuration for the SSTable writer.
type SSTableWriterOptions struct {
	BlockSize         int
	BloomFilterFPRate float64
	// Future options: CompressionType, ChecksumType, etc.
}

// WriterOption is a function type for setting writer options.
type WriterOption func(*SSTableWriterOptions)

// WithBlockSize sets the target block size for data blocks.
func WithBlockSize(size int) WriterOption {
	return func(opts *SSTableWriterOptions) {
		if size > 0 {
			opts.BlockSize = size
		}
	}
}

// WithBloomFilterFPRate sets the desired false positive rate for the Bloom filter.
func WithBloomFilterFPRate(fpRate float64) WriterOption {
	return func(opts *SSTableWriterOptions) {
		if fpRate > 0 && fpRate < 1 {
			opts.BloomFilterFPRate = fpRate
		}
	}
}

// SSTableWriter is responsible for writing key-value pairs to an SSTable file.
type SSTableWriter struct {
	filePath    string
	file        *os.File
	writer      *bufio.Writer
	options     SSTableWriterOptions
	offset      uint64 // Current write offset in the file

	// Data block buffering
	dataBlockBuffer []byte   // Uncompressed data for the current block
	currentBlockEntries uint32 // Number of entries in the current data block
	firstKeyInBlock []byte   // First key in the current data block

	// Index and filter
	indexEntries []indexEntry
	bloomFilter  *bloom.BloomFilter
	numEntries   uint64 // Total number of entries added to the SSTable
	lastKey      []byte // Last key added, to ensure keys are in increasing order
}

// NewSSTableWriter creates a new SSTable writer.
// The caller is responsible for closing the writer using Finish().
func NewSSTableWriter(filePath string, opts ...WriterOption) (*SSTableWriter, error) {
	options := SSTableWriterOptions{
		BlockSize:         DefaultBlockSize,
		BloomFilterFPRate: DefaultBloomFilterFPRate,
	}
	for _, opt := range opts {
		opt(&options)
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	// Initialize bloom filter with a large capacity initially, will be refined later
	// if we know the number of entries beforehand. For now, estimating 1 million entries.
	// This should ideally be configurable or estimated based on expected data size.
	// For the purpose of this implementation, we'll create it, and its actual parameters
	// will be determined by the keys added.
	// The design doc mentions storing m and k, or m/n. bits-and-blooms stores m and k.
	// We will create it before adding keys.
	// A better approach for unknown N might be to estimate N or have a fixed size filter.
	// For now, we'll create it in Finish() once N is known.
	// Or, we can initialize with a very rough estimate if Finish() is too late.
	// Let's initialize it here with a placeholder size, it will be created properly in Finish or when first key is added.

	writer := &SSTableWriter{
		filePath:    filePath,
		file:        file,
		writer:      bufio.NewWriter(file),
		options:     options,
		offset:      0,
		// bloomFilter will be initialized later, or with estimated N if we choose to.
		// For now, let's defer bloom filter initialization until Finish() when N is known.
	}

	// Placeholder for bloom filter until we know the number of entries (N)
	// In a real scenario, if N is unknown, we might use a default size or require an estimate.
	// The bloom filter will be initialized in the Add method upon the first key addition.
	// A default estimate for N will be used if not specified in options (currently not an option).

	return writer, nil
}

// Add appends a key-value pair to the SSTable.
// Keys must be added in strictly increasing lexicographical order.
func (w *SSTableWriter) Add(key []byte, value []byte) error {
	if w.file == nil {
		return os.ErrClosed // Or a custom error indicating the writer is closed
	}

	// Check for out-of-order keys
	// Note: A direct string conversion might be slow for very frequent, short keys.
	// Consider using bytes.Compare for performance critical paths if this becomes a bottleneck.
	if w.lastKey != nil && string(key) <= string(w.lastKey) {
		// Consider returning a more specific error, e.g., ErrKeyOutOfOrder
		return os.ErrInvalid // Using os.ErrInvalid as a placeholder for specific error
	}
	// Store a copy of the key for the next comparison
	w.lastKey = append(make([]byte, 0, len(key)), key...)


	// Initialize bloom filter on first Add call.
	if w.bloomFilter == nil {
		// Estimate N for bloom filter. This is a placeholder.
		// Ideally, N should be provided via options or estimated more accurately.
		// Let's use a default of 1,000,000 items for estimation.
		// The bits-and-blooms library calculates M and K from N and FP rate.
		estimatedN := uint(1000000) // TODO: Make this configurable or improve estimation
		// If w.options had an EstimatedNumEntries, we'd use that.
		w.bloomFilter = bloom.NewWithEstimates(estimatedN, w.options.BloomFilterFPRate)
	}
	w.bloomFilter.Add(key)

	if w.firstKeyInBlock == nil {
		// Store a copy of the first key for the current block
		w.firstKeyInBlock = append(make([]byte, 0, len(key)), key...)
	}

	// Entry format: key_length (varint), key, value_length (varint), value
	// Calculate required size to avoid multiple allocations for 'entry'
	entrySize := binary.MaxVarintLen32 + len(key) + binary.MaxVarintLen32 + len(value)
	entry := make([]byte, 0, entrySize)
	entry = binary.AppendUvarint(entry, uint64(len(key)))
	entry = append(entry, key...)
	entry = binary.AppendUvarint(entry, uint64(len(value)))
	entry = append(entry, value...)

	// If current block + new entry exceeds block size, flush current block.
	// Also, flush if dataBlockBuffer is empty but this new entry itself exceeds the block size (edge case for very large entries).
	if (len(w.dataBlockBuffer) > 0 && len(w.dataBlockBuffer)+len(entry) > w.options.BlockSize) ||
		(len(w.dataBlockBuffer) == 0 && len(entry) > w.options.BlockSize) {
		if err := w.flushDataBlock(); err != nil {
			return err
		}
		// After flush, the current key becomes the first key of the new block
		w.firstKeyInBlock = append(make([]byte, 0, len(key)), key...)
	}

	w.dataBlockBuffer = append(w.dataBlockBuffer, entry...)
	w.currentBlockEntries++
	w.numEntries++

	return nil
}

// flushDataBlock compresses and writes the current data block to disk
// and updates the index.
func (w *SSTableWriter) flushDataBlock() error {
	if len(w.dataBlockBuffer) == 0 {
		return nil
	}

	// Compress the block
	compressedBlock := snappy.Encode(nil, w.dataBlockBuffer)
	
	// Prepend the compression type byte.
	// 0x00 = No compression, 0x01 = Snappy
	blockToWrite := make([]byte, 1+len(compressedBlock))
	blockToWrite[0] = 0x01 // 0x01 for Snappy
	copy(blockToWrite[1:], compressedBlock)


	// Write compressed block (with compression type prefix) to file
	n, err := w.writer.Write(blockToWrite)
	if err != nil {
		return err
	}
	if n != len(blockToWrite) {
		return os.ErrShortWrite 
	}

	// Add index entry
	// Ensure a copy of firstKeyInBlock is stored, as it might be reused.
	clonedFirstKey := append([]byte(nil), w.firstKeyInBlock...)
	w.indexEntries = append(w.indexEntries, indexEntry{
		firstKey:    clonedFirstKey,
		blockOffset: w.offset,
		blockSize:   uint32(n), // Compressed size including the compression type byte
		numEntries:  w.currentBlockEntries,
	})

	w.offset += uint64(n)

	// Reset current block buffer and related fields
	w.dataBlockBuffer = w.dataBlockBuffer[:0] // Reuse buffer
	w.currentBlockEntries = 0
	// firstKeyInBlock will be set by the next key that starts a new block,
	// or explicitly cleared if we want to be super safe (already nilled by Add if flush occurs).
	w.firstKeyInBlock = nil 


	return nil
}

// Finish finalizes the SSTable by writing out any remaining data,
// the filter block, the index block, and the footer. It then closes the file.
func (w *SSTableWriter) Finish() error {
	if w.file == nil {
		return os.ErrClosed // Already closed or not opened
	}

	// Ensure writer is flushed and file is closed at the end
	defer func() {
		if w.writer != nil {
			// Before closing the file, ensure all buffered data in bufio.Writer is written to the OS.
			if err := w.writer.Flush(); err != nil {
				// Log or handle this error? For now, rely on file.Close() to also flush.
				// However, explicit flush is safer.
				// If flush fails, file.Close() might also fail or hide the original error.
				// Consider how to propagate this error if it occurs.
			}
		}
		if w.file != nil {
			w.file.Close()
			w.file = nil // Mark as closed to prevent reuse
		}
	}()

	// 1. Flush any remaining data in the current data block
	if err := w.flushDataBlock(); err != nil {
		return err
	}

	// If no entries were added, we need a valid bloom filter.
	// Add() initializes it. If Add() was never called, numEntries is 0.
	// The bloom filter might be nil. Initialize it to an empty (but valid) one.
	if w.bloomFilter == nil {
		// Using 1 as estimatedN because bits-and-blooms/bloom/v3
		// NewWithEstimates(0, rate) results in k=0, which is not ideal.
		// A filter for 0 items should ideally be empty and take minimal space.
		w.bloomFilter = bloom.NewWithEstimates(1, w.options.BloomFilterFPRate)
	}


	// 2. Write the Filter Block (Bloom Filter)
	filterBlockOffset := w.offset
	var filterBlockSize uint32

	// Use bloomFilter.MarshalBinary() as it's the library's way to serialize.
	// The design doc asks for "underlying bit array" and "parameters".
	// MarshalBinary includes this necessary information for UnmarshalBinary to reconstruct.
	// If a specific cross-language format is needed, manual serialization of
	// K, M (or N, FP), and the bitset would be required. This is simpler for now.
	filterData, err := w.bloomFilter.MarshalBinary()
	if err != nil {
		// This error comes from the bloom filter library itself during marshaling.
		return err
	}

	if len(filterData) > 0 {
		n, err := w.writer.Write(filterData)
		if err != nil {
			return err
		}
		filterBlockSize = uint32(n)
		w.offset += uint64(n)
	} else {
		filterBlockSize = 0 // No data written if filterData is empty
	}


	// 3. Write the Index Block
	// The index block consists of a sequence of index entries.
	// Entry Format from design: key_length (varint), key (bytes),
	// block_offset (varint/fixed64), block_size (varint).
	// My indexEntry struct also includes numEntries, which is an extension.
	// I'll use fixed64 for block_offset for simplicity in writing/reading.
	indexBlockOffset := w.offset
	
	// Use a temporary buffer to build the index block first.
	// This avoids writing partial data if an error occurs mid-way
	// and also makes it easier to get the total size for the footer.
	var indexBuffer bytes.Buffer 

	for _, entry := range w.indexEntries {
		// key_length (varint)
		keyLenBytes := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(keyLenBytes, uint64(len(entry.firstKey)))
		indexBuffer.Write(keyLenBytes[:n])

		// key (bytes)
		indexBuffer.Write(entry.firstKey)
		
		// block_offset (fixed64, LittleEndian)
		offsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(offsetBytes, entry.blockOffset)
		indexBuffer.Write(offsetBytes)

		// block_size (varint) - using uint32 for block size as per struct
		sizeBytes := make([]byte, binary.MaxVarintLen32)
		n = binary.PutUvarint(sizeBytes, uint64(entry.blockSize))
		indexBuffer.Write(sizeBytes[:n])

		// num_entries_in_block (varint) - my extension
		numEntriesBytes := make([]byte, binary.MaxVarintLen32)
		n = binary.PutUvarint(numEntriesBytes, uint64(entry.numEntries))
		indexBuffer.Write(numEntriesBytes[:n])
	}
	
	indexBlockBytes := indexBuffer.Bytes()
	// TODO: Optional: Compress index block (as per design doc). For now, uncompressed.
	
	if len(indexBlockBytes) > 0 {
		nWritten, err := w.writer.Write(indexBlockBytes)
		if err != nil {
			return err
		}
		if nWritten != len(indexBlockBytes) {
			return os.ErrShortWrite
		}
		w.offset += uint64(nWritten)
	}
	indexBlockSize := uint32(len(indexBlockBytes))


	// 4. Write the Footer
	// Footer structure (48 bytes total as per design discussion):
	// 1. filter_block_offset (uint64) - 8 bytes
	// 2. filter_block_size (uint32)   - 4 bytes
	// 3. index_block_offset (uint64)  - 8 bytes
	// 4. index_block_size (uint32)    - 4 bytes
	// 5. total_entries (uint64)       - 8 bytes
	// 6. sstable_format_version (uint32) - 4 bytes (e.g., 1)
	// 7. magic_number (uint64)        - 8 bytes (MagicSepiaSSTable)
	// 8. footer_checksum (uint32)     - 4 bytes (CRC32 of bytes 0-43 of footer)

	footerBytes := make([]byte, 48)
	footerPos := 0

	binary.LittleEndian.PutUint64(footerBytes[footerPos:], filterBlockOffset)
	footerPos += 8
	binary.LittleEndian.PutUint32(footerBytes[footerPos:], filterBlockSize)
	footerPos += 4
	binary.LittleEndian.PutUint64(footerBytes[footerPos:], indexBlockOffset)
	footerPos += 8
	binary.LittleEndian.PutUint32(footerBytes[footerPos:], indexBlockSize)
	footerPos += 4
	binary.LittleEndian.PutUint64(footerBytes[footerPos:], w.numEntries)
	footerPos += 8
	
	sstableFormatVersion := uint32(1) 
	binary.LittleEndian.PutUint32(footerBytes[footerPos:], sstableFormatVersion)
	footerPos += 4

	binary.LittleEndian.PutUint64(footerBytes[footerPos:], MagicSepiaSSTable)
	footerPos += 8 // Current position is 44

	// Calculate checksum of footer contents (first 44 bytes, excluding the checksum itself)
	checksum := crc32.ChecksumIEEE(footerBytes[:footerPos]) // Checksum of bytes 0 to 43
	binary.LittleEndian.PutUint32(footerBytes[footerPos:], checksum)
	// footerPos += 4 // No need to increment, checksum is the last part. Total 48 bytes.

	if _, err := w.writer.Write(footerBytes); err != nil {
		return err
	}

	// Flush bufio.Writer and close file (handled by defer)
	return nil
}

// Close is an alias for Finish to satisfy common interfaces,
// but Finish() is the primary method to finalize the SSTable.
func (w *SSTableWriter) Close() error {
	return w.Finish()
}
