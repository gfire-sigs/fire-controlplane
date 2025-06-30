package dsst

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dbloom"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/wyhash"
)

// Reader allows searching for keys within an SST file.
type Reader struct {
	r                 io.ReaderAt
	footer            SSTFooter
	index             []indexEntry
	configs           SSTableConfigs
	encryptionKey     []byte
	compare           func(key1, key2 []byte) int
	blockBloomFilters []blockBloomFilterEntry // Store bloom filters for each block
	blockCache        map[uint64][]byte       // Cache for decrypted and decompressed blocks
	cacheMutex        sync.RWMutex            // Mutex for thread-safe cache access
}

// NewReader initializes a new SST reader.
func NewReader(r io.ReaderAt, size int64, encryptionKey []byte, compare func(key1, key2 []byte) int) (*Reader, SSTableConfigs, error) {
	footer, err := readFooter(r, size)
	if err != nil {
		return nil, SSTableConfigs{}, fmt.Errorf("failed to read footer: %w", err)
	}

	configs, err := readMetaindexBlock(r, footer.MetaindexHandle)
	if err != nil {
		return nil, SSTableConfigs{}, fmt.Errorf("failed to read metaindex block: %w", err)
	}

	index, err := readIndexBlock(r, footer.IndexHandle, configs.WyhashSeed)
	if err != nil {
		return nil, SSTableConfigs{}, fmt.Errorf("failed to read index block: %w", err)
	}

	var blockBloomFilters []blockBloomFilterEntry
	if footer.BloomFilterHandle.size > 0 {
		blockBloomFilters, err = readBloomFilterBlock(r, footer.BloomFilterHandle, configs.WyhashSeed)
		if err != nil {
			return nil, SSTableConfigs{}, fmt.Errorf("failed to read bloom filter block: %w", err)
		}
	}

	return &Reader{
		r:                 r,
		footer:            footer,
		index:             index,
		configs:           configs,
		encryptionKey:     encryptionKey,
		compare:           compare,
		blockBloomFilters: blockBloomFilters,
		blockCache:        make(map[uint64][]byte),
	}, configs, nil
}

// Get searches for a key and returns its corresponding value.
func (rd *Reader) Get(key []byte) ([]byte, bool, error) {
	// Use the index to find the right data block
	blockHandle, found := rd.findDataBlock(key)
	if !found {
		return nil, false, nil
	}

	// Check bloom filter for the specific block
	for _, entry := range rd.blockBloomFilters {
		if entry.blockHandle.offset == blockHandle.offset && entry.blockHandle.size == blockHandle.size {
			if !entry.bloomFilter.Get(key) {
				return nil, false, nil // Key definitely not in this block
			}
			break
		}
	}

	// Read the data block and search for the key within it
	return rd.findInBlock(blockHandle, key)
}

func readFooter(r io.ReaderAt, size int64) (SSTFooter, error) {
	buf := make([]byte, SST_FOOTER_SIZE)
	_, err := r.ReadAt(buf, size-SST_FOOTER_SIZE)
	if err != nil {
		return SSTFooter{}, err
	}

	var footer SSTFooter
	footer.MetaindexHandle.offset = binary.LittleEndian.Uint64(buf[0:8])
	footer.MetaindexHandle.size = binary.LittleEndian.Uint64(buf[8:16])
	footer.IndexHandle.offset = binary.LittleEndian.Uint64(buf[16:24])
	footer.IndexHandle.size = binary.LittleEndian.Uint64(buf[24:32])
	footer.BloomFilterHandle.offset = binary.LittleEndian.Uint64(buf[32:40]) // New: BloomFilterHandle offset
	footer.BloomFilterHandle.size = binary.LittleEndian.Uint64(buf[40:48])   // New: BloomFilterHandle size
	footer.WyhashSeed = binary.LittleEndian.Uint64(buf[48:56])
	copy(footer.Magic[:], buf[56:72])                       // Adjusted offset
	footer.Version = binary.LittleEndian.Uint64(buf[72:80]) // Adjusted offset

	if string(footer.Magic[:]) != SST_V1_MAGIC {
		return SSTFooter{}, fmt.Errorf("invalid SST magic: got %s", string(footer.Magic[:]))
	}

	return footer, nil
}
func readMetaindexBlock(r io.ReaderAt, handle blockHandle) (SSTableConfigs, error) {
	buf := make([]byte, handle.size)
	_, err := r.ReadAt(buf, int64(handle.offset))
	if err != nil {
		return SSTableConfigs{}, err
	}

	reader := bytes.NewReader(buf)

	var configs SSTableConfigs
	compressionTypeByte, err := reader.ReadByte()
	if err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read compression type: %w", err)
	}
	configs.CompressionType = CompressionType(compressionTypeByte)

	// Read BlockSize (uint32)
	if _, err := io.ReadFull(reader, buf[0:4]); err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read block size: %w", err)
	}
	configs.BlockSize = binary.LittleEndian.Uint32(buf[0:4])

	// Read RestartInterval (uint32)
	if _, err := io.ReadFull(reader, buf[0:4]); err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read restart interval: %w", err)
	}
	configs.RestartInterval = binary.LittleEndian.Uint32(buf[0:4])

	// Read WyhashSeed (uint64)
	if _, err := io.ReadFull(reader, buf[0:8]); err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read wyhash seed: %w", err)
	}
	configs.WyhashSeed = binary.LittleEndian.Uint64(buf[0:8])

	// Read BloomFilterBitsPerKey (int)
	if _, err := io.ReadFull(reader, buf[0:4]); err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read bloom filter bits per key: %w", err)
	}
	configs.BloomFilterBitsPerKey = int(binary.LittleEndian.Uint32(buf[0:4]))

	// Read BloomFilterNumHashFuncs (int)
	if _, err := io.ReadFull(reader, buf[0:4]); err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read bloom filter num hash funcs: %w", err)
	}
	configs.BloomFilterNumHashFuncs = int(binary.LittleEndian.Uint32(buf[0:4]))

	return configs, nil
}

func readIndexBlock(r io.ReaderAt, handle blockHandle, wyhashSeed uint64) ([]indexEntry, error) {
	buf := make([]byte, handle.size)
	_, err := r.ReadAt(buf, int64(handle.offset))
	if err != nil {
		return nil, err
	}

	checksum := wyhash.Hash(buf[:len(buf)-8], wyhashSeed)
	expectedChecksum := binary.LittleEndian.Uint64(buf[len(buf)-8:])
	if checksum != expectedChecksum {
		return nil, fmt.Errorf("index block checksum mismatch")
	}

	var entries []indexEntry
	reader := bytes.NewReader(buf[:len(buf)-8])
	for reader.Len() > 0 {
		entry, err := decodeIndexEntry(reader)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func readBloomFilterBlock(r io.ReaderAt, handle blockHandle, wyhashSeed uint64) ([]blockBloomFilterEntry, error) {
	buf := make([]byte, handle.size)
	_, err := r.ReadAt(buf, int64(handle.offset))
	if err != nil {
		return nil, err
	}

	checksum := wyhash.Hash(buf[:len(buf)-8], wyhashSeed)
	expectedChecksum := binary.LittleEndian.Uint64(buf[len(buf)-8:])
	if checksum != expectedChecksum {
		return nil, fmt.Errorf("bloom filter block checksum mismatch")
	}

	var entries []blockBloomFilterEntry
	reader := bytes.NewReader(buf[:len(buf)-8])
	for reader.Len() > 0 {
		var uint64Buf [8]byte
		var n int
		n, err = io.ReadFull(reader, uint64Buf[:])
		if err != nil || n != 8 {
			return nil, fmt.Errorf("failed to read key length: %w", err)
		}
		keyLen := binary.LittleEndian.Uint64(uint64Buf[:])

		key := make([]byte, keyLen)
		_, err = io.ReadFull(reader, key)
		if err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}

		n, err = io.ReadFull(reader, uint64Buf[:])
		if err != nil || n != 8 {
			return nil, fmt.Errorf("failed to read bloom data length: %w", err)
		}
		bloomDataLen := binary.LittleEndian.Uint64(uint64Buf[:])

		bloomData := make([]byte, bloomDataLen)
		_, err = io.ReadFull(reader, bloomData)
		if err != nil {
			return nil, fmt.Errorf("failed to read bloom data: %w", err)
		}

		bf, err := dbloom.FromBytes(bloomData)
		if err != nil {
			return nil, fmt.Errorf("failed to create bloom filter from bytes: %w", err)
		}

		n, err = io.ReadFull(reader, uint64Buf[:])
		if err != nil || n != 8 {
			return nil, fmt.Errorf("failed to read block handle offset: %w", err)
		}
		offset := binary.LittleEndian.Uint64(uint64Buf[:])

		n, err = io.ReadFull(reader, uint64Buf[:])
		if err != nil || n != 8 {
			return nil, fmt.Errorf("failed to read block handle size: %w", err)
		}
		size := binary.LittleEndian.Uint64(uint64Buf[:])

		entries = append(entries, blockBloomFilterEntry{
			firstKey:    key,
			bloomFilter: bf,
			blockHandle: blockHandle{offset: offset, size: size},
		})
	}

	return entries, nil
}

func (rd *Reader) findDataBlock(key []byte) (blockHandle, bool) {
	// Binary search over the index entries
	low, high := 0, len(rd.index)-1
	for low <= high {
		mid := (low + high) / 2
		if rd.compare(key, rd.index[mid].firstKey) < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	if high < 0 {
		return blockHandle{}, false
	}

	return rd.index[high].blockHandle, true
}

func (rd *Reader) findInBlock(handle blockHandle, key []byte) ([]byte, bool, error) {
	// Use the block offset as the cache key
	cacheKey := handle.offset

	// Check if the block is in cache
	rd.cacheMutex.RLock()
	cachedBlock, found := rd.blockCache[cacheKey]
	rd.cacheMutex.RUnlock()

	var decodedBlock []byte
	var err error

	if found {
		decodedBlock = cachedBlock
	} else {
		rawBlock := make([]byte, handle.size)
		_, err = rd.r.ReadAt(rawBlock, int64(handle.offset))
		if err != nil {
			return nil, false, err
		}

		// Read block header
		blockHeader := BlockHeader{
			CompressionType: CompressionType(rawBlock[0]),
		}
		copy(blockHeader.InitializationVector[:], rawBlock[1:13])

		// Calculate checksum on the block data (excluding checksum itself).
		// The checksum is on the *encrypted* data + header.
		checksum := wyhash.Hash(rawBlock[:len(rawBlock)-8], rd.configs.WyhashSeed)
		expectedChecksum := binary.LittleEndian.Uint64(rawBlock[len(rawBlock)-8:])
		if checksum != expectedChecksum {
			return nil, false, fmt.Errorf("data block checksum mismatch")
		}

		// encryptedData starts after the header and ends before the checksum.
		// Authentication tag is now after the encrypted data as per updated spec.
		encryptedDataWithTag := rawBlock[13 : len(rawBlock)-8] // 1 byte for CompressionType + 12 bytes for IV

		// Decrypt the data
		blockCipher, err := aes.NewCipher(rd.encryptionKey)
		if err != nil {
			return nil, false, fmt.Errorf("failed to create AES cipher: %w", err)
		}
		gcm, err := cipher.NewGCM(blockCipher)
		if err != nil {
			return nil, false, fmt.Errorf("failed to create GCM: %w", err)
		}

		// Pre-allocate a buffer for decrypted data to avoid unnecessary allocations
		decryptedData, err := gcm.Open(encryptedDataWithTag[:0], blockHeader.InitializationVector[:], encryptedDataWithTag, nil)
		if err != nil {
			return nil, false, fmt.Errorf("failed to decrypt block: %w", err)
		}

		switch blockHeader.CompressionType {
		case CompressionTypeSnappy:
			decodedBlock, err = snappy.Decode(nil, decryptedData)
			if err != nil {
				return nil, false, fmt.Errorf("failed to decompress snappy block: %w", err)
			}
		case CompressionTypeZstd:
			reader := bytes.NewReader(decryptedData)
			zstdReader, err := zstd.NewReader(reader)
			if err != nil {
				return nil, false, fmt.Errorf("failed to create zstd reader: %w", err)
			}
			defer zstdReader.Close()
			decodedBlock, err = io.ReadAll(zstdReader)
			if err != nil {
				return nil, false, fmt.Errorf("failed to decompress zstd block: %w", err)
			}
		default:
			decodedBlock = decryptedData
		}

		// Store the decoded block in cache
		rd.cacheMutex.Lock()
		rd.blockCache[cacheKey] = decodedBlock
		rd.cacheMutex.Unlock()
	}

	// Extract restart points and their count from the end of the decoded block
	numRestartPoints := binary.LittleEndian.Uint32(decodedBlock[len(decodedBlock)-4:])
	restartPointsStart := len(decodedBlock) - 4 - int(numRestartPoints)*4

	restartPoints := make([]uint32, numRestartPoints)
	for i := 0; i < int(numRestartPoints); i++ {
		restartPoints[i] = binary.LittleEndian.Uint32(decodedBlock[restartPointsStart+i*4 : restartPointsStart+(i+1)*4])
	}

	// Find the starting offset using binary search on restart points
	startEntryOffset := uint32(0) // Default to start from the beginning of the block
	if numRestartPoints > 0 {
		low, high := 0, int(numRestartPoints)-1
		for low <= high {
			mid := (low + high) / 2
			currentOffset := restartPoints[mid]

			tempReader := bytes.NewReader(decodedBlock[:restartPointsStart])
			tempReader.Seek(int64(currentOffset), io.SeekStart)

			// Decode the key at the restart point (prevKey is nil for restart points)
			kv, err := dsstDecodeEntry(tempReader, nil)
			if err != nil {
				return nil, false, fmt.Errorf("failed to decode entry at restart point %d: %w", mid, err)
			}

			cmp := rd.compare(key, kv.Key)
			if cmp < 0 {
				high = mid - 1
			} else {
				startEntryOffset = currentOffset
				low = mid + 1
			}
			// Return the entry to the pool after use
			ReleaseKVEntry(&kv)
		}
	}

	dataReader := bytes.NewReader(decodedBlock[:restartPointsStart])
	dataReader.Seek(int64(startEntryOffset), io.SeekStart)

	var prevKey []byte
	// If we started from a non-zero offset, we need to re-decode entries from that offset
	// until we find the target key or pass it.
	// The `prevKey` for the first entry after seeking will be `nil` because restart points reset prefix compression.
	// Subsequent `prevKey` values will be correctly updated by the loop.

	for dataReader.Len() > 0 {
		kv, err := dsstDecodeEntry(dataReader, prevKey)
		if err != nil {
			return nil, false, err
		}

		// If the current key is greater than the target key, then the target key is not in this block.
		// This check is crucial for correctness after binary search.
		if rd.compare(kv.Key, key) > 0 {
			ReleaseKVEntry(&kv)
			return nil, false, nil
		}

		if bytes.Equal(kv.Key, key) {
			if kv.EntryType == EntryTypeTombstone {
				ReleaseKVEntry(&kv)
				return nil, false, nil // Found a tombstone, key is deleted
			}
			return kv.Value, true, nil
		}
		prevKey = kv.Key
		ReleaseKVEntry(&kv)
	}

	return nil, false, nil
}

func decodeIndexEntry(r *bytes.Reader) (indexEntry, error) {
	var uint64Buf [8]byte
	var n int
	n, err := io.ReadFull(r, uint64Buf[:])
	if err != nil || n != 8 {
		return indexEntry{}, fmt.Errorf("failed to read key length: %w", err)
	}
	keyLen := binary.LittleEndian.Uint64(uint64Buf[:])

	key := make([]byte, keyLen)
	_, err = io.ReadFull(r, key)
	if err != nil {
		return indexEntry{}, fmt.Errorf("failed to read key: %w", err)
	}

	n, err = io.ReadFull(r, uint64Buf[:])
	if err != nil || n != 8 {
		return indexEntry{}, fmt.Errorf("failed to read offset: %w", err)
	}
	offset := binary.LittleEndian.Uint64(uint64Buf[:])

	n, err = io.ReadFull(r, uint64Buf[:])
	if err != nil || n != 8 {
		return indexEntry{}, fmt.Errorf("failed to read size: %w", err)
	}
	size := binary.LittleEndian.Uint64(uint64Buf[:])

	return indexEntry{firstKey: key, blockHandle: blockHandle{offset: offset, size: size}}, nil
}

// Iterator allows iterating over the key-value pairs in an SST file.
type Iterator struct {
	rd           *Reader
	blockIndex   int // Current block index in rd.index
	blockIter    *bytes.Reader
	currentKV    *KVEntry
	prevKey      []byte
	decodedBlock []byte // Store the decoded block data
	// restartPoints stores the offsets of restart points within the current block.
	restartPoints []uint32
	// currentRestartPointIndex is the index of the current restart point within the restartPoints slice.
	currentRestartPointIndex int
	restartPointsStart       int // Start offset of restart points in decodedBlock
}

// NewIterator creates a new iterator for the Reader.
func (rd *Reader) Iterator() *Iterator {
	return &Iterator{
		rd: rd,
	}
}

// First positions the iterator at the first key in the SST file.
func (it *Iterator) First() {
	if len(it.rd.index) == 0 {
		it.blockIndex = -1
		return
	}
	it.blockIndex = 0
	it.loadBlock()
	it.Next() // Position at the first element
}

// Valid returns true if the iterator is positioned at a valid element.
func (it *Iterator) Valid() bool {
	return it.currentKV != nil
}

// Next moves the iterator to the next key.
func (it *Iterator) Next() {
	// If the current block iterator is exhausted, move to the next block.
	if it.blockIter == nil || it.blockIter.Len() == 0 {
		it.blockIndex++
		if it.blockIndex >= len(it.rd.index) {
			it.currentKV = nil // No more blocks, invalidate iterator.
			return
		}
		it.loadBlock()
		it.prevKey = nil // Reset prevKey for new block.
		it.currentRestartPointIndex = 0
	}

	// Decode the next entry from the current block.
	kv, err := dsstDecodeEntry(it.blockIter, it.prevKey)
	if err != nil {
		it.currentKV = nil // Invalidate iterator on error.
		return
	}
	// If there was a previous entry, return it to the pool before overwriting
	if it.currentKV != nil {
		ReleaseKVEntry(it.currentKV)
	}
	it.currentKV = &kv
	it.prevKey = kv.Key
}

// Key returns the key at the current iterator position.
func (it *Iterator) Key() []byte {
	if it.currentKV == nil {
		return nil
	}
	return it.currentKV.Key
}

// Value returns the value at the current iterator position.
func (it *Iterator) Value() []byte {
	if it.currentKV == nil || it.currentKV.EntryType == EntryTypeTombstone {
		return nil
	}
	return it.currentKV.Value
}

// Close releases the iterator's resources.
func (it *Iterator) Close() {
	// Return the current entry to the pool if it exists
	if it.currentKV != nil {
		ReleaseKVEntry(it.currentKV)
		it.currentKV = nil
	}
}

// Seek moves the iterator to the first key that is greater than or equal to the given key.
func (it *Iterator) Seek(key []byte) {
	// Find the block that might contain the key.
	blockHandle, found := it.rd.findDataBlock(key)
	if !found {
		it.currentKV = nil
		return
	}

	// Find the index of the block.
	for i, entry := range it.rd.index {
		if entry.blockHandle == blockHandle {
			it.blockIndex = i
			break
		}
	}

	it.loadBlock()

	// Binary search within the restart points to find the closest restart point.
	low, high := 0, len(it.restartPoints)-1
	startEntryOffset := uint32(0)
	for low <= high {
		mid := (low + high) / 2
		currentOffset := it.restartPoints[mid]

		tempReader := bytes.NewReader(it.decodedBlock[:it.restartPointsStart]) // Use the stored decodedBlock
		tempReader.Seek(int64(currentOffset), io.SeekStart)

		kv, err := dsstDecodeEntry(tempReader, nil) // prevKey is nil for restart points
		if err != nil {
			it.currentKV = nil
			return
		}

		cmp := it.rd.compare(key, kv.Key)
		if cmp <= 0 {
			startEntryOffset = currentOffset
			high = mid - 1
		} else {
			startEntryOffset = currentOffset
			low = mid + 1
		}
	}

	it.blockIter.Seek(int64(startEntryOffset), io.SeekStart)
	it.prevKey = nil // Reset prevKey after seeking within the block.

	// Iterate from the restart point until the key is found or passed.
	for it.blockIter.Len() > 0 {
		kv, err := dsstDecodeEntry(it.blockIter, it.prevKey)
		if err != nil {
			it.currentKV = nil
			return
		}
		// If there was a previous entry, return it to the pool before overwriting
		if it.currentKV != nil {
			ReleaseKVEntry(it.currentKV)
		}
		it.currentKV = &kv
		it.prevKey = kv.Key

		if it.rd.compare(it.currentKV.Key, key) >= 0 {
			return // Found the key or the first key greater than it.
		}
	}

	// If we reached the end of the block and didn't find the key, move to the next.
	it.Next()
}

func (it *Iterator) loadBlock() {
	handle := it.rd.index[it.blockIndex].blockHandle
	rawBlock := make([]byte, handle.size)
	_, err := it.rd.r.ReadAt(rawBlock, int64(handle.offset))
	if err != nil {
		// Handle error, perhaps log and invalidate iterator
		it.blockIter = nil
		return
	}

	// Read block header
	blockHeader := BlockHeader{
		CompressionType: CompressionType(rawBlock[0]),
	}
	copy(blockHeader.InitializationVector[:], rawBlock[1:13])

	// encryptedData starts after the header and ends before the checksum.
	// Authentication tag is now after the encrypted data as per updated spec.
	encryptedDataWithTag := rawBlock[13 : len(rawBlock)-8] // 1 byte for CompressionType + 12 bytes for IV
	encryptedData := encryptedDataWithTag[:len(encryptedDataWithTag)-aes.BlockSize]
	authTag := encryptedDataWithTag[len(encryptedDataWithTag)-aes.BlockSize:]

	// Decrypt the data
	blockCipher, err := aes.NewCipher(it.rd.encryptionKey)
	if err != nil {
		// Handle error
		it.blockIter = nil
		return
	}
	gcm, err := cipher.NewGCM(blockCipher)
	if err != nil {
		// Handle error
		it.blockIter = nil
		return
	}

	// Pre-allocate a buffer for decrypted data to avoid unnecessary allocations
	decryptedData := make([]byte, len(encryptedData))
	decryptedData, err = gcm.Open(decryptedData[:0], blockHeader.InitializationVector[:], append(encryptedData, authTag...), nil)
	if err != nil {
		// Handle error
		it.blockIter = nil
		return
	}

	var decodedBlock []byte
	switch blockHeader.CompressionType {
	case CompressionTypeSnappy:
		decodedBlock, err = snappy.Decode(nil, decryptedData)
		if err != nil {
			// Handle error
			it.blockIter = nil
			return
		}
	case CompressionTypeZstd:
		reader := bytes.NewReader(decryptedData)
		zstdReader, err := zstd.NewReader(reader)
		if err != nil {
			// Handle error
			it.blockIter = nil
			return
		}
		defer zstdReader.Close()
		decodedBlock, err = io.ReadAll(zstdReader)
		if err != nil {
			// Handle error
			it.blockIter = nil
			return
		}
	default:
		decodedBlock = decryptedData
	}

	// Extract restart points and their count from the end of the decoded block
	numRestartPoints := binary.LittleEndian.Uint32(decodedBlock[len(decodedBlock)-4:])
	it.restartPoints = make([]uint32, numRestartPoints)
	restartPointsStart := len(decodedBlock) - 4 - int(numRestartPoints)*4

	for i := 0; i < int(numRestartPoints); i++ {
		it.restartPoints[i] = binary.LittleEndian.Uint32(decodedBlock[restartPointsStart+i*4 : restartPointsStart+(i+1)*4])
	}

	// The blockIter should only contain the key-value entries, not restart points or checksum
	it.decodedBlock = decodedBlock // Store the decoded block
	it.restartPointsStart = restartPointsStart
	it.blockIter = bytes.NewReader(it.decodedBlock[:it.restartPointsStart])
	it.currentRestartPointIndex = 0
}
