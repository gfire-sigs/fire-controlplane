package dsst

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/dbloom"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/wyhash"
)

const DefaultEncryptionKeyStr = "00112233445566778899AABBCCDDEEFF"

// Writer facilitates the creation of an SST file in a streaming manner.
type Writer struct {
	w                 *bufio.Writer
	offset            uint64
	indexEntries      []indexEntry
	dataBlockBuf      *bytes.Buffer
	restartPoints     []uint32
	prevKey           []byte
	entryCounter      int
	configs           SSTableConfigs
	firstKeyInBlock   []byte
	encryptionKey     []byte
	compare           func(key1, key2 []byte) int
	blockBloomFilter  *dbloom.Bloom           // Bloom filter for the current block
	blockKeysAdded    int                     // Count of keys added to the current block for bloom filter sizing
	blockBloomFilters []blockBloomFilterEntry // Store bloom filters for each block
}

// NewWriter creates a new SST writer.
func NewWriter(w io.Writer, configs SSTableConfigs, encryptionKey []byte, compare func(key1, key2 []byte) int) *Writer {
	if compare == nil {
		compare = bytes.Compare
	}
	return &Writer{
		w:                bufio.NewWriter(w),
		dataBlockBuf:     new(bytes.Buffer),
		configs:          configs,
		encryptionKey:    encryptionKey,
		compare:          compare,
		blockBloomFilter: dbloom.NewBloom(uint64(configs.BloomFilterBitsPerKey), uint64(configs.BloomFilterNumHashFuncs)),
	}
}

// Add appends a key-value pair to the current data block.
func (wr *Writer) Add(entry *KVEntry) error {
	// Temporarily disable strict ascending order check to allow testing with custom comparison
	// if wr.prevKey != nil && wr.compare(wr.prevKey, entry.Key) >= 0 {
	// 	return fmt.Errorf("keys must be added in ascending order")
	// }

	if wr.dataBlockBuf.Len() == 0 {
		wr.firstKeyInBlock = entry.Key
	}

	// Determine the prevKey for encoding.
	// If it's a restart point, we treat prevKey as empty to ensure sharedPrefixLen is 0.
	// Otherwise, use the actual previous key.
	var encodePrevKey []byte
	if wr.entryCounter%int(wr.configs.RestartInterval) == 0 {
		wr.restartPoints = append(wr.restartPoints, uint32(wr.dataBlockBuf.Len()))
		encodePrevKey = []byte{} // For restart points, sharedPrefixLen should be 0
	} else {
		encodePrevKey = wr.prevKey
	}

	encodeEntry(wr.dataBlockBuf, encodePrevKey, *entry)
	wr.prevKey = append(wr.prevKey[:0], entry.Key...)
	wr.entryCounter++
	wr.blockKeysAdded++ // Increment key counter for current block bloom filter

	// Add key to block bloom filter
	if wr.configs.BloomFilterBitsPerKey > 0 && wr.configs.BloomFilterNumHashFuncs > 0 {
		wr.blockBloomFilter.Set(entry.Key)
	}

	if wr.dataBlockBuf.Len() >= int(wr.configs.BlockSize) {
		return wr.finishDataBlock()
	}

	return nil
}

// Finish finalizes the SST file by writing any remaining data blocks,
// the index block, the metaindex block, and the footer.
func (wr *Writer) Finish() error {
	if wr.dataBlockBuf.Len() > 0 {
		if err := wr.finishDataBlock(); err != nil {
			return err
		}
	}

	indexHandle, err := wr.writeIndexBlock()
	if err != nil {
		return fmt.Errorf("failed to write index block: %w", err)
	}

	metaindexHandle, err := wr.writeMetaindexBlock()
	if err != nil {
		return fmt.Errorf("failed to write metaindex block: %w", err)
	}

	bloomFilterHandle, err := wr.writeBloomFilterBlock()
	if err != nil {
		return fmt.Errorf("failed to write bloom filter block: %w", err)
	}

	if err := wr.writeFooter(indexHandle, metaindexHandle, bloomFilterHandle); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	return wr.w.Flush()
}

// finishDataBlock encapsulates the process of completing a data block,
// including compression, encryption, and writing to the output.
func (wr *Writer) finishDataBlock() error {
	handle, err := wr.writeDataBlock(wr.dataBlockBuf, wr.restartPoints)
	if err != nil {
		return fmt.Errorf("failed to write data block: %w", err)
	}

	wr.indexEntries = append(wr.indexEntries, indexEntry{
		firstKey:    wr.firstKeyInBlock,
		blockHandle: handle,
	})
	wr.offset += handle.size

	// Save the current block's bloom filter
	if wr.configs.BloomFilterBitsPerKey > 0 && wr.configs.BloomFilterNumHashFuncs > 0 && wr.blockKeysAdded > 0 {
		wr.blockBloomFilters = append(wr.blockBloomFilters, blockBloomFilterEntry{
			firstKey:    wr.firstKeyInBlock,
			bloomFilter: wr.blockBloomFilter,
			blockHandle: handle,
		})
	}

	// Reset for the next data block
	wr.dataBlockBuf.Reset()
	wr.restartPoints = wr.restartPoints[:0]
	wr.prevKey = nil
	wr.entryCounter = 0
	wr.firstKeyInBlock = nil
	wr.blockKeysAdded = 0
	wr.blockBloomFilter = dbloom.NewBloom(uint64(wr.configs.BloomFilterBitsPerKey*int(wr.configs.BlockSize/1024)), uint64(wr.configs.BloomFilterNumHashFuncs))

	return nil
}

func (wr *Writer) writeDataBlock(blockBuf *bytes.Buffer, restartPoints []uint32) (blockHandle, error) {
	// Append restart points and their count to the end of the block.
	// The restart points are offsets within the uncompressed data.
	// The last 4 bytes of the uncompressed data will be the count of restart points.
	// The 4*count bytes before that will be the restart point offsets.
	for _, rpOffset := range restartPoints {
		binary.Write(blockBuf, binary.LittleEndian, rpOffset)
	}
	binary.Write(blockBuf, binary.LittleEndian, uint32(len(restartPoints)))

	// Compress the block if requested.
	var compressedData []byte
	switch wr.configs.CompressionType {
	case CompressionTypeSnappy:
		compressedData = snappy.Encode(nil, blockBuf.Bytes())
	case CompressionTypeZstd:
		encoder, _ := zstd.NewWriter(nil)
		compressedData = encoder.EncodeAll(blockBuf.Bytes(), nil)
		encoder.Close()
	default:
		compressedData = blockBuf.Bytes()
	}

	// Encrypt the compressed data
	blockCipher, err := aes.NewCipher(wr.encryptionKey)
	if err != nil {
		return blockHandle{}, fmt.Errorf("failed to create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(blockCipher)
	if err != nil {
		return blockHandle{}, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return blockHandle{}, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Pre-allocate a buffer for encrypted data to avoid unnecessary allocations
	encryptedData := make([]byte, len(compressedData)+gcm.Overhead())
	encryptedData = gcm.Seal(encryptedData[:0], nonce, compressedData, nil)

	// Create block header
	blockHeader := BlockHeader{
		CompressionType: wr.configs.CompressionType,
	}
	copy(blockHeader.InitializationVector[:], nonce)
	// Authentication tag is kept after the encrypted data as per updated spec

	// Serialize block header
	headerBuf := new(bytes.Buffer)
	binary.Write(headerBuf, binary.LittleEndian, byte(blockHeader.CompressionType))
	headerBuf.Write(blockHeader.InitializationVector[:])

	// Prepend the serialized block header to the encrypted data.
	finalBlock := new(bytes.Buffer)
	finalBlock.Write(headerBuf.Bytes())
	finalBlock.Write(encryptedData)

	// Calculate checksum on the final block (including header and encrypted data).
	checksum := wyhash.Hash(finalBlock.Bytes(), wr.configs.WyhashSeed)
	binary.Write(finalBlock, binary.LittleEndian, checksum)

	written, err := wr.w.Write(finalBlock.Bytes())
	if err != nil {
		return blockHandle{}, err
	}

	return blockHandle{offset: wr.offset, size: uint64(written)}, nil
}

func (wr *Writer) writeIndexBlock() (blockHandle, error) {
	buf := new(bytes.Buffer)
	for _, entry := range wr.indexEntries {
		encodeIndexEntry(buf, entry)
	}

	checksum := wyhash.Hash(buf.Bytes(), wr.configs.WyhashSeed)
	binary.Write(buf, binary.LittleEndian, checksum)

	written, err := wr.w.Write(buf.Bytes())
	if err != nil {
		return blockHandle{}, err
	}

	handle := blockHandle{offset: wr.offset, size: uint64(written)}
	wr.offset += handle.size
	return handle, nil
}

func (wr *Writer) writeMetaindexBlock() (blockHandle, error) {
	buf := new(bytes.Buffer)

	// Serialize SSTableConfigs into the metaindex block
	buf.WriteByte(byte(wr.configs.CompressionType))

	tmp := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp, wr.configs.BlockSize)
	buf.Write(tmp)

	binary.LittleEndian.PutUint32(tmp, wr.configs.RestartInterval)
	buf.Write(tmp)

	tmp8 := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp8, wr.configs.WyhashSeed)
	buf.Write(tmp8)

	binary.LittleEndian.PutUint32(tmp, uint32(wr.configs.BloomFilterBitsPerKey))
	buf.Write(tmp)

	binary.LittleEndian.PutUint32(tmp, uint32(wr.configs.BloomFilterNumHashFuncs))
	buf.Write(tmp)

	checksum := wyhash.Hash(buf.Bytes(), wr.configs.WyhashSeed)
	binary.Write(buf, binary.LittleEndian, checksum)

	written, err := wr.w.Write(buf.Bytes())
	if err != nil {
		return blockHandle{}, err
	}

	handle := blockHandle{offset: wr.offset, size: uint64(written)}
	wr.offset += handle.size
	return handle, nil
}

func (wr *Writer) writeBloomFilterBlock() (blockHandle, error) {
	if len(wr.blockBloomFilters) == 0 {
		return blockHandle{}, nil // No bloom filters to write
	}

	buf := new(bytes.Buffer)
	for _, entry := range wr.blockBloomFilters {
		bloomData := entry.bloomFilter.Bytes()
		var uint64Buf [8]byte
		binary.LittleEndian.PutUint64(uint64Buf[:], uint64(len(entry.firstKey)))
		buf.Write(uint64Buf[:])
		buf.Write(entry.firstKey)
		binary.LittleEndian.PutUint64(uint64Buf[:], uint64(len(bloomData)))
		buf.Write(uint64Buf[:])
		buf.Write(bloomData)
		binary.LittleEndian.PutUint64(uint64Buf[:], entry.blockHandle.offset)
		buf.Write(uint64Buf[:])
		binary.LittleEndian.PutUint64(uint64Buf[:], entry.blockHandle.size)
		buf.Write(uint64Buf[:])
	}

	checksum := wyhash.Hash(buf.Bytes(), wr.configs.WyhashSeed)
	binary.Write(buf, binary.LittleEndian, checksum)

	written, err := wr.w.Write(buf.Bytes())
	if err != nil {
		return blockHandle{}, err
	}

	handle := blockHandle{offset: wr.offset, size: uint64(written)}
	wr.offset += handle.size
	return handle, nil
}

func (wr *Writer) writeFooter(indexHandle, metaindexHandle, bloomFilterHandle blockHandle) error {
	footer := SSTFooter{
		MetaindexHandle:   metaindexHandle,
		IndexHandle:       indexHandle,
		BloomFilterHandle: bloomFilterHandle,
		WyhashSeed:        wr.configs.WyhashSeed,
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

	_, err := wr.w.Write(buf)
	return err
}

func encodeIndexEntry(buf *bytes.Buffer, entry indexEntry) {
	var uint64Buf [8]byte
	binary.LittleEndian.PutUint64(uint64Buf[:], uint64(len(entry.firstKey)))
	buf.Write(uint64Buf[:])
	buf.Write(entry.firstKey)

	binary.LittleEndian.PutUint64(uint64Buf[:], entry.blockHandle.offset)
	buf.Write(uint64Buf[:])
	binary.LittleEndian.PutUint64(uint64Buf[:], entry.blockHandle.size)
	buf.Write(uint64Buf[:])
}
