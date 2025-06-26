
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
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/wyhash"
)

// Writer facilitates the creation of an SST file in a streaming manner.
type Writer struct {
	w               *bufio.Writer
	offset          uint64
	indexEntries    []indexEntry
	dataBlockBuf    *bytes.Buffer
	restartPoints   []uint32
	prevKey         []byte
	entryCounter    int
	configs         SSTableConfigs
	firstKeyInBlock []byte
}

// NewWriter creates a new SST writer.
func NewWriter(w io.Writer, configs SSTableConfigs) *Writer {
	return &Writer{
		w:            bufio.NewWriter(w),
		dataBlockBuf: new(bytes.Buffer),
		configs:      configs,
	}
}

// Add appends a key-value pair to the current data block.
func (wr *Writer) Add(key, value []byte) error {
	if wr.prevKey != nil && bytes.Compare(wr.prevKey, key) >= 0 {
		return fmt.Errorf("keys must be added in ascending order")
	}

	if wr.dataBlockBuf.Len() == 0 {
		wr.firstKeyInBlock = key
	}

	if wr.entryCounter%int(wr.configs.RestartInterval) == 0 {
		wr.restartPoints = append(wr.restartPoints, uint32(wr.dataBlockBuf.Len()))
	}

	encodeEntry(wr.dataBlockBuf, wr.prevKey, KVEntry{Key: key, Value: value})
	wr.prevKey = append(wr.prevKey[:0], key...)
	wr.entryCounter++

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

	if err := wr.writeFooter(indexHandle, metaindexHandle); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	return wr.w.Flush()
}

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

	// Reset for the next data block
	wr.dataBlockBuf.Reset()
	wr.restartPoints = wr.restartPoints[:0]
	wr.prevKey = nil
	wr.entryCounter = 0
	wr.firstKeyInBlock = nil

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
	blockCipher, err := aes.NewCipher(wr.configs.EncryptionKey)
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

	encryptedData := gcm.Seal(nil, nonce, compressedData, nil)

	// Create block header
	blockHeader := BlockHeader{
		CompressionType: wr.configs.CompressionType,
	}
	copy(blockHeader.InitializationVector[:], nonce)
	copy(blockHeader.AuthenticationTag[:], encryptedData[len(encryptedData)-aes.BlockSize:])
	encryptedData = encryptedData[:len(encryptedData)-aes.BlockSize]

	// Serialize block header
	headerBuf := new(bytes.Buffer)
	binary.Write(headerBuf, binary.LittleEndian, byte(blockHeader.CompressionType))
	headerBuf.Write(blockHeader.InitializationVector[:])
	headerBuf.Write(blockHeader.AuthenticationTag[:])

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
	binary.Write(buf, binary.LittleEndian, byte(wr.configs.CompressionType))
	binary.Write(buf, binary.LittleEndian, wr.configs.BlockSize)
	binary.Write(buf, binary.LittleEndian, wr.configs.RestartInterval)
	binary.Write(buf, binary.LittleEndian, wr.configs.WyhashSeed)
	buf.Write(wr.configs.EncryptionKey)

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

func (wr *Writer) writeFooter(indexHandle, metaindexHandle blockHandle) error {
	footer := SSTFooter{
		MetaindexHandle: metaindexHandle,
		IndexHandle:     indexHandle,
		WyhashSeed:      wr.configs.WyhashSeed,
		Version:         1,
	}
	copy(footer.Magic[:], SST_V1_MAGIC)

	buf := make([]byte, SST_FOOTER_SIZE)
	binary.LittleEndian.PutUint64(buf[0:8], footer.MetaindexHandle.offset)
	binary.LittleEndian.PutUint64(buf[8:16], footer.MetaindexHandle.size)
	binary.LittleEndian.PutUint64(buf[16:24], footer.IndexHandle.offset)
	binary.LittleEndian.PutUint64(buf[24:32], footer.IndexHandle.size)
	binary.LittleEndian.PutUint64(buf[32:40], footer.WyhashSeed)
	copy(buf[40:56], footer.Magic[:])
	binary.LittleEndian.PutUint64(buf[56:64], footer.Version)

	_, err := wr.w.Write(buf)
	return err
}

func encodeIndexEntry(buf *bytes.Buffer, entry indexEntry) {
	var uvarintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(uvarintBuf[:], uint64(len(entry.firstKey)))
	buf.Write(uvarintBuf[:n])
	buf.Write(entry.firstKey)

	n = binary.PutUvarint(uvarintBuf[:], entry.blockHandle.offset)
	buf.Write(uvarintBuf[:n])
	n = binary.PutUvarint(uvarintBuf[:], entry.blockHandle.size)
	buf.Write(uvarintBuf[:n])
}
