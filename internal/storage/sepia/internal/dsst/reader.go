
package dsst

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/wyhash"
)

// Reader allows searching for keys within an SST file.
type Reader struct {
	r       io.ReaderAt
	footer  SSTFooter
	index   []indexEntry
	configs SSTableConfigs
}

// NewReader initializes a new SST reader.
func NewReader(r io.ReaderAt, size int64) (*Reader, SSTableConfigs, error) {
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

	return &Reader{
		r:       r,
		footer:  footer,
		index:   index,
		configs: configs,
	}, configs, nil
}

// Get searches for a key and returns its corresponding value.
func (rd *Reader) Get(key []byte) ([]byte, bool, error) {
	// Use the index to find the right data block
	blockHandle, found := rd.findDataBlock(key)
	if !found {
		return nil, false, nil
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
	footer.WyhashSeed = binary.LittleEndian.Uint64(buf[32:40])
	copy(footer.Magic[:], buf[40:56])
	footer.Version = binary.LittleEndian.Uint64(buf[56:64])

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

	if err := binary.Read(reader, binary.LittleEndian, &configs.BlockSize); err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read block size: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &configs.RestartInterval); err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read restart interval: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &configs.WyhashSeed); err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read wyhash seed: %w", err)
	}

	configs.EncryptionKey = make([]byte, 32) // AES-256 key is 32 bytes
	if _, err := io.ReadFull(reader, configs.EncryptionKey); err != nil {
		return SSTableConfigs{}, fmt.Errorf("failed to read encryption key: %w", err)
	}

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

func (rd *Reader) findDataBlock(key []byte) (blockHandle, bool) {
	// Binary search over the index entries
	low, high := 0, len(rd.index)-1
	for low <= high {
		mid := (low + high) / 2
		if bytes.Compare(key, rd.index[mid].firstKey) < 0 {
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
	rawBlock := make([]byte, handle.size)
	_, err := rd.r.ReadAt(rawBlock, int64(handle.offset))
	if err != nil {
		return nil, false, err
	}

	// Read block header
	blockHeader := BlockHeader{
		CompressionType: CompressionType(rawBlock[0]),
	}
	copy(blockHeader.InitializationVector[:], rawBlock[1:13])
	copy(blockHeader.AuthenticationTag[:], rawBlock[13:13+aes.BlockSize])

	// Calculate checksum on the block data (excluding header and checksum itself).
	// The checksum is on the *encrypted* data + header.
	checksum := wyhash.Hash(rawBlock[:len(rawBlock)-8], rd.configs.WyhashSeed)
	expectedChecksum := binary.LittleEndian.Uint64(rawBlock[len(rawBlock)-8:])
	if checksum != expectedChecksum {
		return nil, false, fmt.Errorf("data block checksum mismatch")
	}

	// encryptedData starts after the header and ends before the checksum.
	encryptedData := rawBlock[29 : len(rawBlock)-8] // 1 byte for CompressionType + 12 bytes for IV + 16 bytes for AuthTag

	// Decrypt the data
	blockCipher, err := aes.NewCipher(rd.configs.EncryptionKey)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(blockCipher)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create GCM: %w", err)
	}

	// The encryptedData does not contain the tag, it's separate in the header.
	// We need to append the tag to the encryptedData before opening.
	decryptedData, err := gcm.Open(nil, blockHeader.InitializationVector[:], append(encryptedData, blockHeader.AuthenticationTag[:]...), nil)
	if err != nil {
		return nil, false, fmt.Errorf("failed to decrypt block: %w", err)
	}

	var decodedBlock []byte
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

	// Extract restart points and their count from the end of the decoded block
	numRestartPoints := binary.LittleEndian.Uint32(decodedBlock[len(decodedBlock)-4:])
	restartPointsStart := len(decodedBlock) - 4 - int(numRestartPoints)*4

	// Create a reader for the actual key-value data, excluding restart points and their count
	dataReader := bytes.NewReader(decodedBlock[:restartPointsStart])

	// Decode the block and search for the key
	var prevKey []byte
	for dataReader.Len() > 0 {
		kv, err := decodeEntry(dataReader, prevKey)
		if err != nil {
			return nil, false, err
		}

		if bytes.Equal(kv.Key, key) {
			return kv.Value, true, nil
		}
		prevKey = kv.Key
	}

	return nil, false, nil
}

func decodeIndexEntry(r *bytes.Reader) (indexEntry, error) {
	keyLen, err := binary.ReadUvarint(r)
	if err != nil {
		return indexEntry{}, err
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return indexEntry{}, err
	}

	offset, err := binary.ReadUvarint(r)
	if err != nil {
		return indexEntry{}, err
	}

	size, err := binary.ReadUvarint(r)
	if err != nil {
		return indexEntry{}, err
	}

	return indexEntry{firstKey: key, blockHandle: blockHandle{offset: offset, size: size}}, nil
}
