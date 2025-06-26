
package dsst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/wyhash"
)

// Reader allows searching for keys within an SST file.
type Reader struct {
	r      io.ReaderAt
	footer SSTFooter
	index  []indexEntry
	seed   uint64
}

// NewReader initializes a new SST reader.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	footer, err := readFooter(r, size)
	if err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	index, err := readIndexBlock(r, footer.IndexHandle, footer.WyhashSeed)
	if err != nil {
		return nil, fmt.Errorf("failed to read index block: %w", err)
	}

	return &Reader{
		r:      r,
		footer: footer,
		index:  index,
		seed:   footer.WyhashSeed,
	}, nil
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

func readIndexBlock(r io.ReaderAt, handle blockHandle, seed uint64) ([]indexEntry, error) {
	buf := make([]byte, handle.size)
	_, err := r.ReadAt(buf, int64(handle.offset))
	if err != nil {
		return nil, err
	}

	checksum := wyhash.Hash(buf[:len(buf)-8], seed)
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
	buf := make([]byte, handle.size)
	_, err := rd.r.ReadAt(buf, int64(handle.offset))
	if err != nil {
		return nil, false, err
	}

	checksum := wyhash.Hash(buf[:len(buf)-8], rd.seed)
	expectedChecksum := binary.LittleEndian.Uint64(buf[len(buf)-8:])
	if checksum != expectedChecksum {
		return nil, false, fmt.Errorf("data block checksum mismatch")
	}

	// Decode the block and search for the key
	reader := bytes.NewReader(buf[:len(buf)-8])
	var prevKey []byte
	for reader.Len() > 0 {
		// Stop before reading restart points
		if reader.Len() <= 4+4*int(binary.LittleEndian.Uint32(buf[len(buf)-12:len(buf)-8])) {
			break
		}

		kv, err := decodeEntry(reader, prevKey)
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
