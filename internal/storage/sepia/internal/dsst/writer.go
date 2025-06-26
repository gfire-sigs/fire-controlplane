
package dsst

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

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
	wyhashSeed      uint64
	firstKeyInBlock []byte
}

// NewWriter creates a new SST writer.
func NewWriter(w io.Writer) *Writer {
	// For tests, we might want a fixed seed. For production, a random seed is better.
	// We will use a fixed seed for now to make tests deterministic.
	seed := uint64(0)
	return &Writer{
		w:            bufio.NewWriter(w),
		dataBlockBuf: new(bytes.Buffer),
		wyhashSeed:   seed,
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

	if wr.entryCounter%SST_RESTART_POINT_INTERVAL == 0 {
		wr.restartPoints = append(wr.restartPoints, uint32(wr.dataBlockBuf.Len()))
	}

	encodeEntry(wr.dataBlockBuf, wr.prevKey, KVEntry{Key: key, Value: value})
	wr.prevKey = append(wr.prevKey[:0], key...)
	wr.entryCounter++

	if wr.dataBlockBuf.Len() >= SST_MAX_BLOCK_SIZE {
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
	for _, rpOffset := range restartPoints {
		binary.Write(blockBuf, binary.LittleEndian, rpOffset)
	}
	binary.Write(blockBuf, binary.LittleEndian, uint32(len(restartPoints)))

	checksum := wyhash.Hash(blockBuf.Bytes(), wr.wyhashSeed)
	binary.Write(blockBuf, binary.LittleEndian, checksum)

	written, err := wr.w.Write(blockBuf.Bytes())
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

	checksum := wyhash.Hash(buf.Bytes(), wr.wyhashSeed)
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
	buf := new(bytes.Buffer) // Empty metaindex block

	checksum := wyhash.Hash(buf.Bytes(), wr.wyhashSeed)
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
		WyhashSeed:      wr.wyhashSeed,
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
