package dsst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
)

const (
	Magic   = 0x53535446 // "SSTF"
	Version = 1
)

// Writer writes SST files.
type Writer struct {
	file                  vfs.File
	options               *Options
	encryptor             *Encryptor
	blockBuilder          *BlockBuilder
	indexBuilder          *BlockBuilder // Stores key -> block_offset
	rangeTombstoneBuilder *RangeTombstoneBlockBuilder
	filter                *BloomFilterPolicy
	keys                  [][]byte // For bloom filter
	offset                int64
	closed                bool
	err                   error

	// Stats
	minKey []byte
	maxKey []byte
	minTS  uint64
	maxTS  uint64
	first  bool
}

// NewWriter creates a new SST writer.
func NewWriter(file vfs.File, opts *Options) (*Writer, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	encryptor, err := NewEncryptor(opts.EncryptionKey)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		file:                  file,
		options:               opts,
		encryptor:             encryptor,
		blockBuilder:          NewBlockBuilder(16), // Restart interval 16
		indexBuilder:          NewBlockBuilder(1),  // Index blocks don't need much prefix compression usually
		rangeTombstoneBuilder: NewRangeTombstoneBlockBuilder(),
		filter:                NewBloomFilterPolicy(opts.BloomBitsPerKey),
		first:                 true,
		minTS:                 ^uint64(0), // Max uint64
		maxTS:                 0,
	}

	if err := w.writeHeader(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *Writer) writeHeader() error {
	// Magic (4) + Version (4) + Encrypted DEK (variable)
	var buf [8]byte
	binary.LittleEndian.PutUint32(buf[0:], Magic)
	binary.LittleEndian.PutUint32(buf[4:], Version)
	if _, err := w.file.Write(buf[:]); err != nil {
		return err
	}
	w.offset += 8

	encryptedDEK, err := w.encryptor.EncryptDEK()
	if err != nil {
		return err
	}

	// Write DEK length + DEK
	var dekLenBuf [4]byte
	binary.LittleEndian.PutUint32(dekLenBuf[:], uint32(len(encryptedDEK)))
	if _, err := w.file.Write(dekLenBuf[:]); err != nil {
		return err
	}
	w.offset += 4

	if _, err := w.file.Write(encryptedDEK); err != nil {
		return err
	}
	w.offset += int64(len(encryptedDEK))

	return nil
}

// Add adds a key-value pair.
func (w *Writer) Add(key, value []byte) error {
	return w.add(key, value, 0) // 0 for Put
}

// AddDeleted adds a deletion tombstone.
func (w *Writer) AddDeleted(key []byte) error {
	return w.add(key, nil, 1) // 1 for Delete
}

// AddRangeTombstone adds a range deletion tombstone [start, end).
func (w *Writer) AddRangeTombstone(start, end []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return errors.New("writer is closed")
	}
	return w.rangeTombstoneBuilder.Add(start, end)
}

func (w *Writer) add(key, value []byte, kind byte) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return errors.New("writer is closed")
	}

	// Add to bloom filter keys
	if w.options.BloomBitsPerKey > 0 {
		w.keys = append(w.keys, append([]byte(nil), key...))
	}

	// Update stats
	if w.first {
		w.minKey = append([]byte(nil), key...)
		w.first = false
	}
	w.maxKey = append(w.maxKey[:0], key...)

	if w.options.KeyTSExtractor != nil {
		ts := w.options.KeyTSExtractor(key)
		if ts < w.minTS {
			w.minTS = ts
		}
		if ts > w.maxTS {
			w.maxTS = ts
		}
	}

	if err := w.blockBuilder.Add(key, value, kind); err != nil {
		w.err = err
		return err
	}

	// Flush block if full
	if w.blockBuilder.CurrentSizeEstimate() >= w.options.BlockSize {
		if err := w.flushBlock(); err != nil {
			w.err = err
			return err
		}
	}
	return nil
}

func (w *Writer) flushBlock() error {
	if w.blockBuilder.IsEmpty() {
		return nil
	}

	// Get last key for index
	lastKey := append([]byte(nil), w.blockBuilder.lastKey...)

	blockData := w.blockBuilder.Finish()

	// Compress (TODO: Snappy)

	// Encrypt
	// We use a new IV for each block. For simplicity, we can use the offset as IV (padded).
	// In production, use a random IV or a counter.
	iv := make([]byte, 16)
	binary.LittleEndian.PutUint64(iv, uint64(w.offset))

	// Write block handle to index: last_key -> offset, size
	// Value in index is varint(offset) + varint(size)
	var handleBuf [binary.MaxVarintLen64 * 2]byte
	n := binary.PutUvarint(handleBuf[0:], uint64(w.offset))
	n += binary.PutUvarint(handleBuf[n:], uint64(len(blockData))) // Encrypted size will be same as plaintext for CTR

	if err := w.indexBuilder.Add(lastKey, handleBuf[:n], 0); err != nil {
		return err
	}

	// Write IV
	if _, err := w.file.Write(iv); err != nil {
		return err
	}
	w.offset += 16

	// Write Encrypted Data
	sw, err := w.encryptor.NewStreamWriter(w.file, iv)
	if err != nil {
		return err
	}
	if _, err := sw.Write(blockData); err != nil {
		return err
	}
	w.offset += int64(len(blockData))

	// Write Checksum (CRC32 of unencrypted data)
	checksum := crc32.ChecksumIEEE(blockData)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], checksum)
	if _, err := w.file.Write(crcBuf[:]); err != nil {
		return err
	}
	w.offset += 4

	w.blockBuilder.Reset()
	return nil
}

// Close finishes writing the SST file.
func (w *Writer) Close() error {
	if w.closed {
		return w.err
	}

	// Flush pending data block
	if err := w.flushBlock(); err != nil {
		w.err = err
		return err
	}

	// Write Filter Block
	var filterOffset, filterSize uint64
	if w.options.BloomBitsPerKey > 0 && len(w.keys) > 0 {
		filterData := w.filter.CreateFilter(w.keys)
		filterOffset = uint64(w.offset)
		filterSize = uint64(len(filterData))

		if _, err := w.file.Write(filterData); err != nil {
			w.err = err
			return err
		}
		w.offset += int64(len(filterData))
	}

	// Write Range Tombstone Block
	var rangeTombstoneOffset, rangeTombstoneSize uint64
	if !w.rangeTombstoneBuilder.IsEmpty() {
		rtData := w.rangeTombstoneBuilder.Finish()
		rangeTombstoneOffset = uint64(w.offset)
		rangeTombstoneSize = uint64(len(rtData))

		// Encrypt Range Tombstones
		iv := make([]byte, 16)
		binary.LittleEndian.PutUint64(iv, uint64(w.offset))
		if _, err := w.file.Write(iv); err != nil {
			w.err = err
			return err
		}
		w.offset += 16

		sw, err := w.encryptor.NewStreamWriter(w.file, iv)
		if err != nil {
			w.err = err
			return err
		}
		if _, err := sw.Write(rtData); err != nil {
			w.err = err
			return err
		}
		w.offset += int64(len(rtData))
	}

	// Write Index Block
	indexOffset := uint64(w.offset)
	indexData := w.indexBuilder.Finish()
	// Index is not encrypted in this simple version, but typically should be.
	// For this task, let's write it plain for simplicity or encrypt it similarly.
	// Let's encrypt it for consistency.
	iv := make([]byte, 16)
	binary.LittleEndian.PutUint64(iv, uint64(w.offset))
	if _, err := w.file.Write(iv); err != nil {
		w.err = err
		return err
	}
	w.offset += 16

	sw, err := w.encryptor.NewStreamWriter(w.file, iv)
	if err != nil {
		w.err = err
		return err
	}
	if _, err := sw.Write(indexData); err != nil {
		w.err = err
		return err
	}
	w.offset += int64(len(indexData))
	indexSize := uint64(len(indexData))

	// Write Properties Block
	// MinKeyLen(varint) + MinKey + MaxKeyLen(varint) + MaxKey + MinTS(8) + MaxTS(8)
	var propsBuf bytes.Buffer

	// MinKey
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(w.minKey)))
	propsBuf.Write(buf[:n])
	propsBuf.Write(w.minKey)

	// MaxKey
	n = binary.PutUvarint(buf[:], uint64(len(w.maxKey)))
	propsBuf.Write(buf[:n])
	propsBuf.Write(w.maxKey)

	// MinTS
	var tsBuf [8]byte
	binary.LittleEndian.PutUint64(tsBuf[:], w.minTS)
	propsBuf.Write(tsBuf[:])

	// MaxTS
	binary.LittleEndian.PutUint64(tsBuf[:], w.maxTS)
	propsBuf.Write(tsBuf[:])

	propsData := propsBuf.Bytes()
	propsOffset := uint64(w.offset)
	propsSize := uint64(len(propsData))

	// Encrypt Properties
	iv = make([]byte, 16)
	binary.LittleEndian.PutUint64(iv, uint64(w.offset))
	if _, err := w.file.Write(iv); err != nil {
		w.err = err
		return err
	}
	w.offset += 16

	sw, err = w.encryptor.NewStreamWriter(w.file, iv)
	if err != nil {
		w.err = err
		return err
	}
	if _, err := sw.Write(propsData); err != nil {
		w.err = err
		return err
	}
	w.offset += int64(len(propsData))

	// Write Footer
	// Index Offset (8), Index Size (8), Filter Offset (8), Filter Size (8), Props Offset (8), Props Size (8), RangeTombstone Offset (8), RangeTombstone Size (8), Magic (4)
	var footer [68]byte
	binary.LittleEndian.PutUint64(footer[0:], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:], indexSize)
	binary.LittleEndian.PutUint64(footer[16:], filterOffset)
	binary.LittleEndian.PutUint64(footer[24:], filterSize)
	binary.LittleEndian.PutUint64(footer[32:], propsOffset)
	binary.LittleEndian.PutUint64(footer[40:], propsSize)
	binary.LittleEndian.PutUint64(footer[48:], rangeTombstoneOffset)
	binary.LittleEndian.PutUint64(footer[56:], rangeTombstoneSize)
	binary.LittleEndian.PutUint32(footer[64:], Magic)

	if _, err := w.file.Write(footer[:]); err != nil {
		w.err = err
		return err
	}

	w.closed = true
	return w.file.Close()
}

// MinKey returns the minimum key written to the SST.
func (w *Writer) MinKey() []byte {
	return w.minKey
}

// MaxKey returns the maximum key written to the SST.
func (w *Writer) MaxKey() []byte {
	return w.maxKey
}

// Size returns the current size of the SST file.
func (w *Writer) Size() uint64 {
	return uint64(w.offset)
}
