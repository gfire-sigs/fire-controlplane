package dsst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/vfs"
)

// Reader reads SST files.
type Reader struct {
	file      vfs.File
	options   *Options
	encryptor *Encryptor

	// Footer info
	indexOffset  uint64
	indexSize    uint64
	filterOffset uint64
	filterSize   uint64
	propsOffset  uint64
	propsSize    uint64
	rtOffset     uint64
	rtSize       uint64

	// Loaded meta
	indexBlock *BlockIterator
	filter     *BloomFilterPolicy
	filterData []byte

	// Properties
	MinKey []byte
	MaxKey []byte
	MinTS  uint64
	MaxTS  uint64
}

// NewReader opens an SST file for reading.
func NewReader(file vfs.File, opts *Options) (*Reader, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	// Read Header
	// Magic (4) + Version (4) + DEK Len (4) + Encrypted DEK (variable)
	header := make([]byte, 12)
	if _, err := file.ReadAt(header, 0); err != nil {
		return nil, err
	}

	magic := binary.LittleEndian.Uint32(header[0:])
	if magic != Magic {
		return nil, errors.New("invalid magic number")
	}
	version := binary.LittleEndian.Uint32(header[4:])
	if version != Version {
		return nil, errors.New("unsupported version")
	}
	dekLen := binary.LittleEndian.Uint32(header[8:])

	encryptedDEK := make([]byte, dekLen)
	if _, err := file.ReadAt(encryptedDEK, 12); err != nil {
		return nil, err
	}

	// Decrypt DEK
	dek, err := DecryptDEK(opts.EncryptionKey, encryptedDEK)
	if err != nil {
		return nil, err
	}

	encryptor, err := NewEncryptorWithDEK(opts.EncryptionKey, dek)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		file:      file,
		options:   opts,
		encryptor: encryptor,
	}

	if err := r.readFooter(); err != nil {
		return nil, err
	}

	if err := r.loadIndex(); err != nil {
		return nil, err
	}

	if err := r.loadFilter(); err != nil {
		return nil, err
	}

	if err := r.loadProperties(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Reader) readFooter() error {
	stat, err := r.file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	if fileSize < 68 {
		return errors.New("file too small")
	}

	footer := make([]byte, 68)
	if _, err := r.file.ReadAt(footer, fileSize-68); err != nil {
		return err
	}

	magic := binary.LittleEndian.Uint32(footer[64:])
	if magic != Magic {
		return errors.New("invalid footer magic")
	}

	r.indexOffset = binary.LittleEndian.Uint64(footer[0:])
	r.indexSize = binary.LittleEndian.Uint64(footer[8:])
	r.filterOffset = binary.LittleEndian.Uint64(footer[16:])
	r.filterSize = binary.LittleEndian.Uint64(footer[24:])
	r.propsOffset = binary.LittleEndian.Uint64(footer[32:])
	r.propsSize = binary.LittleEndian.Uint64(footer[40:])
	r.rtOffset = binary.LittleEndian.Uint64(footer[48:])
	r.rtSize = binary.LittleEndian.Uint64(footer[56:])

	return nil
}

func (r *Reader) loadProperties() error {
	if r.propsSize == 0 {
		return nil
	}

	// Properties are encrypted
	iv := make([]byte, 16)
	if _, err := r.file.ReadAt(iv, int64(r.propsOffset)); err != nil {
		return err
	}

	encryptedProps := make([]byte, r.propsSize)
	if _, err := r.file.ReadAt(encryptedProps, int64(r.propsOffset)+16); err != nil {
		return err
	}

	sr, err := r.encryptor.NewStreamReader(bytes.NewReader(encryptedProps), iv)
	if err != nil {
		return err
	}
	propsData, err := io.ReadAll(sr)
	if err != nil {
		return err
	}

	// Parse Properties
	// MinKeyLen(varint) + MinKey + MaxKeyLen(varint) + MaxKey + MinTS(8) + MaxTS(8)
	buf := bytes.NewReader(propsData)

	minKeyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return err
	}
	r.MinKey = make([]byte, minKeyLen)
	if _, err := io.ReadFull(buf, r.MinKey); err != nil {
		return err
	}

	maxKeyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return err
	}
	r.MaxKey = make([]byte, maxKeyLen)
	if _, err := io.ReadFull(buf, r.MaxKey); err != nil {
		return err
	}

	var tsBuf [8]byte
	if _, err := io.ReadFull(buf, tsBuf[:]); err != nil {
		return err
	}
	r.MinTS = binary.LittleEndian.Uint64(tsBuf[:])

	if _, err := io.ReadFull(buf, tsBuf[:]); err != nil {
		return err
	}
	r.MaxTS = binary.LittleEndian.Uint64(tsBuf[:])

	return nil
}

func (r *Reader) loadIndex() error {
	// Index is encrypted.
	// IV is at indexOffset
	iv := make([]byte, 16)
	if _, err := r.file.ReadAt(iv, int64(r.indexOffset)); err != nil {
		return err
	}

	encryptedIndex := make([]byte, r.indexSize)
	if _, err := r.file.ReadAt(encryptedIndex, int64(r.indexOffset)+16); err != nil {
		return err
	}

	// Decrypt
	sr, err := r.encryptor.NewStreamReader(bytes.NewReader(encryptedIndex), iv)
	if err != nil {
		return err
	}
	indexData, err := io.ReadAll(sr)
	if err != nil {
		return err
	}

	r.indexBlock, err = NewBlockIterator(indexData)
	return err
}

func (r *Reader) loadFilter() error {
	if r.filterSize == 0 {
		return nil
	}

	r.filterData = make([]byte, r.filterSize)
	if _, err := r.file.ReadAt(r.filterData, int64(r.filterOffset)); err != nil {
		return err
	}

	r.filter = NewBloomFilterPolicy(r.options.BloomBitsPerKey)
	return nil
}

// NewIterator creates a new iterator for the SST file.
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{
		reader:    r,
		indexIter: r.indexBlock, // Use the loaded index block iterator
	}
}

// NewRangeTombstoneIterator creates a new iterator for range tombstones.
func (r *Reader) NewRangeTombstoneIterator() (*RangeTombstoneIterator, error) {
	if r.rtSize == 0 {
		return nil, nil
	}

	// Range Tombstones are encrypted
	iv := make([]byte, 16)
	if _, err := r.file.ReadAt(iv, int64(r.rtOffset)); err != nil {
		return nil, err
	}

	encryptedRT := make([]byte, r.rtSize)
	if _, err := r.file.ReadAt(encryptedRT, int64(r.rtOffset)+16); err != nil {
		return nil, err
	}

	sr, err := r.encryptor.NewStreamReader(bytes.NewReader(encryptedRT), iv)
	if err != nil {
		return nil, err
	}
	rtData, err := io.ReadAll(sr)
	if err != nil {
		return nil, err
	}

	return NewRangeTombstoneIterator(rtData)
}

// Iterator iterates over the SST file.
type Iterator struct {
	reader    *Reader
	indexIter *BlockIterator
	dataIter  *BlockIterator
	err       error
	valid     bool
}

// Seek seeks to the first key >= target.
func (it *Iterator) Seek(target []byte) {
	// Binary search in index
	// Since BlockIterator doesn't support binary search yet (it's linear scan for now in this simple impl),
	// we will just scan the index. In a real impl, we'd binary search the restarts.
	// For this task, let's assume linear scan of index is "okay" or we improve BlockIterator later.
	// Actually, let's implement a basic seek in BlockIterator or just scan here.

	it.indexIter.SeekToFirst()
	for it.indexIter.Next() {
		// Key in index is the LAST key in the block.
		// If target <= last_key, then the target might be in this block.
		if bytes.Compare(target, it.indexIter.Key()) <= 0 {
			// Found the block
			it.loadBlock(it.indexIter.Value())
			if it.err != nil {
				it.valid = false
				return
			}
			// Seek in data block
			// Again, linear scan for now.
			it.dataIter.SeekToFirst()
			for it.dataIter.Next() {
				if bytes.Compare(it.dataIter.Key(), target) >= 0 {
					it.valid = true
					return
				}
			}
			// Not found in this block, but might be in next?
			// Since keys are sorted, if we are past the target in this block, it's not here.
			// But wait, we seeked to the first block where last_key >= target.
			// So if it's not in this block, it must be greater than all keys in this block?
			// No, if we are here, it means target <= last_key.
			// If we scanned the whole block and didn't find >= target, something is wrong or logic error.
			// Actually, if target > all keys in block, then target > last_key, which contradicts the index check.
			// So it MUST be in this block if it exists.
			it.valid = false
			return
		}
	}
	it.valid = false
}

func (it *Iterator) loadBlock(handle []byte) {
	// Decode handle: offset (varint), size (varint)
	offset, n1 := binary.Uvarint(handle)
	size, n2 := binary.Uvarint(handle[n1:])
	_ = n2 // unused

	// Read IV
	iv := make([]byte, 16)
	if _, err := it.reader.file.ReadAt(iv, int64(offset)); err != nil {
		it.err = err
		return
	}

	// Read Encrypted Block
	encryptedData := make([]byte, size)
	if _, err := it.reader.file.ReadAt(encryptedData, int64(offset)+16); err != nil {
		it.err = err
		return
	}

	// Read Checksum
	var crcBuf [4]byte
	if _, err := it.reader.file.ReadAt(crcBuf[:], int64(offset)+16+int64(size)); err != nil {
		it.err = err
		return
	}
	expectedCRC := binary.LittleEndian.Uint32(crcBuf[:])

	// Decrypt
	sr, err := it.reader.encryptor.NewStreamReader(bytes.NewReader(encryptedData), iv)
	if err != nil {
		it.err = err
		return
	}
	data, err := io.ReadAll(sr)
	if err != nil {
		it.err = err
		return
	}

	// Verify Checksum
	if crc32.ChecksumIEEE(data) != expectedCRC {
		it.err = errors.New("block checksum mismatch")
		return
	}

	it.dataIter, it.err = NewBlockIterator(data)
}

// First moves to the first key.
func (it *Iterator) First() {
	it.indexIter.SeekToFirst()
	if it.indexIter.Next() {
		it.loadBlock(it.indexIter.Value())
		if it.err != nil {
			it.valid = false
			return
		}
		it.dataIter.SeekToFirst()
		if it.dataIter.Next() {
			it.valid = true
			return
		}
	}
	it.valid = false
}

// Next advances to the next key.
func (it *Iterator) Next() {
	if !it.valid {
		return
	}
	if it.dataIter.Next() {
		return
	}
	// End of block, move to next block
	if it.indexIter.Next() {
		it.loadBlock(it.indexIter.Value())
		if it.err != nil {
			it.valid = false
			return
		}
		it.dataIter.SeekToFirst()
		if it.dataIter.Next() {
			return
		}
	}
	it.valid = false
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	return it.dataIter.Key()
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	return it.dataIter.Value()
}

// Kind returns the kind of the current entry.
func (it *Iterator) Kind() byte {
	return it.dataIter.Kind()
}

// Valid returns true if the iterator is valid.
func (it *Iterator) Valid() bool {
	return it.valid
}

// Close closes the iterator.
func (it *Iterator) Close() {
	it.err = nil
	// Nothing to close really for block iterator, but if we had open resources...
}
