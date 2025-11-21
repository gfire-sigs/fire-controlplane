package dsst

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// BlockBuilder constructs a data block with prefix compression.
type BlockBuilder struct {
	buffer          bytes.Buffer
	restarts        []uint32
	counter         int
	restartInterval int
	lastKey         []byte
	finished        bool
}

// NewBlockBuilder creates a new BlockBuilder.
// restartInterval is the number of keys between restart points.
func NewBlockBuilder(restartInterval int) *BlockBuilder {
	return &BlockBuilder{
		restartInterval: restartInterval,
	}
}

// Reset resets the builder for reuse.
func (b *BlockBuilder) Reset() {
	b.buffer.Reset()
	b.restarts = b.restarts[:0]
	b.counter = 0
	b.lastKey = nil
	b.finished = false
}

// Add adds a key-value pair to the block.
// Keys must be added in sorted order.
func (b *BlockBuilder) Add(key, value []byte, kind byte) error {
	if b.finished {
		return errors.New("block builder is finished")
	}
	if len(b.lastKey) > 0 && bytes.Compare(key, b.lastKey) <= 0 {
		return errors.New("keys must be added in sorted order")
	}

	shared := 0
	if b.counter < b.restartInterval {
		// See how much we share with the previous key
		minLen := len(b.lastKey)
		if len(key) < minLen {
			minLen = len(key)
		}
		for shared < minLen && b.lastKey[shared] == key[shared] {
			shared++
		}
	} else {
		// Restart point
		b.restarts = append(b.restarts, uint32(b.buffer.Len()))
		b.counter = 0
	}

	nonShared := len(key) - shared

	// Write header: <shared><non_shared><value_len>
	// Using varint for compactness
	var buf [binary.MaxVarintLen64 * 3]byte
	n := binary.PutUvarint(buf[0:], uint64(shared))
	n += binary.PutUvarint(buf[n:], uint64(nonShared))
	n += binary.PutUvarint(buf[n:], uint64(len(value)+1)) // +1 for kind
	b.buffer.Write(buf[:n])

	// Write non-shared key part
	b.buffer.Write(key[shared:])

	// Write kind
	b.buffer.WriteByte(kind)

	// Write value
	b.buffer.Write(value)

	// Update state
	b.lastKey = append(b.lastKey[:0], key...)
	b.counter++
	return nil
}

// Finish finalizes the block and returns the encoded data.
func (b *BlockBuilder) Finish() []byte {
	if !b.finished {
		// Append restarts
		for _, r := range b.restarts {
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], r)
			b.buffer.Write(buf[:])
		}
		// Append number of restarts
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(len(b.restarts)))
		b.buffer.Write(buf[:])
		b.finished = true
	}
	return b.buffer.Bytes()
}

// CurrentSizeEstimate returns an estimate of the current block size.
func (b *BlockBuilder) CurrentSizeEstimate() int {
	return b.buffer.Len() + len(b.restarts)*4 + 4
}

// IsEmpty returns true if the block is empty.
func (b *BlockBuilder) IsEmpty() bool {
	return b.buffer.Len() == 0
}

// BlockIterator iterates over a data block.
type BlockIterator struct {
	data        []byte
	restarts    []uint32
	numRestarts uint32

	// Current state
	currentOffset int
	key           []byte
	value         []byte
	kind          byte
	err           error
}

// NewBlockIterator creates a new BlockIterator for the given block data.
func NewBlockIterator(data []byte) (*BlockIterator, error) {
	if len(data) < 4 {
		return nil, errors.New("block too small")
	}

	numRestarts := binary.LittleEndian.Uint32(data[len(data)-4:])
	if len(data) < 4+int(numRestarts)*4 {
		return nil, errors.New("block too small for restarts")
	}

	restartsOffset := len(data) - 4 - int(numRestarts)*4
	restarts := make([]uint32, numRestarts)
	for i := 0; i < int(numRestarts); i++ {
		restarts[i] = binary.LittleEndian.Uint32(data[restartsOffset+i*4:])
	}

	return &BlockIterator{
		data:        data[:restartsOffset],
		restarts:    restarts,
		numRestarts: numRestarts,
		key:         make([]byte, 0, 256),
	}, nil
}

// Next advances to the next key-value pair.
func (it *BlockIterator) Next() bool {
	if it.currentOffset >= len(it.data) {
		return false
	}

	// Read header
	shared, n1 := binary.Uvarint(it.data[it.currentOffset:])
	if n1 <= 0 {
		it.err = errors.New("corrupted block: invalid shared len")
		return false
	}
	it.currentOffset += n1

	nonShared, n2 := binary.Uvarint(it.data[it.currentOffset:])
	if n2 <= 0 {
		it.err = errors.New("corrupted block: invalid non-shared len")
		return false
	}
	it.currentOffset += n2

	valueLen, n3 := binary.Uvarint(it.data[it.currentOffset:])
	if n3 <= 0 {
		it.err = errors.New("corrupted block: invalid value len")
		return false
	}
	it.currentOffset += n3

	// Reconstruct key
	if uint64(len(it.key)) < shared {
		it.err = errors.New("corrupted block: shared > current key")
		return false
	}
	it.key = it.key[:shared]
	if it.currentOffset+int(nonShared) > len(it.data) {
		it.err = errors.New("corrupted block: key out of bounds")
		return false
	}
	it.key = append(it.key, it.data[it.currentOffset:it.currentOffset+int(nonShared)]...)
	it.currentOffset += int(nonShared)

	// Read kind
	if it.currentOffset >= len(it.data) {
		it.err = errors.New("corrupted block: missing kind")
		return false
	}
	it.kind = it.data[it.currentOffset]
	it.currentOffset++

	// Read value
	valueLen-- // Subtract 1 for kind
	if it.currentOffset+int(valueLen) > len(it.data) {
		it.err = errors.New("corrupted block: value out of bounds")
		return false
	}
	it.value = it.data[it.currentOffset : it.currentOffset+int(valueLen)]
	it.currentOffset += int(valueLen)

	return true
}

// Key returns the current key.
func (it *BlockIterator) Key() []byte {
	return it.key
}

// Value returns the current value.
func (it *BlockIterator) Value() []byte {
	return it.value
}

// Kind returns the kind of the current entry (0 for Put, 1 for Delete).
func (it *BlockIterator) Kind() byte {
	return it.kind
}

// Error returns the last error encountered.
func (it *BlockIterator) Error() error {
	return it.err
}

// SeekToFirst seeks to the first key in the block.
func (it *BlockIterator) SeekToFirst() {
	it.currentOffset = 0
	it.key = it.key[:0]
	it.err = nil
}
