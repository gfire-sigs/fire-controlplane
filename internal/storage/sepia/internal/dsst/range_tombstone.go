package dsst

// RangeTombstone represents a deletion of a key range [Start, End).
type RangeTombstone struct {
	Start []byte
	End   []byte
}

// RangeTombstoneBlockBuilder builds a block of range tombstones.
// Since range tombstones are usually few, we can use a simpler encoding or the same BlockBuilder.
// Let's use BlockBuilder where Key=Start, Value=End. Kind is implied or we can use a specific kind.
// However, BlockBuilder does prefix compression which is good.
// We will use a separate BlockBuilder for range tombstones.
type RangeTombstoneBlockBuilder struct {
	builder *BlockBuilder
}

func NewRangeTombstoneBlockBuilder() *RangeTombstoneBlockBuilder {
	return &RangeTombstoneBlockBuilder{
		builder: NewBlockBuilder(16),
	}
}

func (b *RangeTombstoneBlockBuilder) Add(start, end []byte) error {
	// We store Start as Key, End as Value. Kind is 0 (irrelevant for this block type).
	return b.builder.Add(start, end, 0)
}

func (b *RangeTombstoneBlockBuilder) Finish() []byte {
	return b.builder.Finish()
}

func (b *RangeTombstoneBlockBuilder) IsEmpty() bool {
	return b.builder.IsEmpty()
}

// RangeTombstoneIterator iterates over range tombstones.
type RangeTombstoneIterator struct {
	iter *BlockIterator
}

func NewRangeTombstoneIterator(data []byte) (*RangeTombstoneIterator, error) {
	iter, err := NewBlockIterator(data)
	if err != nil {
		return nil, err
	}
	return &RangeTombstoneIterator{iter: iter}, nil
}

func (it *RangeTombstoneIterator) Next() bool {
	return it.iter.Next()
}

func (it *RangeTombstoneIterator) Tombstone() RangeTombstone {
	return RangeTombstone{
		Start: it.iter.Key(),
		End:   it.iter.Value(),
	}
}

func (it *RangeTombstoneIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}
