package iterator

import (
	"bytes"
)

// Iterator defines the interface for iterating over key-value pairs.
type Iterator interface {
	First()
	Seek(key []byte)

	Valid() bool
	Next()

	Key() []byte
	Value() []byte

	Close()
}

func NewMergeIterator(iters []Iterator) Iterator {
	return newMergeIteratorInternal(iters)
}

func newMergeIteratorInternal(iters []Iterator) Iterator {
	// Filter out nil iterators to prevent panics.
	n := 0
	for _, iter := range iters {
		if iter != nil {
			iters[n] = iter
			n++
		}
	}
	iters = iters[:n]

	if len(iters) == 0 {
		return nil
	}
	if len(iters) == 1 {
		return iters[0]
	}
	return &MergeIterator{iters: iters}
}

// MergeIterator merges multiple sorted iterators into a single sorted iterator.
// Handles duplicate keys by returning only the newest non-deleted value.
type MergeIterator struct {
	iters      []Iterator
	current    Iterator
	currentKey []byte // Current key to avoid duplicates
}

func (mi *MergeIterator) First() {
	// Position all iterators at beginning and find first valid entry
	for _, iter := range mi.iters {
		iter.First()
	}
	mi.findNextValid()
}

func (mi *MergeIterator) Valid() bool {
	return mi.current != nil && mi.current.Valid()
}

func (mi *MergeIterator) Next() {
	if mi.current == nil {
		return
	}

	// Advance all iterators at current key to avoid duplicates
	for _, iter := range mi.iters {
		if iter.Valid() && bytes.Equal(iter.Key(), mi.currentKey) {
			iter.Next()
		}
	}

	mi.findNextValid()
}

func (mi *MergeIterator) Key() []byte {
	if mi.current == nil {
		return nil
	}
	return mi.currentKey
}

func (mi *MergeIterator) Value() []byte {
	if mi.current == nil {
		return nil
	}
	return mi.current.Value()
}

// Seek positions the iterator at the first key >= target.
func (mi *MergeIterator) Seek(key []byte) {
	// Seek all iterators to target position
	for _, iter := range mi.iters {
		iter.Seek(key)
	}
	mi.findNextValid()
}

func (mi *MergeIterator) Close() {
	for _, iter := range mi.iters {
		iter.Close()
	}
}

// findNextValid finds the next non-deleted entry across all iterators.
func (mi *MergeIterator) findNextValid() {
	mi.current = nil
	mi.currentKey = nil

	for {
		var smallestKey []byte
		var candidateIterators []Iterator

		// Find smallest key among all valid iterators
		for _, iter := range mi.iters {
			if !iter.Valid() {
				continue
			}
			if smallestKey == nil || bytes.Compare(iter.Key(), smallestKey) < 0 {
				smallestKey = iter.Key()
				candidateIterators = []Iterator{iter}
			} else if bytes.Equal(iter.Key(), smallestKey) {
				candidateIterators = append(candidateIterators, iter)
			}
		}

		if smallestKey == nil {
			return // No more entries
		}

		// Find first non-deleted entry among candidates
		var bestIterator Iterator
		for _, iter := range candidateIterators {
			if iter.Value() != nil { // Non-tombstone found
				bestIterator = iter
				break
			}
		}

		if bestIterator != nil {
			// Valid entry found
			mi.current = bestIterator
			mi.currentKey = smallestKey
			return
		}

		// All candidates are tombstones - advance and retry
		for _, iter := range candidateIterators {
			iter.Next()
		}
	}
}
