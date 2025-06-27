package iterator

import (
	"bytes"
)

// Iterator defines the interface for iterating over key-value pairs.
type Iterator interface {
	First()
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Seek(key []byte)
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

// MergeIterator merges multiple iterators into a single, sorted iterator.
type MergeIterator struct {
	iters   []Iterator
	current Iterator
	// The key that the iterator is currently positioned at.
	// Used to ensure that Next() advances to a key strictly greater than the current key.
	// This is important for merge iterators to avoid returning duplicate keys
	// when multiple underlying iterators have the same key.
	currentKey []byte
}

func (mi *MergeIterator) First() {
	for _, iter := range mi.iters {
		iter.First()
	}
	mi.findNextValid() // Renamed from findSmallest
}

func (mi *MergeIterator) Valid() bool {
	return mi.current != nil && mi.current.Valid()
}

func (mi *MergeIterator) Next() {
	if mi.current == nil {
		return
	}

	// Advance all iterators that are currently at mi.currentKey.
	// This ensures that we don't get stuck on the same key if multiple iterators
	// have it, and that we correctly move to the next unique key.
	for _, iter := range mi.iters {
		if iter.Valid() && bytes.Equal(iter.Key(), mi.currentKey) {
			iter.Next()
		}
	}

	mi.findNextValid() // Renamed from findSmallest
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

// Seek moves the iterator to the first key that is greater than or equal to the given key.
func (mi *MergeIterator) Seek(key []byte) {
	for _, iter := range mi.iters {
		iter.Seek(key)
	}
	mi.findNextValid() // Renamed from findSmallest
}

func (mi *MergeIterator) Close() {
	for _, iter := range mi.iters {
		iter.Close()
	}
}

// findNextValid finds the next valid (non-tombstone) key-value pair.
func (mi *MergeIterator) findNextValid() {
	mi.current = nil
	mi.currentKey = nil

	for {
		var smallestKey []byte
		var candidateIterators []Iterator

		// Find the smallest key among all valid iterators
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
			// No more valid entries
			return
		}

		// Now, from the candidate iterators (all having smallestKey),
		// find the one that is NOT a tombstone, prioritizing newer iterators.
		var bestIterator Iterator
		for _, iter := range candidateIterators {
			if iter.Value() != nil { // Found a non-tombstone
				bestIterator = iter
				break // Found the newest non-tombstone, so we can stop
			}
		}

		if bestIterator != nil {
			// Found a valid (non-tombstone) entry for smallestKey
			mi.current = bestIterator
			mi.currentKey = smallestKey
			return
		} else {
			// All iterators for smallestKey are tombstones.
			// Advance all of them and try again.
			for _, iter := range candidateIterators {
				iter.Next()
			}
		}
	}
}
