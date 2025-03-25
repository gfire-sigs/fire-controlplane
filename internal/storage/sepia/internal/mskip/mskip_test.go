// Package mskip provides tests for the memory-optimized skiplist implementation.
package mskip

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"
)

// TestSkipListInsert verifies the basic operations of the skiplist:
// 1. Creation and initialization of an empty skiplist
// 2. Searching in an empty skiplist returns the head node
// 3. Inserting a new key-value pair
// 4. Verifying the inserted key and value
// 5. Sequential insertion of multiple key-value pairs
// 6. Proper linking of nodes in the skiplist
func TestSkipListInsert(t *testing.T) {
	// Initialize arena with 1MB capacity
	arena := marena.NewArena(1 << 20)

	// Create skiplist with bytes.Compare as the comparison function
	skl, err := NewSkipList(arena, bytes.Compare, 42)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Search in empty skiplist
	key0 := []byte("key0")
	if skl.seeklt(key0, nil) != skl.head {
		t.Fatalf("expected head node")
	}

	// Test 2: Insert first key-value pair
	key1 := []byte("key1")
	value1 := []byte("value1")

	var log [MSKIP_MAX_LEVEL]uint32
	before := skl.seeklt(key1, &log)
	if before != skl.head {
		t.Fatalf("expected head node, got %d", before)
	}

	newnode := skl.insertNext(&log, key1, value1)
	if newnode == 0 {
		t.Fatalf("insertNext failed")
	}

	// Verify key1 and value1 were properly stored
	if string(skl.arena.View(skl.getNode(newnode).keyPtr)) != "key1" {
		t.Fatalf("expected key1")
	}

	if string(skl.arena.View(skl.getNode(newnode).valuePtr)) != "value1" {
		t.Fatalf("expected value1")
	}

	// Test 3: Insert second key-value pair
	key2 := []byte("key2")
	value2 := []byte("value2")

	// Verify search finds the correct insertion point
	before = skl.seeklt(key2, &log)
	if before != newnode {
		t.Fatalf("expected newnode = %d, got %d", newnode, before)
	}

	newnode = skl.insertNext(&log, key2, value2)
	if newnode == 0 {
		t.Fatalf("insertNext failed")
	}

	// Test 4: Insert third key-value pair
	key3 := []byte("key3")
	value3 := []byte("value3")

	// Verify search finds the correct insertion point
	before = skl.seeklt(key3, &log)
	if before != newnode {
		t.Fatalf("expected newnode = %d, got %d", newnode, before)
	}

	newnode = skl.insertNext(&log, key3, value3)
	if newnode == 0 {
		t.Fatalf("insertNext failed")
	}

	// Test 5: Value overwrite for existing key
	value1_updated := []byte("value1_updated")
	before = skl.seeklt(key1, &log)
	newnode = skl.insertNext(&log, key1, value1_updated)
	if newnode == 0 {
		t.Fatalf("value overwrite failed")
	}

	// Verify the value was updated
	if string(skl.arena.View(skl.getNode(newnode).valuePtr)) != "value1_updated" {
		t.Fatalf("expected value1_updated, got %s", string(skl.arena.View(skl.getNode(newnode).valuePtr)))
	}

	// Verify the key remains unchanged
	if string(skl.arena.View(skl.getNode(newnode).keyPtr)) != "key1" {
		t.Fatalf("expected key1, got %s", string(skl.arena.View(skl.getNode(newnode).keyPtr)))
	}
}

// TestSkipListIterator verifies the iterator operations of the skiplist:
// 1. Forward iteration through sorted elements
// 2. Seeking to specific positions (Less than, Less than or Equal)
// 3. Backward iteration from a given position
func TestSkipListIterator(t *testing.T) {
	// Initialize arena with 1MB capacity
	arena := marena.NewArena(1 << 20)

	// Create skiplist with string comparison
	compareStrings := func(a, b []byte) int {
		return strings.Compare(string(a), string(b))
	}
	skl, err := NewSkipList(arena, compareStrings, 12345)
	if err != nil {
		t.Fatal(err)
	}

	// Test data
	data := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "red",
		"date":   "brown",
		"fig":    "purple",
	}

	// Insert test data
	for k, v := range data {
		if ok := skl.Insert([]byte(k), []byte(v)); !ok {
			t.Fatalf("failed to insert key %s", k)
		}
	}

	// Test forward iteration
	iter := skl.Iterator()
	defer iter.Close()

	expected := []string{"apple", "banana", "cherry", "date", "fig"}
	i := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if i >= len(expected) {
			t.Fatal("iterator exceeded expected number of elements")
		}
		if string(iter.Key()) != expected[i] {
			t.Errorf("forward iteration: expected key %s, got %s", expected[i], string(iter.Key()))
		}
		if string(iter.Value()) != data[expected[i]] {
			t.Errorf("forward iteration: expected value %s, got %s", data[expected[i]], string(iter.Value()))
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("forward iteration: expected %d elements, got %d", len(expected), i)
	}

	// Test SeekLE (Less than or Equal)
	iter.SeekLE([]byte("cherry"))
	if !iter.Valid() {
		t.Fatal("SeekLE: iterator should be valid")
	}
	if string(iter.Key()) != "cherry" {
		t.Errorf("SeekLE: expected key 'cherry', got %s", string(iter.Key()))
	}
	if string(iter.Value()) != "red" {
		t.Errorf("SeekLE: expected value 'red', got %s", string(iter.Value()))
	}

	// Test SeekLT (Less Than)
	iter.SeekLT([]byte("cherry"))
	if !iter.Valid() {
		t.Fatal("SeekLT: iterator should be valid")
	}
	if string(iter.Key()) != "banana" {
		t.Errorf("SeekLT: expected key 'banana', got %s", string(iter.Key()))
	}
	if string(iter.Value()) != "yellow" {
		t.Errorf("SeekLT: expected value 'yellow', got %s", string(iter.Value()))
	}

	// Test backward iteration from 'fig'
	iter.SeekLE([]byte("fig"))
	expectedReverse := []string{"fig", "date", "cherry", "banana", "apple"}
	i = 0
	for ; iter.Valid(); iter.Prev() {
		if i >= len(expectedReverse) {
			t.Fatal("backward iteration: exceeded expected number of elements")
		}
		if string(iter.Key()) != expectedReverse[i] {
			t.Errorf("backward iteration: expected key %s, got %s", expectedReverse[i], string(iter.Key()))
		}
		if string(iter.Value()) != data[expectedReverse[i]] {
			t.Errorf("backward iteration: expected value %s, got %s", data[expectedReverse[i]], string(iter.Value()))
		}
		i++
	}
	if i != len(expectedReverse) {
		t.Errorf("backward iteration: expected %d elements, got %d", len(expectedReverse), i)
	}
}

// BenchmarkSkipList measures the performance of skiplist operations.
// The benchmark:
// 1. Creates a skiplist with 100MB arena capacity
// 2. Inserts 1,000,000 sorted key-value pairs
// 3. Performs random lookups on the inserted keys
//
// This helps evaluate:
// - Memory allocation efficiency
// - Search performance with random access patterns
// - Skiplist level distribution effects
func BenchmarkSkipList(b *testing.B) {
	// Initialize arena with 100MB capacity
	arena := marena.NewArena(100 << 20)

	// Create skiplist with deterministic seed for reproducible results
	skl, err := NewSkipList(arena, bytes.Compare, 42)
	if err != nil {
		b.Fatal(err)
	}

	// Generate 1,000,000 sorted keys for consistent test data
	var keys [][]byte
	for i := 0; i < 1_000_000; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		keys = append(keys, key)
	}

	// Insert all keys with corresponding values
	var log [MSKIP_MAX_LEVEL]uint32
	for i, key := range keys {
		value := []byte(fmt.Sprintf("value%08d", i))
		skl.seeklt(key, &log)
		if skl.insertNext(&log, key, value) == 0 {
			b.Fatal("insert failed")
		}
	}

	// Create RNG with fixed seed for reproducible random key selection
	rng := rand.New(rand.NewPCG(42, 24))

	// Reset timer before starting the actual benchmark
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Pick a random key from our list to simulate random access pattern
		idx := rng.Int() % len(keys)
		key := keys[idx]

		// Find the key and verify it exists at the expected location
		node := skl.seeklt(key, &log)
		next := skl.getNode(node).nexts[0]
		nextValue := skl.arena.View(skl.getNode(next).keyPtr)
		if bytes.Compare(key, nextValue) != 0 {
			b.Fatalf("expected key %s, got %s", key, nextValue)
		}
	}
}

// BenchmarkIteratorScan measures the performance of sequential scanning through
// the skiplist using an iterator. The benchmark:
// 1. Creates a skiplist with 100MB arena capacity
// 2. Inserts 1,000,000 sorted key-value pairs (same as BenchmarkSkipList)
// 3. Performs sequential scans through all elements
//
// This helps evaluate:
// - Iterator initialization overhead
// - Sequential access performance
// - Memory access patterns for in-order traversal
func BenchmarkIteratorScan(b *testing.B) {
	// Initialize arena with 100MB capacity
	arena := marena.NewArena(100 << 20)

	// Create skiplist with deterministic seed for reproducible results
	skl, err := NewSkipList(arena, bytes.Compare, 42)
	if err != nil {
		b.Fatal(err)
	}

	// Generate 1,000,000 sorted keys for consistent test data
	var keys [][]byte
	for i := 0; i < 1_000_000; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		keys = append(keys, key)
	}

	// Insert all keys with corresponding values
	var log [MSKIP_MAX_LEVEL]uint32
	for i, key := range keys {
		value := []byte(fmt.Sprintf("value%08d", i))
		skl.seeklt(key, &log)
		if skl.insertNext(&log, key, value) == 0 {
			b.Fatal("insert failed")
		}
	}

	b.SetBytes(int64(len(keys)))

	// Reset timer before starting the actual benchmark
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter := skl.Iterator()
		// Scan through all elements
		var count int
		for iter.First(); iter.Valid(); iter.Next() {
			count++
		}
		iter.Close()
		if count != len(keys) {
			b.Fatalf("expected %d elements, got %d", len(keys), count)
		}
	}
}
