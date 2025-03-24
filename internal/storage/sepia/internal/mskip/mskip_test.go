package mskip

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"
)

func TestSkipListInsert(t *testing.T) {
	arena := marena.NewArena(1 << 20)

	skl, err := NewSkipList(arena, bytes.Compare, 42)
	if err != nil {
		t.Fatal(err)
	}

	key0 := []byte("key0")
	if skl.seeklt(key0, nil) != skl.head {
		t.Fatalf("expected head node")
	}

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

	if string(skl.arena.View(skl.getNode(newnode).keyPtr)) != "key1" {
		t.Fatalf("expected key1")
	}

	if string(skl.arena.View(skl.getNode(newnode).valuePtr)) != "value1" {
		t.Fatalf("expected value1")
	}

	key2 := []byte("key2")
	value2 := []byte("value2")

	before = skl.seeklt(key2, &log)
	if before != newnode {
		t.Fatalf("expected newnode = %d, got %d", newnode, before)
	}

	newnode = skl.insertNext(&log, key2, value2)
	if newnode == 0 {
		t.Fatalf("insertNext failed")
	}

	key3 := []byte("key3")
	value3 := []byte("value3")

	before = skl.seeklt(key3, &log)
	if before != newnode {
		t.Fatalf("expected newnode = %d, got %d", newnode, before)
	}

	newnode = skl.insertNext(&log, key3, value3)
	if newnode == 0 {
		t.Fatalf("insertNext failed")
	}
}

func BenchmarkSkipList(b *testing.B) {
	// Initialize arena with 64MB capacity
	arena := marena.NewArena(64 << 20)

	// Create skiplist
	skl, err := NewSkipList(arena, bytes.Compare, 42)
	if err != nil {
		b.Fatal(err)
	}

	// Generate 10000 sorted keys
	var keys [][]byte
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		keys = append(keys, key)
	}

	// Insert all keys with values
	var log [MSKIP_MAX_LEVEL]uint32
	for i, key := range keys {
		value := []byte(fmt.Sprintf("value%08d", i))
		skl.seeklt(key, &log)
		if skl.insertNext(&log, key, value) == 0 {
			b.Fatal("insert failed")
		}
	}

	// Benchmark seek operations
	b.ResetTimer()
	rng := rand.New(rand.NewPCG(42, 24))
	for i := 0; i < b.N; i++ {
		// Pick a random key from our list
		idx := rng.Int() % len(keys)
		key := keys[idx]

		// Find the key
		node := skl.seeklt(key, &log)
		next := skl.getNode(node).nexts[0]
		nextValue := skl.arena.View(skl.getNode(next).keyPtr)
		if bytes.Compare(key, nextValue) != 0 {
			b.Fatalf("expected key %s, got %s", key, nextValue)
		}
	}
}
