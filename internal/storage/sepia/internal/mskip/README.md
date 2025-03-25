# Memory-Optimized Skip List Implementation

## Overview

Package mskip implements a memory-optimized skip list data structure using a custom arena allocator. It provides O(log n) average time complexity for insertions and searches while maintaining efficient memory usage through arena-based allocation.

## Features

- Memory-efficient skip list implementation using arena allocation
- O(log n) average time complexity for insertions and searches
- Bidirectional iteration support
- Configurable maximum level (default: 24)
- Thread-safe reference counting
- Key-value storage as byte slices for flexibility
- Custom key comparison function support
- Memory-efficient node allocation based on actual level size

## Implementation Details

### Memory Layout

The skip list nodes are carefully designed for optimal memory usage:

```
key_ptr: uint64      // Pointer to key bytes in arena
value_ptr: uint64    // Pointer to value bytes in arena
level: int32        // Current level of the node (1 to MSKIP_MAX_LEVEL)
reserved: 4 bytes   // Padding for alignment
nexts: uint32[level] // Array of pointers to next nodes at each level
```

### Node Level Distribution

Node levels follow a probabilistic distribution where:
- Level 1 has probability 1
- Level 2 has probability 1/2
- Level 3 has probability 1/4
And so on, ensuring balanced performance characteristics.

## Usage Example

```go
// Initialize arena with 1MB capacity
arena := marena.NewArena(1 << 20)

// Create skiplist with bytes.Compare as comparison function
skl, err := NewSkipList(arena, bytes.Compare, 42)
if err != nil {
    return err
}

// Insert key-value pairs
skl.Insert([]byte("key1"), []byte("value1"))

// Iterate through elements
iter := skl.Iterator()
defer iter.Close()

for iter.First(); iter.Valid(); iter.Next() {
    key := iter.Key()
    value := iter.Value()
    // Process key-value pair
}

// Seek to specific positions
iter.SeekLE([]byte("key1")) // Seek less than or equal
iter.SeekLT([]byte("key2")) // Seek less than
```

## Performance Characteristics

- Search Operations: O(log n) average time complexity
- Insert Operations: O(log n) average time complexity
- Memory Usage: Optimized through:
  - Arena-based allocation
  - Level-based node sizing
  - Pointer compression (32-bit arena offsets)
  - Memory pooling for iterators

## Internal Use Only

This package is designed for internal use within the Sepia storage engine. It should not be used directly by external packages.