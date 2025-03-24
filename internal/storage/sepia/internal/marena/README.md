# Marena: Memory Arena Package

The `marena` package provides an efficient memory arena implementation for managing memory allocations in a controlled and thread-safe manner. It is designed to handle fixed-size memory blocks with alignment and atomic operations.

## Features

- **Memory Arena Initialization**: Create a memory arena with a buffer aligned to a fixed page size (`ARENA_PAGESIZE`).
- **Efficient Allocation**: Allocate memory blocks with alignment and return 64-bit addresses containing offset and size.
- **Multiple Allocations**: Allocate multiple memory blocks in a single operation.
- **Buffer View**: Access a slice of the arena's buffer corresponding to a given address.
- **Thread-Safe Operations**: Use atomic operations to ensure thread safety.
- **Reset and Remaining Space**: Reset the arena to reuse memory and check remaining space.

## Constants

- `ARENA_MAX_ALLOC_SIZE`: Maximum allocatable size (`2^31 - 1` bytes).
- `ARENA_PAGESIZE`: Fixed page size for alignment (`2^16` bytes).
- `ARENA_INVALID_ADDRESS`: Represents an invalid address (`0`).
- `ARENA_MIN_ADDRESS`: Minimal valid arena address (`128`).

## Usage

### Creating a New Arena

```go
import "pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"

a := marena.NewArena(1024) // Create a new arena with 1024 bytes
```

### Allocating Memory

```go
addr := a.Allocate(100) // Allocate 100 bytes
if addr == marena.ARENA_INVALID_ADDRESS {
    log.Fatal("Allocation failed")
}
```

### Accessing Buffer View

```go
view := a.View(addr) // Get a slice of the buffer for the allocated address
if view == nil {
    log.Fatal("Invalid address range")
}
```

### Resetting the Arena

```go
a.Reset() // Reset the arena to reuse memory
```

### Checking Remaining Space

```go
remaining := a.Remaining() // Get the number of bytes remaining in the arena
```

### Allocating Multiple Blocks

```go
var addr1, addr2 uint64 = 50, 200
ok := a.AllocateMultiple(&addr1, &addr2)
if !ok {
    log.Fatal("Multiple allocation failed")
}
```

## Testing

The package includes comprehensive tests to verify its functionality. Key test cases include:

- **Arena Initialization**: Ensures a new arena is created successfully.
- **Single and Multiple Allocations**: Verifies allocation of memory blocks and multiple blocks in one operation.
- **Buffer View**: Checks that the buffer view matches the allocated size and modifications persist.
- **Reset and Remaining Space**: Tests resetting the arena and calculating remaining space.
- **Failure Scenarios**: Validates behavior when allocations exceed maximum size or available space.

Run the tests using the following command:

```bash
go test ./internal/storage/sepia/internal/marena
```

## License

This package is licensed under the [MIT License](../../../LICENSE).