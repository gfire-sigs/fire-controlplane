package marena_test

import (
	"testing"

	"pkg.gfire.dev/controlplane/internal/storage/sepia/internal/marena"
)

// TestNewArena verifies that a new arena is created.
func TestNewArena(t *testing.T) {
	a := marena.NewArena(1024)
	if a == nil {
		t.Fatal("NewArena returned nil")
	}
	// Since the arena size is unexported, we only verify that allocation works.
	addr := a.Allocate(100)
	if addr == marena.ARENA_INVALID_ADDRESS {
		t.Error("Allocation failed in new arena")
	}
}

// TestAllocate checks the Allocate function for a valid allocation.
func TestAllocate(t *testing.T) {
	a := marena.NewArena(65536)
	addr := a.Allocate(100)
	if addr == marena.ARENA_INVALID_ADDRESS {
		t.Fatal("Allocation returned invalid address")
	}
	gotSize := marena.Size(addr)
	wantSize := 100
	if gotSize != wantSize {
		t.Errorf("Size(addr) = %d, want %d", gotSize, wantSize)
	}
	offset := marena.Offset(addr)
	if offset < marena.ARENA_MIN_ADDRESS {
		t.Errorf("Offset(addr) = %d, want >= %d", offset, marena.ARENA_MIN_ADDRESS)
	}
}

// TestAllocateMultiple verifies that multiple allocations can succeed in one operation.
func TestAllocateMultiple(t *testing.T) {
	a := marena.NewArena(65536)
	var addr1, addr2, addr3 uint64 = 50, 200, 300
	ok := a.AllocateMultiple(&addr1, &addr2, &addr3)
	if !ok {
		t.Fatal("AllocateMultiple failed")
	}
	if marena.Size(addr1) != 50 {
		t.Errorf("addr1 size = %d, want 50", marena.Size(addr1))
	}
	if marena.Size(addr2) != 200 {
		t.Errorf("addr2 size = %d, want 200", marena.Size(addr2))
	}
	if marena.Size(addr3) != 300 {
		t.Errorf("addr3 size = %d, want 300", marena.Size(addr3))
	}
}

// TestView checks that the buffer view returned matches the allocated size and that modifications persist.
func TestView(t *testing.T) {
	a := marena.NewArena(65536)
	addr := a.Allocate(100)
	if addr == marena.ARENA_INVALID_ADDRESS {
		t.Fatal("Allocation failed")
	}
	view := a.View(addr)
	if view == nil {
		t.Fatal("View returned nil")
	}
	if len(view) != 100 {
		t.Errorf("len(View(addr)) = %d, want 100", len(view))
	}
	// Write data into the view and verify.
	for i := 0; i < len(view); i++ {
		view[i] = byte(i % 256)
	}
	for i := 0; i < len(view); i++ {
		if view[i] != byte(i%256) {
			t.Errorf("view[%d] = %d, expected %d", i, view[i], byte(i%256))
		}
	}
}

// TestSizeAndOffset directly tests the Size and Offset functions.
func TestSizeAndOffset(t *testing.T) {
	a := marena.NewArena(65536)
	addr := a.Allocate(100)
	size := marena.Size(addr)
	off := marena.Offset(addr)
	if size != 100 {
		t.Errorf("expected size 100, got %d", size)
	}
	if off < marena.ARENA_MIN_ADDRESS {
		t.Errorf("expected offset >= %d, got %d", marena.ARENA_MIN_ADDRESS, off)
	}
}

// TestAllocationFailures verifies various allocation failure scenarios.
func TestAllocationFailures(t *testing.T) {
	t.Run("exceeds max size", func(t *testing.T) {
		a := marena.NewArena(marena.ARENA_PAGESIZE)
		addr := a.Allocate(1 << 31)
		if addr != marena.ARENA_INVALID_ADDRESS {
			t.Error("allocation should fail when size exceeds ARENA_MAX_ALLOC_SIZE")
		}
	})

	t.Run("insufficient space", func(t *testing.T) {
		// Create a minimal arena that will be aligned to ARENA_PAGESIZE
		a := marena.NewArena(marena.ARENA_PAGESIZE)
		// First allocation - use almost all space, considering alignment
		addr1 := a.Allocate(marena.ARENA_PAGESIZE - marena.ARENA_MIN_ADDRESS - 16)
		if addr1 == marena.ARENA_INVALID_ADDRESS {
			t.Fatal("first allocation should succeed")
		}
		// Second allocation should fail - request remaining space plus extra
		addr2 := a.Allocate(32)
		if addr2 != marena.ARENA_INVALID_ADDRESS {
			t.Errorf("second allocation should fail, got valid address %d", addr2)
		}
	})
}

// TestAllocateMultipleFailures verifies failure scenarios for multiple allocations.
func TestAllocateMultipleFailures(t *testing.T) {
	t.Run("exceeds total space", func(t *testing.T) {
		// Create arena with minimum page size
		a := marena.NewArena(marena.ARENA_PAGESIZE)
		// Request allocations that together exceed available space
		spacePerAlloc := (marena.ARENA_PAGESIZE - marena.ARENA_MIN_ADDRESS) / 2
		var addr1, addr2 uint64 = uint64(spacePerAlloc), uint64(spacePerAlloc + 64)
		ok := a.AllocateMultiple(&addr1, &addr2)
		if ok {
			t.Error("AllocateMultiple should fail when total size exceeds arena size")
		}
	})

	t.Run("individual size too large", func(t *testing.T) {
		a := marena.NewArena(65536)
		var addr1, addr2 uint64 = 100, 1 << 31
		ok := a.AllocateMultiple(&addr1, &addr2)
		if ok {
			t.Error("AllocateMultiple should fail when any size exceeds ARENA_MAX_ALLOC_SIZE")
		}
	})
}

func TestArenaReset(t *testing.T) {
	a := marena.NewArena(1024)
	addr := a.Allocate(100)
	if addr == marena.ARENA_INVALID_ADDRESS {
		t.Fatal("Allocation failed before Reset")
	}

	a.Reset()

	if a.Allocate(100) == marena.ARENA_INVALID_ADDRESS {
		t.Error("Allocation failed after Reset")
	}
}

func TestArenaRemaining(t *testing.T) {
	a := marena.NewArena(marena.ARENA_PAGESIZE)
	initialRemaining := a.Remaining()
	if initialRemaining != marena.ARENA_PAGESIZE-marena.ARENA_MIN_ADDRESS {
		t.Errorf("Expected initial remaining to be %d, got %d",
			marena.ARENA_PAGESIZE-marena.ARENA_MIN_ADDRESS, initialRemaining)
	}

	a.Allocate(200)
	newRemaining := a.Remaining()
	expected := marena.ARENA_PAGESIZE - marena.ARENA_MIN_ADDRESS - int64((200+7)>>3<<3)
	if newRemaining != expected {
		t.Errorf("Expected new remaining = %d, got %d", expected, newRemaining)
	}
}
