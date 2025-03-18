package fsm

import (
	"context"
	"io"
)

type UpdateType int32

const (
	UpdateTypeCustom    UpdateType = 0
	UpdateTypeNetwork   UpdateType = 1
	UpdateTypeLogger    UpdateType = 2
	UpdateTypeTimer     UpdateType = 3
	UpdateTypeSignal    UpdateType = 4
	UpdateTypeError     UpdateType = 5
	UpdateTypeStorage   UpdateType = 6
	UpdateTypeSystem    UpdateType = 7
	UpdateTypeUser      UpdateType = 8
	UpdateTypeLifecycle UpdateType = 9
	UpdateTypeMetrics   UpdateType = 10
	UpdateTypeConfig    UpdateType = 11
	UpdateTypeHealth    UpdateType = 12
)

type Update struct {
	Registered int64 // Registered is the time when the update was registered in nanoseconds
	Type       UpdateType
	Data       []byte
}

type CurrentContext struct {
	Now     int64 // Now is the current time in nanoseconds
	Updates []Update
}

// FSM represents the internal interface for finite state machine operations
type FSM interface {
	// Forward moves the FSM forward by one step
	Forward(ctx context.Context, c *CurrentContext) error
	// Updates returns the list of updates that the FSM has generated (updates are also included in the FSM's state)
	Updates() []Update

	// Restore restores the FSM to a specific state provided by the reader
	Restore(ctx context.Context, r io.Reader) error
	// Snapshot returns a writer that can be used to save the current state of the FSM
	Snapshot(ctx context.Context) io.Writer
}

// FSMFactory represents the internal interface for creating finite state machines
type FSMFactory interface {
	// New creates a new FSM
	New() FSM
}
