# The FSM Package

The FSM (Finite State Machine) package provides an implementation of a state machine for managing distributed system state and coordinating updates across the gfire control plane.

## Overview

This package implements a Finite State Machine (FSM) that:
- Processes typed updates for different aspects of the system
- Maintains temporal context for state transitions
- Supports state snapshots and restoration
- Integrates with the consensus system for distributed coordination

## Components

### Update Types

The FSM supports various update types for different system aspects:
- Network (type 1)
- Logger (type 2)
- Timer (type 3)
- Signal (type 4)
- Error (type 5)
- Storage (type 6)
- System (type 7)
- User (type 8)
- Lifecycle (type 9)
- Metrics (type 10)
- Config (type 11)
- Health (type 12)
- Custom (type 0)

### Current Context

The FSM maintains a CurrentContext that includes:
- Current timestamp (in nanoseconds)
- List of pending updates

### Core Interfaces

#### FSM Interface

```go
type FSM interface {
    // Forward moves the FSM forward by one step
    Forward(ctx context.Context, c *CurrentContext) error
    
    // Updates returns the list of updates that the FSM has generated
    Updates() []Update
    
    // Restore restores the FSM to a specific state
    Restore(ctx context.Context, r io.Reader) error
    
    // Snapshot returns a writer for saving the current state
    Snapshot(ctx context.Context) io.Writer
}
```

#### FSMFactory Interface

```go
type FSMFactory interface {
    // New creates a new FSM instance
    New() FSM
}
```

## State Management

### Updates

Updates are the primary mechanism for state changes and include:
- Registration timestamp (nanoseconds)
- Update type (one of the defined update types)
- Update data (byte array)

### State Persistence

The FSM provides built-in support for:
- Taking snapshots of the current state
- Restoring state from snapshots
- Maintaining update history

## Integration

The FSM package is designed to work seamlessly with the consensus package to ensure consistent state transitions across distributed nodes. It provides the foundation for:
- Distributed state management
- Consistent update processing
- Recoverable system state
- Coordinated state transitions