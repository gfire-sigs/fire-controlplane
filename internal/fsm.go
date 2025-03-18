package fsm

import (
	"fmt"
)

// State represents an FSM state.
type State string

// Event represents an event triggering a state transition.
type Event string

// Predefined states.
const (
	StateIdle    State = "idle"
	StateRunning State = "running"
	StateStopped State = "stopped"
)

// FSM defines a simple finite state machine.
type FSM struct {
	currentState State
	transitions  map[State]map[Event]State
}

// NewFSM initializes a new FSM with the given start state and default transitions.
func NewFSM(initial State) *FSM {
	// Define transitions where transitions[current][event] = next state.
	transitions := map[State]map[Event]State{
		StateIdle: {
			"start": StateRunning,
		},
		StateRunning: {
			"stop": StateStopped,
		},
		StateStopped: {
			"reset": StateIdle,
		},
	}

	return &FSM{
		currentState: initial,
		transitions:  transitions,
	}
}

// CurrentState returns the current state of the FSM.
func (f *FSM) CurrentState() State {
	return f.currentState
}

// Trigger processes an event and transitions to the next state if defined.
func (f *FSM) Trigger(event Event) error {
	if nextStates, ok := f.transitions[f.currentState]; ok {
		if next, ok := nextStates[event]; ok {
			fmt.Printf("Transitioning from %s to %s on event %s\n", f.currentState, next, event)
			f.currentState = next
			return nil
		}
	}
	return fmt.Errorf("no transition defined from state %s on event %s", f.currentState, event)
}

/*
Example Usage:

package main

import (
	"fmt"
	"internal/fsm"
)

func main() {
	// Initialize FSM with initial state.
	machine := fsm.NewFSM(fsm.StateIdle)
	fmt.Println("Initial State:", machine.CurrentState())

	// Trigger the 'start' event.
	if err := machine.Trigger("start"); err != nil {
		fmt.Println("Error:", err)
	}
	fmt.Println("State after 'start':", machine.CurrentState())

	// Trigger the 'stop' event.
	if err := machine.Trigger("stop"); err != nil {
		fmt.Println("Error:", err)
	}
	fmt.Println("State after 'stop':", machine.CurrentState())

	// Trigger the 'reset' event.
	if err := machine.Trigger("reset"); err != nil {
		fmt.Println("Error:", err)
	}
	fmt.Println("State after 'reset':", machine.CurrentState())
}
*/
