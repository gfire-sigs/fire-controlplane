package fsm

type State interface {
}

type FSM interface {
	Forward()
	Restore(State) error
	Snapshot() (State, error)
}
