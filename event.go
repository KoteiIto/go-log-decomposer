package decomposer

type InputEvent struct {
	Table string
	Log   []byte
}

func NewInputEvent(table string, log []byte) *InputEvent {
	return &InputEvent{
		Table: table,
		Log:   log,
	}
}

type OutputEvent struct {
	Table  string
	Record map[string]interface{}
}

func NewOutputEvent(table string) *OutputEvent {
	return &OutputEvent{
		Table:  table,
		Record: map[string]interface{}{},
	}
}
