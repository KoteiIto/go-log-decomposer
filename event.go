package decomposer

type InputEvent struct {
	Name string
	Log  []byte
}

func NewInputEvent(name string, log []byte) *InputEvent {
	return &InputEvent{
		Name: name,
		Log:  log,
	}
}

type OutputEvent struct {
	Name   string
	Record map[string]interface{}
}

func NewOutputEvent(name string) *OutputEvent {
	return &OutputEvent{
		Name:   name,
		Record: map[string]interface{}{},
	}
}
