package decomposer

import (
	"encoding/json"
)

type ReplacementRule interface {
	Replace(string) string
}

type UIDGenerator interface {
	Generate(tableName string) interface{}
}

type UIDFieldGenerator interface {
	Generate(tableName string, obj map[string]interface{}) string
}

type Decomposer struct {
	EventEmitter      chan *InputEvent
	EventListener     chan *OutputEvent
	ErrorListener     chan error
	UnmarshalLog      func([]byte, interface{}) error
	UIDGenerator      UIDGenerator
	UIDFieldGenerator UIDFieldGenerator
	ColumnNameRule    ReplacementRule
	replacementCache  *replacementCache
}

func NewDecomposer(fs ...func(*Decomposer)) *Decomposer {
	decomposer := &Decomposer{
		EventEmitter:      make(chan *InputEvent, 1000),
		EventListener:     make(chan *OutputEvent, 1000),
		ErrorListener:     make(chan error, 1000),
		UnmarshalLog:      json.Unmarshal,
		UIDGenerator:      NewDefaultUIDGenerator(),
		UIDFieldGenerator: NewDefaultUIDFieldGenerator(),
		ColumnNameRule:    NewDefaultReplacementRule(),
		replacementCache:  newReplacementCache(),
	}
	for _, f := range fs {
		f(decomposer)
	}

	go decomposer.start()

	return decomposer
}

func (d *Decomposer) start() {
LOOP:
	for {
		select {
		case inputEvent, ok := <-d.EventEmitter:
			if ok {
				d.do(inputEvent)
			} else {
				break LOOP
			}
		}
	}
}

func (d *Decomposer) do(inputEvent *InputEvent) {
	var obj map[string]interface{}
	err := d.UnmarshalLog(inputEvent.Log, &obj)
	if err != nil {
		d.ErrorListener <- err
		return
	}

	d.decomposeObject(inputEvent.Table, obj)
}

func (d *Decomposer) decomposeObject(
	tableName string,
	obj map[string]interface{},
) {
	if len(obj) == 0 {
		return
	}

	uidColumnName := d.UIDFieldGenerator.Generate(tableName, obj)
	uidColumnValue := d.UIDGenerator.Generate(tableName)
	obj[uidColumnName] = uidColumnValue

	outputEvent := NewOutputEvent(tableName)
	for originalColumnName, columnValue := range obj {
		var columnName string
		if d.ColumnNameRule != nil {
			cachedName, ok := d.replacementCache.Get(originalColumnName)
			if ok {
				columnName = cachedName
			} else {
				columnName = d.ColumnNameRule.Replace(originalColumnName)
				d.replacementCache.Set(originalColumnName, columnName)
			}
		}
		switch v := columnValue.(type) {
		case map[string]interface{}:
			v[uidColumnName] = uidColumnValue
			d.decomposeObject(columnName, v)
		case []interface{}:
			d.decomposeArray(columnName, v, uidColumnName, uidColumnValue)
		default:
			outputEvent.Record[columnName] = columnValue
		}
	}
	d.EventListener <- outputEvent
}

func (d *Decomposer) decomposeArray(
	tableName string,
	arr []interface{},
	uidColumnName string,
	uidColumnValue interface{},
) {
	if len(arr) == 0 {
		return
	}

	for i, v := range arr {
		obj, ok := v.(map[string]interface{})
		if !ok {
			obj = map[string]interface{}{}
			obj[tableName] = v
		}
		obj["index"] = i + 1
		obj[uidColumnName] = uidColumnValue
		d.decomposeObject(tableName, obj)
	}
}
