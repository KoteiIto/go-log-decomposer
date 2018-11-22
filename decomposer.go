package decomposer

import (
	"encoding/json"
	"regexp"
	"strings"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

type UIDGenerator interface {
	Generate(tableName string) interface{}
}

type UIDFieldGenerator interface {
	Generate(tableName string, obj map[string]interface{}) string
}

type Decomposer struct {
	WorkerCount          int
	EventEmitter         chan *InputEvent
	EventListener        chan *OutputEvent
	ErrorListener        chan error
	UnmarshalLog         func([]byte, interface{}) error
	UIDGenerator         UIDGenerator
	UIDFieldGenerator    UIDFieldGenerator
	replacementCache     *replacementCache
	ReplaceColumnName    func(original string) string
	CreateChildTableName func(table, column string) string
}

func NewDecomposer(fs ...func(*Decomposer)) *Decomposer {
	decomposer := &Decomposer{
		WorkerCount:       1,
		EventEmitter:      make(chan *InputEvent, 1000),
		EventListener:     make(chan *OutputEvent, 1000),
		ErrorListener:     make(chan error, 1000),
		UnmarshalLog:      json.Unmarshal,
		UIDGenerator:      NewDefaultUIDGenerator(),
		UIDFieldGenerator: NewDefaultUIDFieldGenerator(),
		replacementCache:  newReplacementCache(),
		ReplaceColumnName: func(original string) string {
			snake := matchFirstCap.ReplaceAllString(original, "${1}_${2}")
			snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
			return strings.ToLower(snake)
		},
		CreateChildTableName: func(table, column string) string {
			return table + "_" + column
		},
	}
	for _, f := range fs {
		f(decomposer)
	}

	go decomposer.start()

	return decomposer
}

func (d *Decomposer) start() {
	for i := 0; i < d.WorkerCount; i++ {
		go d.startWorker()
	}
}

func (d *Decomposer) startWorker() {
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

	d.decomposeObject(inputEvent.Name, obj)
}

func (d *Decomposer) decomposeObject(
	name string,
	obj map[string]interface{},
) {
	if len(obj) == 0 {
		return
	}

	splitedName := strings.Split(name, "/")
	splitedNameLen := len(splitedName)
	tableName := splitedName[splitedNameLen-1]

	outputEvent := NewOutputEvent(name)
	for columnName, columnValue := range obj {
		columnName := d.replaceColumnName(columnName)
		switch columnValue.(type) {
		case map[string]interface{}:
		case []interface{}:
		default:
			delete(obj, columnName)
			outputEvent.Record[columnName] = columnValue
		}
	}

	uidColumnName := d.UIDFieldGenerator.Generate(tableName, outputEvent.Record)
	uidColumnValue := d.UIDGenerator.Generate(tableName)
	outputEvent.Record[uidColumnName] = uidColumnValue
	d.EventListener <- outputEvent

	for columnName, columnValue := range obj {
		columnName := d.replaceColumnName(columnName)
		childTableName := d.CreateChildTableName(tableName, columnName)
		splitedName[splitedNameLen-1] = childTableName
		childName := strings.Join(splitedName, "/")
		switch v := columnValue.(type) {
		case map[string]interface{}:
			v[uidColumnName] = uidColumnValue
			d.decomposeObject(childName, v)
		case []interface{}:
			d.decomposeArray(childName, v, uidColumnName, uidColumnValue)
		}
	}
}

func (d *Decomposer) decomposeArray(
	name string,
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
			obj["value"] = v
		}
		obj["index"] = i
		obj[uidColumnName] = uidColumnValue
		d.decomposeObject(name, obj)
	}
}

func (d *Decomposer) replaceColumnName(originalColumnName string) string {
	var columnName string
	cachedName, ok := d.replacementCache.Get(originalColumnName)
	if ok {
		columnName = cachedName
	} else {
		columnName = d.ReplaceColumnName(originalColumnName)
		d.replacementCache.Set(originalColumnName, columnName)
	}
	return columnName
}
