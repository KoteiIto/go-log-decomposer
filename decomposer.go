package decomposer

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/rs/xid"
)

type Option struct {
	LogBufferLength   int
	EventBufferLength int
	TableFieldName    string
	LogParser         func([]byte, interface{}) error
	IDGenerator       func() string
	ColumnNameRule    ReplacementRule
	IDNameRule        ReplacementRule
}

type Decomposer struct {
	logChan          chan []byte
	EventChan        chan Event
	ErrorChan        chan error
	tableFieldName   string
	logParser        func([]byte, interface{}) error
	idGenerator      func() string
	columnNameRule   ReplacementRule
	idNameRule       ReplacementRule
	replacementCache *replacementCache
}

func NewDecomposer(fs ...func(*Option)) *Decomposer {
	option := &Option{
		LogBufferLength:   1000,
		EventBufferLength: 1000,
		TableFieldName:    "table",
		LogParser:         json.Unmarshal,
		ColumnNameRule:    NewSnakeCaseReplacementRule(),
		IDNameRule:        NewSnakeCaseUIDKeyRule(),
		IDGenerator: func() string {
			return xid.New().String()
		},
	}
	for _, f := range fs {
		f(option)
	}

	decomposer := &Decomposer{
		logChan:          make(chan []byte, option.LogBufferLength),
		EventChan:        make(chan Event, option.EventBufferLength),
		ErrorChan:        make(chan error, 1000),
		tableFieldName:   option.TableFieldName,
		logParser:        option.LogParser,
		idGenerator:      option.IDGenerator,
		columnNameRule:   option.ColumnNameRule,
		idNameRule:       option.IDNameRule,
		replacementCache: newReplacementCache(),
	}
	go decomposer.start()

	return decomposer
}

func (d *Decomposer) Do(log []byte) {
	d.logChan <- log
}

func (d *Decomposer) End() {
	close(d.logChan)
}

func (d *Decomposer) start() {
	for {
		select {
		case log, ok := <-d.logChan:
			if ok {
				d.decomposeLog(log)
			} else {
				break
			}
		}
	}
}

func (d *Decomposer) decomposeLog(log []byte) {
	var obj map[string]interface{}
	err := d.logParser(log, &obj)
	if err != nil {
		d.ErrorChan <- err
		return
	}

	tableNameIF, ok := obj[d.tableFieldName]
	if !ok {
		d.ErrorChan <- fmt.Errorf("can not find table field name -> [%s]. log -> [%s]",
			d.tableFieldName, string(log))
		return
	}

	tableName, ok := tableNameIF.(string)
	if !ok {
		d.ErrorChan <- fmt.Errorf("invalid type of table field value. it must be string")
		return
	}

	d.decomposeObject(tableName, obj)
}

func (d *Decomposer) decomposeObject(
	tableName string,
	obj map[string]interface{},
) {
	if len(obj) == 0 {
		return
	}

	uidColumnName := tableName
	if d.idNameRule != nil {
		uidColumnName = d.idNameRule.Replace(tableName)
	}
	uidColumnValue := d.idGenerator()
	obj[uidColumnName] = uidColumnValue

	event := Event{
		table:  tableName,
		record: map[string]interface{}{},
	}
	for originalColumnName, columnValue := range obj {
		var columnName string
		if d.columnNameRule != nil {
			cachedName, ok := d.replacementCache.Get(originalColumnName)
			if ok {
				columnName = cachedName
			} else {
				columnName = d.columnNameRule.Replace(originalColumnName)
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
			event.record[columnName] = columnValue
		}
	}
	d.EventChan <- event
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

type Event struct {
	table  string
	record map[string]interface{}
}

type ReplacementRule interface {
	Replace(string) string
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")

var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

type SnakeCaseReplacementRule struct{}

func NewSnakeCaseReplacementRule() *SnakeCaseReplacementRule {
	return &SnakeCaseReplacementRule{}
}

func (r *SnakeCaseReplacementRule) Replace(s string) string {
	snake := matchFirstCap.ReplaceAllString(s, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

type SnakeCaseUIDKeyRule struct{}

func NewSnakeCaseUIDKeyRule() *SnakeCaseUIDKeyRule {
	return &SnakeCaseUIDKeyRule{}
}

func (r *SnakeCaseUIDKeyRule) Replace(s string) string {
	return s + "_id"
}

type replacementCache struct {
	cache map[string]string
	mutex *sync.Mutex
}

func newReplacementCache() *replacementCache {
	return &replacementCache{
		cache: map[string]string{},
		mutex: &sync.Mutex{},
	}
}

func (c replacementCache) Set(key string, value string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cache[key] = value
}

func (c replacementCache) Get(key string) (value string, ok bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	value, ok = c.cache[key]
	return
}
