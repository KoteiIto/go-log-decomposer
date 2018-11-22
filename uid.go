package decomposer

import (
	"github.com/rs/xid"
)

type DefaultUIDGenerator struct{}

func NewDefaultUIDGenerator() *DefaultUIDGenerator {
	return &DefaultUIDGenerator{}
}

func (x *DefaultUIDGenerator) Generate(_ string) interface{} {
	return xid.New().String()
}

type DefaultUIDFieldGenerator struct{}

func NewDefaultUIDFieldGenerator() *DefaultUIDFieldGenerator {
	return &DefaultUIDFieldGenerator{}
}

func (u *DefaultUIDFieldGenerator) Generate(tableName string, record map[string]interface{}) string {
	if _, ok := record["id"]; !ok {
		return "id"
	}

	fieldName := tableName + "_id"
	if _, ok := record[fieldName]; !ok {
		return fieldName
	}

	return "_" + fieldName
}
