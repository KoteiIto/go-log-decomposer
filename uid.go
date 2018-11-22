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

func (u *DefaultUIDFieldGenerator) Generate(tableName string, obj map[string]interface{}) string {
	if _, ok := obj["id"]; !ok {
		return "id"
	}

	fieldName := tableName + "_id"
	if _, ok := obj[fieldName]; !ok {
		return fieldName
	}

	return "_" + fieldName
}
