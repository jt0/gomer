package id_test

import (
	"reflect"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/_test/helpers/fields_test"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/id"
)

type IdTest struct {
	Id string `id:"$id"`
}

type CreateIdTest struct {
	Id string `id:"create:$id"`
}

type UpdateIdTest struct {
	Id string `id:"update:$id"`
}

const defaultId = "DeFaUlTiD"

func init() {
	fields.RegisterFieldFunctions(map[string]func(reflect.Value) interface{}{
		"$id": func(_ reflect.Value) interface{} {
			return defaultId
		},
	})

	fields.SetTagKeyToFieldToolMap(map[string]fields.FieldTool{"id": id.IdFieldTool})
}

func TestIdTool(t *testing.T) {
	fields_test.RunTests(t, []fields_test.TestCase{
		{"No scope, implicit id assignment", id.IdFieldTool, fields.ToolContext{}, &IdTest{}, nil, &IdTest{defaultId}},
		{"Wrong scope ('unknown'), implicit id assignment", id.IdFieldTool, fields.AddScopeToContext("unknown"), &IdTest{}, nil, &IdTest{defaultId}},
		{"No scope, id not assigned", id.IdFieldTool, fields.ToolContext{}, &CreateIdTest{}, nil, &CreateIdTest{}},
		{"Right scope ('create'), explicit id assignment", id.IdFieldTool, fields.AddScopeToContext("create"), &CreateIdTest{}, nil, &CreateIdTest{defaultId}},
		{"Wrong scope ('update'), id not assigned", id.IdFieldTool, fields.AddScopeToContext("update"), &CreateIdTest{}, nil, &CreateIdTest{}},
		{"Wrong scope ('create'), id not assigned", id.IdFieldTool, fields.AddScopeToContext("create"), &UpdateIdTest{}, nil, &UpdateIdTest{}},
		{"Right scope ('update'), explicit id assignment", id.IdFieldTool, fields.AddScopeToContext("update"), &UpdateIdTest{}, nil, &UpdateIdTest{defaultId}},
	})
}

func TestTwoIdFieldsFail(t *testing.T) {
	type TwoIdsTest struct {
		Id1 string `id:"$id"`
		Id2 string `id:"$id"`
	}

	_, ge := fields.NewFields(reflect.TypeOf(TwoIdsTest{}))
	assert.ErrorType(t, ge, &gomerr.ConfigurationError{}, "Should fail due to multiple fields with 'id' struct tag")
}
