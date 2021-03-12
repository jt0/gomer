package fields_test

import (
	"reflect"
	"testing"

	"github.com/jt0/gomer/_test/helpers/fields_test"
	"github.com/jt0/gomer/fields"
)

type DefaultTest struct {
	StringWithDefaultFunction string `default:"$color"`
	StringWithDefaultValue    string `default:"=123"`
	IntWithDefaultValue       int    `default:"=123"`
}

const orange = "orange"

func init() {
	fields.RegisterFieldFunctions(map[string]func(reflect.Value) interface{}{
		"$color": func(_ reflect.Value) interface{} {
			return orange
		},
	})

	fields.StructTagToFieldTools(map[string]fields.FieldTool{"default": fields.FieldDefaultTool})
}

func TestDefaultTool(t *testing.T) {
	fields_test.RunTests(t, []fields_test.TestCase{
		{"Simple test", fields.FieldDefaultTool, fields.EnsureContext(), &DefaultTest{}, nil, &DefaultTest{orange, "123", 123}},
	})
}
