package fields_test

import (
	"reflect"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

type TestCase struct {
	Name     string
	Tool     fields.FieldTool
	Context  fields.ToolContext
	Input    interface{}
	Expected interface{} // can be the same type as output or a gomerr.Gomerr
}

func RunTests(t *testing.T, tests []TestCase) {
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			targetType := reflect.TypeOf(tt.Input).Elem()
			fs, ge := fields.Get(targetType)
			assert.Success(t, ge)

			ge = fs.ApplyTools(reflect.ValueOf(tt.Input).Elem(), fields.Application{tt.Tool.Name(), tt.Context})
			if expectedError, ok := tt.Expected.(gomerr.Gomerr); !ok {
				assert.Success(t, ge)
				assert.Equals(t, tt.Expected, tt.Input)
			} else {
				assert.ErrorType(t, ge, expectedError, "Error did not match expected type")
			}
		})
	}
}
