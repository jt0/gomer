package structs_test

import (
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

type TestCase struct {
	Name     string
	Tool     *structs.Tool
	Context  *structs.ToolContext
	Input    interface{}
	Expected interface{} // can be the same type as output or a gomerr.Gomerr
}

func RunTests(t *testing.T, tests []TestCase) {
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ge := structs.ApplyTools(tt.Input, tt.Context, tt.Tool)
			if expectedError, ok := tt.Expected.(gomerr.Gomerr); !ok {
				assert.Success(t, ge)
				assert.Equals(t, tt.Expected, tt.Input)
			} else {
				assert.ErrorType(t, ge, expectedError, "Error did not match expected type")
			}
		})
	}
}
