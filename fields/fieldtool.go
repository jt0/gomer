package fields

import (
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

type FieldTool interface {
	Name() string
	New(structType reflect.Type, structField reflect.StructField, input interface{}) (FieldTool, gomerr.Gomerr)
	Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) gomerr.Gomerr
}

type ToolContext map[string]interface{}

func (tc ToolContext) Add(key string, value interface{}) ToolContext {
	etc := EnsureContext(tc)
	etc[key] = value
	return etc
}

func (tc ToolContext) IncrementInt(key string, amount int) {
	if cv, ok := tc[key]; !ok {
		tc[key] = amount
	} else if ci, ok := cv.(int); ok {
		tc[key] = ci + amount
	} // Field defaultValue is something other than an int so ignore.
}

func EnsureContext(toolContext ...ToolContext) ToolContext {
	if len(toolContext) > 0 && toolContext[0] != nil {
		return toolContext[0]
	} else {
		return ToolContext{}
	}
}
