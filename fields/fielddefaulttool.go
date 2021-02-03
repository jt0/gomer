package fields

import (
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

var FieldDefaultTool = ScopingWrapper{FieldTool: fieldDefaultTool{}}

// Either 'value' or 'function' will be set based on the provided input.
type fieldDefaultTool struct {
	value       string
	function    func(structValue reflect.Value) interface{}
	bypassIfSet bool
}

func (t fieldDefaultTool) Name() string {
	return "FieldDefaultTool"
}

func (t fieldDefaultTool) New(_ reflect.Type, _ reflect.StructField, input interface{}) (FieldTool, gomerr.Gomerr) {
	dt := fieldDefaultTool{}

	value := input.(string)
	if value[:1] == "?" {
		dt.bypassIfSet = true
		value = value[1:]
	}

	if value[:1] != "$" {
		dt.value = value
	} else if fn := GetFieldFunction(value); fn != nil {
		dt.function = fn
	} else {
		dt.value = value
	}

	return dt, nil
}

func (t fieldDefaultTool) Apply(structValue reflect.Value, fieldValue reflect.Value, _ ToolContext) gomerr.Gomerr {
	if !fieldValue.IsValid() || (t.bypassIfSet && !fieldValue.IsZero()) {
		return nil
	}

	var defaultValue interface{}
	if t.function != nil {
		defaultValue = t.function(structValue)
	} else {
		defaultValue = t.value
	}

	if ge := flect.SetValue(fieldValue, defaultValue); ge != nil {
		return gomerr.Configuration("Unable to set field to default value").AddAttribute("Value", defaultValue).Wrap(ge)
	}

	return nil
}
