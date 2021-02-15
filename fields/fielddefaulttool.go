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
	return "fields.FieldDefaultTool"
}

func (t fieldDefaultTool) New(structType reflect.Type, structField reflect.StructField, input interface{}) (FieldToolApplier, gomerr.Gomerr) {
	if input == nil {
		return nil, nil
	}

	fdt := fieldDefaultTool{}

	defaultValueString := input.(string)
	if len(defaultValueString) == 0 {
		return fdt, nil
	}

	if defaultValueString[:1] == "?" {
		fdt.bypassIfSet = true
		defaultValueString = defaultValueString[1:]
	}

	if len(defaultValueString) > 0 && defaultValueString[:1] != "$" {
		fdt.value = defaultValueString
	} else if fn := GetFieldFunction(defaultValueString); fn != nil {
		fdt.function = fn
	} else {
		fdt.value = defaultValueString
	}

	return fdt, nil
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
