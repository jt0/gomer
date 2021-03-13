package fields

import (
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

// DefaultFieldTool provides a FieldTool implementation that will create one of several types of Applier that will set a
// default value to a field.
//
// The value the field is set to is determined by the config. It can specify either a constant value or a dynamic
// one based on the result of calling a specified function. A constant value config is expressed as '=<value>', and
// a dynamic value config as '$<function_name>' where the function has been registered via RegisterFieldFunctions.
//
// If a field should only be set if it doesn't already have a value, a '?' can be pre-pended to the config input. This
// option will cause the Applier to first test for a non-zero value and only if true will it set the default.
func DefaultFieldTool() FieldTool {
	if defaultToolInstance == nil {
		defaultToolInstance = ScopingWrapper{defaultFieldTool{}}
	}
	return defaultToolInstance
}

var defaultToolInstance FieldTool

type defaultFieldTool struct{}

func (defaultFieldTool) Name() string {
	return "fields.DefaultFieldTool"
}

func (t defaultFieldTool) Applier(_ reflect.Type, _ reflect.StructField, config interface{}) (Applier, gomerr.Gomerr) {
	defaultString, ok := config.(string)
	if !ok || defaultString == "" {
		return nil, nil
	}

	var testForZero bool
	if defaultString[:1] == "?" {
		testForZero = true
		defaultString = defaultString[1:]
	}

	var defaultApplier Applier
	if defaultString[:1] == "=" {
		defaultApplier = ValueApplier{defaultString[1:]}
	} else if fn := GetFieldFunction(defaultString); fn != nil {
		defaultApplier = FunctionApplier{fn}
	} else {
		return nil, gomerr.Configuration("Unsure how to handle config").AddAttribute("Config", config.(string))
	}

	if testForZero {
		defaultApplier = ApplyAndTestApplier{nil, NonZero, defaultApplier}
	}

	return defaultApplier, nil
}
