package fields

import (
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

var FieldDefaultTool = ScopingWrapper{fieldDefaultTool{}}

type fieldDefaultTool struct{}

func (t fieldDefaultTool) Name() string {
	return "fields.FieldDefaultTool"
}

func (t fieldDefaultTool) New(_ reflect.Type, _ reflect.StructField, input interface{}) (Applier, gomerr.Gomerr) {
	defaultString, ok := input.(string)
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
		return nil, gomerr.Configuration("Unsure how to handle input").AddAttribute("Input", input.(string))
	}

	if testForZero {
		defaultApplier = ApplyAndTestApplier{NoopApplier{}, NonZero, defaultApplier}
	}

	return defaultApplier, nil
}
