package fields

import (
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

type FieldTool interface {
	Name() string
	New(structType reflect.Type, structField reflect.StructField, input interface{}) (Applier, gomerr.Gomerr)
}

type Applier interface {
	Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) gomerr.Gomerr
}

type FunctionApplier struct {
	Function func(structValue reflect.Value) interface{}
}

func (a FunctionApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, _ ToolContext) gomerr.Gomerr {
	defaultValue := a.Function(structValue)
	if ge := flect.SetValue(fieldValue, defaultValue); ge != nil {
		return gomerr.Configuration("Unable to set field to function result").AddAttribute("FunctionResult", defaultValue).Wrap(ge)
	}
	return nil
}

type ValueApplier struct {
	Value string
}

func (a ValueApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, _ ToolContext) gomerr.Gomerr {
	if ge := flect.SetValue(fieldValue, a.Value); ge != nil {
		return gomerr.Configuration("Unable to set field to value").AddAttribute("Value", a.Value).Wrap(ge)
	}
	return nil
}

type ApplyAndTestApplier struct {
	Applier  Applier
	IsValid  func(value reflect.Value) bool
	Fallback Applier
}

var (
	NonZero = func(v reflect.Value) bool { return !v.IsZero() }
)

func (a ApplyAndTestApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) gomerr.Gomerr {
	var applierGe gomerr.Gomerr

	if a.Applier != nil {
		applierGe = a.Applier.Apply(structValue, fieldValue, toolContext)
	}

	if applierGe == nil && a.IsValid(fieldValue) {
		return nil
	}

	if a.Fallback != nil {
		ge := a.Fallback.Apply(structValue, fieldValue, toolContext)
		if ge != nil {
			return ge.Wrap(applierGe) // Okay if applierGe is nil or not-nil
		}
		return nil
	} else if applierGe != nil {
		return applierGe
	}

	return gomerr.Configuration("Field value failed to validate and no fallback applier is specified.")
}

type NoopApplier struct{}

func (NoopApplier) Apply(reflect.Value, reflect.Value, ToolContext) gomerr.Gomerr {
	return nil
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
