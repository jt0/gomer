package fields

import (
	"fmt"
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

type FieldTool interface {
	Name() string
	Applier(structType reflect.Type, structField reflect.StructField, input interface{}) (Applier, gomerr.Gomerr)
}

type MustUse interface {
	MustUse() bool
}

type Applier interface {
	Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) gomerr.Gomerr
}

type ConfigProvider interface {
	ConfigPerTool(structType reflect.Type, structField reflect.StructField) map[FieldTool]interface{}
}

var FieldToolConfigProvider ConfigProvider = StructTagConfigProvider{}

type StructTagConfigProvider map[string]FieldTool

func (s StructTagConfigProvider) WithKey(tagKey string, tool FieldTool) StructTagConfigProvider {
	if tagKey != "" {
		s[tagKey] = tool
	}

	return s
}

func (s StructTagConfigProvider) ConfigPerTool(_ reflect.Type, structField reflect.StructField) map[FieldTool]interface{} {
	cpt := make(map[FieldTool]interface{})
	for tagKey, fieldTool := range s {
		if tagValue, ok := structField.Tag.Lookup(tagKey); ok {
			cpt[fieldTool] = tagValue
		} else if tool, ok := fieldTool.(MustUse); ok && tool.MustUse() {
			cpt[fieldTool] = ""
		}
	}

	return cpt
}

type FunctionApplier struct {
	FieldName string
	Function  func(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) interface{}
}

func (a FunctionApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) gomerr.Gomerr {
	functionValue := a.Function(structValue, fieldValue, toolContext)
	if ge := flect.SetValue(fieldValue, functionValue); ge != nil {
		return gomerr.Configuration("Unable to set field to function result").AddAttributes("Field", a.FieldName, "FunctionResult", functionValue).Wrap(ge)
	}

	return nil
}

type MethodApplier struct {
	FieldName  string
	MethodName string
}

func (a MethodApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, _ ToolContext) gomerr.Gomerr {
	method := structValue.MethodByName(a.MethodName)

	var in []reflect.Value
	if method.Type().NumIn() == 1 {
		in = []reflect.Value{fieldValue}
	}

	results := method.Call(in)
	result := results[0].Interface()
	if ge := flect.SetValue(fieldValue, result); ge != nil {
		return gomerr.Configuration("Unable to set field to method result").AddAttributes("Field", a.FieldName, "Method", a.MethodName, "MethodResult", result).Wrap(ge)
	}

	return nil
}

type ValueApplier struct {
	FieldName   string
	StaticValue string
}

func (a ValueApplier) Apply(_ reflect.Value, fieldValue reflect.Value, _ ToolContext) gomerr.Gomerr {
	if ge := flect.SetValue(fieldValue, a.StaticValue); ge != nil {
		return gomerr.Configuration("Unable to set field to value").AddAttributes("Field", a.FieldName, "Value", a.StaticValue).Wrap(ge)
	}

	return nil
}

type ApplyAndTestApplier struct {
	FieldName string
	Left      Applier
	Test      func(value reflect.Value) bool
	Right     Applier
}

var (
	NonZero = func(v reflect.Value) bool { return !v.IsZero() }
)

func (a ApplyAndTestApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) gomerr.Gomerr {
	var leftGe gomerr.Gomerr

	if a.Left != nil {
		leftGe = a.Left.Apply(structValue, fieldValue, toolContext)
	}

	if leftGe == nil && a.Test(fieldValue) {
		return nil
	}

	if a.Right != nil {
		ge := a.Right.Apply(structValue, fieldValue, toolContext)
		if ge != nil {
			return ge.AddAttributes("Field", a.FieldName).Wrap(leftGe) // Okay if applierGe is nil or not-nil
		} else if leftGe != nil {
			fmt.Println("Left-side applier failed, but right side succeeded. Left error:\n", leftGe.Error())
		}
		return nil
	} else if leftGe != nil {
		return leftGe.AddAttributes("Field", a.FieldName)
	}

	return gomerr.Configuration("Field value failed to validate and no secondary applier is specified.").AddAttributes("Field", a.FieldName)
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
