package structs

import (
	"reflect"
	"strings"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

type Applier interface {
	Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) gomerr.Gomerr
}

type StructApplier struct {
	Source string
}

func (a StructApplier) Apply(sv reflect.Value, fv reflect.Value, _ ToolContext) gomerr.Gomerr {
	value, ge := ValueFromStruct(sv, fv, a.Source)
	if ge != nil {
		return ge
	}

	if ge = flect.SetValue(fv, value); ge != nil {
		return gomerr.Configuration("unable to set value").AddAttributes("source", a.Source, "value", value).Wrap(ge)
	}

	return nil
}

func ValueFromStruct(sv reflect.Value, fv reflect.Value, source string) (any, gomerr.Gomerr) {
	if source == "$" {
		return sv.Interface(), nil
	}

	if len(source) < 3 || source[0:2] != "$." {
		return nil, gomerr.Configuration("unexpected source format (expected field/function name with '$.' prefix): " + source)
	}

	source = source[2:]
	if source[len(source)-1] == ')' {
		m := sv.MethodByName(source[0:strings.IndexByte(source, '(')])
		if !m.IsValid() {
			return nil, gomerr.Configuration("source method not found").AddAttribute("source", source)
		}

		var in []reflect.Value
		if m.Type().NumIn() == 1 {
			in = []reflect.Value{fv}
		}

		results := m.Call(in)
		return results[0].Interface(), nil
	}

	f := sv.FieldByName(source)
	if !f.IsValid() {
		return nil, gomerr.Configuration("source field not found").AddAttribute("source", source)
	}

	if f.Kind() == reflect.Ptr && f.IsNil() && source == "Attributes" {
		println("Source is nil!!")
	}
	return f.Interface(), nil
}

type ValueApplier struct {
	StaticValue string
}

var valueConstants = make(map[string]any)

func RegisterValueConstants(constants map[string]any) {
	for k, v := range constants {
		if len(k) < 2 || len(k) > 64 || k[0] != '#' {
			panic("Constants must start with a '#' symbol and be between 2 and 64 characters long")
		}
		valueConstants[k] = v
	}
}

func (a ValueApplier) Apply(_ reflect.Value, fv reflect.Value, _ ToolContext) gomerr.Gomerr {
	staticValue, ok := valueConstants[a.StaticValue]
	if !ok {
		staticValue = a.StaticValue
	}
	if ge := flect.SetValue(fv, staticValue); ge != nil {
		return gomerr.Configuration("unable to set field to value").AddAttribute("value", a.StaticValue).Wrap(ge)
	}

	return nil
}

type NoApplier struct{}

func (NoApplier) Apply(reflect.Value, reflect.Value, ToolContext) gomerr.Gomerr {
	return nil
}
