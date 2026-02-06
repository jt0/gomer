package structs

import (
	"reflect"
	"strings"
	"time"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

type ToolFunction func(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) (output any, ge gomerr.Gomerr)

func (f ToolFunction) Apply(sv reflect.Value, fv reflect.Value, tc ToolContext) gomerr.Gomerr {
	value, ge := f(sv, fv, tc)
	if ge != nil {
		return ge
	}

	if ge = flect.SetValue(fv, value); ge != nil {
		return gomerr.Configuration("Unable to set field to function result").AddAttribute("FunctionResult", value).Wrap(ge)
	}

	return nil
}

func init() {
	SetNowToolFunctionPrecision(time.Millisecond)
}

func RegisterToolFunctions(functions map[string]ToolFunction) gomerr.Gomerr {
	var errors []gomerr.Gomerr
	for name, function := range functions {
		if ge := RegisterToolFunction(name, function); ge != nil {
			errors = append(errors, ge)
		}
	}
	return gomerr.Batcher(errors)
}

func RegisterToolFunction(name string, function ToolFunction) gomerr.Gomerr {
	if len(name) < 2 {
		return gomerr.Configuration("Field function names must start with a '$' symbol and be between 2 and 64 characters long")
	} else if name[0] != '$' || len(name) > 64 {
		return gomerr.Configuration("Field function names must start with a '$' symbol and be between 2 and 64 characters long")
	} else if name[1] == '_' && !strings.HasPrefix(reflect.TypeOf(function).PkgPath(), "github.com/jt0/gomer") {
		return gomerr.Configuration("Field function names cannot start with '$_'")
	}

	putToolFunction(name, function)
	return nil
}

func SetNowToolFunctionPrecision(precision time.Duration) {
	putToolFunction("$_now", func(reflect.Value, reflect.Value, ToolContext) (any, gomerr.Gomerr) {
		return time.Now().UTC().Truncate(precision), nil
	})
}

var functions = map[string]ToolFunction{}

func putToolFunction(name string, function ToolFunction) {
	functions[strings.ToLower(name)] = function
}

func GetToolFunction(name string) ToolFunction {
	return functions[strings.ToLower(name)]
}
