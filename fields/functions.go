package fields

import (
	"reflect"
)

var fieldFunctions = map[string]func(structValue reflect.Value) interface{}{}

func RegisterFieldFunctions(functions map[string]func(structValue reflect.Value) interface{}) {
	if fieldFunctions == nil {
		fieldFunctions = make(map[string]func(structValue reflect.Value) interface{})
	}

	for fnName, function := range functions {
		if len(fnName) < 2 || len(fnName) > 64 || fnName[0] != '$' {
			panic("Field function names must start with a '$' symbol and between 2 and 64 characters long")
		}

		if fnName[1:2] == "_" {
			panic("Function Name must not start with an underscore")
		}

		fieldFunctions[fnName] = function
	}
}

func GetFieldFunction(functionName string) func(structValue reflect.Value) interface{} {
	return fieldFunctions[functionName]
}
