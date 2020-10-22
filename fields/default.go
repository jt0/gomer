package fields

import (
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

type DefaultFunction func(reflect.Value) interface{}

func RegisterDefaultFunctions(functions map[string]DefaultFunction) {
	for fnName := range functions {
		if ge := gomerr.Test("Function names must start with a '$' symbol and between 2 and 64 characters long", fnName, dollarNameConstraint); ge != nil {
			panic(ge)
		}

		if fnName[1:2] == "_" {
			panic("Function names first name character must not an underscore")
		}
	}

	registeredDefaultFunctions = functions
}

var registeredDefaultFunctions map[string]DefaultFunction

func (f *field) defaultTag(defaultTag string) {
	if defaultTag == "" {
		return
	}

	if defaultTag[:1] == "?" {
		f.bypassDefaultIfSet = true
		defaultTag = defaultTag[1:]
	}

	if defaultTag[:1] == "$" {
		if fn, ok := registeredDefaultFunctions[defaultTag]; ok {
			f.defaultValueFunction = fn
			return
		}
	}

	f.defaultValue = defaultTag
}
