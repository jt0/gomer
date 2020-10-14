package constraint

import (
	"reflect"
)

var Required = required()

func required() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return !reflect.ValueOf(value).IsZero()
	}}.setDetails("Required", true)
}

var Invalid = invalid()

func invalid() Constrainer {
	return Constrainer{test: func(interface{}) bool {
		return false
	}}.setDetails("Invalid", true)
}
