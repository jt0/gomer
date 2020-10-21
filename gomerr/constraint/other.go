package constraint

import (
	"reflect"
)

var Required = required()

func required() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		vv := reflect.ValueOf(value)
		return vv.IsValid() && (vv.Kind() != reflect.Ptr || !vv.IsNil())
	}}.setDetails("Required", true)
}

var Invalid = invalid()

func invalid() Constrainer {
	return Constrainer{test: func(interface{}) bool {
		return false
	}}.setDetails("Invalid", true)
}
