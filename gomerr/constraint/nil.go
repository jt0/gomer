package constraint

import (
	"reflect"
)

var Nil = isNil()

func isNil() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		vv := reflect.ValueOf(value)
		if !vv.IsValid() {
			return false
		}
		switch vv.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			return vv.IsNil()
		default:
			return false
		}
	}}.setDetails("Nil", true, LookupName, "nil")
}

var NotNil = notNil()

func notNil() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return !Nil.Test(value)
	}}.setDetails("Nil", false, LookupName, "notnil")
}
