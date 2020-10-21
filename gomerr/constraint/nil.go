package constraint

import (
	"reflect"
)

var Nil = isNil()

func isNil() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		vv := reflect.ValueOf(value)
		switch vv.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			return reflect.ValueOf(value).IsNil()
		default:
			return false
		}
	}}.setDetails("Nil", true)
}

var NotNil = notNil()

//goland:noinspection GoBoolExpressions
func notNil() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		vv := reflect.ValueOf(value)
		switch vv.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			return !reflect.ValueOf(value).IsNil()
		default:
			return !false
		}
	}}.setDetails("Nil", false)
}
