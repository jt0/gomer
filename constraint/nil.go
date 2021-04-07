package constraint

import (
	"reflect"
)

var Nil = func() *constraint {
	return &constraint{"Nil", nil, func(toTest interface{}) bool {
		vv := reflect.ValueOf(toTest)
		if !vv.IsValid() {
			return false
		}
		switch vv.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			return vv.IsNil()
		default:
			return false
		}
	}}
}()

var NotNil = func() *constraint {
	return &constraint{"NotNil", "", func(toTest interface{}) bool {
		return !Nil.Test(toTest)
	}}
}()
