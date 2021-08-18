package constraint

import (
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

var (
	Nil    = nilConstraint("IsNil", false)
	NotNil = nilConstraint("IsNotNil", true)
)

func nilConstraint(name string, errorIfNil bool) Constraint {
	return New(name, nil, func(toTest interface{}) gomerr.Gomerr {
		switch vv := reflect.ValueOf(toTest); vv.Kind() {
		case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
			if vv.IsNil() == errorIfNil {
				return NotSatisfied(toTest)
			}
			return nil
		default:
			return gomerr.Unprocessable("Test value is not a nil-able type", reflect.TypeOf(toTest))
		}
	})
}
