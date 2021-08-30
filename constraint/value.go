package constraint

import (
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

var (
	IsNil    = nilConstraint("IsNil", false)
	IsNotNil = nilConstraint("IsNotNil", true)
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

func Nil(value *interface{}) Constraint {
	return New("Nil", value, func(toTest interface{}) gomerr.Gomerr {
		return IsNil.Test(*value)
	})
}

func NotNil(value *interface{}) Constraint {
	return New("NotNil", value, func(toTest interface{}) gomerr.Gomerr {
		return IsNotNil.Test(*value)
	})
}

var (
	IsZero    = zeroConstraint("IsZero", false)
	IsNotZero = zeroConstraint("IsNotZero", true)
)

func zeroConstraint(name string, errorIfZero bool) Constraint {
	return New(name, nil, func(toTest interface{}) gomerr.Gomerr {
		if reflect.ValueOf(toTest).IsZero() == errorIfZero {
			return NotSatisfied(toTest)
		}
		return nil
	})
}

func Zero(value *interface{}) Constraint {
	return New("Zero", value, func(toTest interface{}) gomerr.Gomerr {
		return IsZero.Test(*value)
	})
}

func NotZero(value *interface{}) Constraint {
	return New("NotZero", value, func(toTest interface{}) gomerr.Gomerr {
		return IsNotZero.Test(*value)
	})
}

var Required = New("Required", nil, func(toTest interface{}) gomerr.Gomerr {
	ttv, ok := flect.ReadableIndirectValue(toTest)
	if !ok || ttv.IsZero() {
		return NotSatisfied(toTest)
	}
	return nil
})
