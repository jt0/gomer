package constraint

import (
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

var (
	IsNil    = nilConstraint("isNil", false)
	IsNotNil = nilConstraint("isNotNil", true)
)

func nilConstraint(name string, errorIfNil bool) Constraint {
	return New(name, nil, func(toTest interface{}) gomerr.Gomerr {
		ttv := reflect.ValueOf(toTest)
		if !ttv.IsValid() {
			if errorIfNil {
				return NotSatisfied(name[2:])
			}
			return nil
		}
		switch ttv.Kind() {
		case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
			if ttv.IsNil() == errorIfNil {
				return NotSatisfied(name[2:])
			}
			return nil
		default:
			return gomerr.Unprocessable("test value is not a nil-able type", reflect.TypeOf(toTest))
		}
	})
}

func Nil(value *interface{}) Constraint {
	return New("nil", value, func(interface{}) gomerr.Gomerr {
		return IsNil.Test(value)
	})
}

func NotNil(value *interface{}) Constraint {
	return New("notNil", value, func(interface{}) gomerr.Gomerr {
		return IsNotNil.Test(value)
	})
}

var (
	IsZero    = zeroConstraint("isZero", false)
	IsNotZero = zeroConstraint("isNotZero", true)
)

func zeroConstraint(name string, errorIfZero bool) Constraint {
	return New(name, nil, func(toTest interface{}) gomerr.Gomerr {
		ttv := reflect.ValueOf(toTest)
		if !ttv.IsValid() && errorIfZero {
			return NotSatisfied(name[2:])
		}
		if ttv.IsZero() == errorIfZero {
			return NotSatisfied(name[2:])
		}
		return nil
	})
}

func Zero(value *interface{}) Constraint {
	return New("zero", value, func(interface{}) gomerr.Gomerr {
		return IsZero.Test(*value)
	})
}

func NotZero(value *interface{}) Constraint {
	return New("notZero", value, func(interface{}) gomerr.Gomerr {
		return IsNotZero.Test(*value)
	})
}

var IsRequired = New("isRequired", nil, func(toTest interface{}) gomerr.Gomerr {
	ttv, ok := flect.ReadableIndirectValue(toTest)
	if !ok || ttv.IsZero() {
		return NotSatisfied(toTest)
	}
	return nil
})

func Required(value *interface{}) Constraint {
	return New("required", value, func(interface{}) gomerr.Gomerr {
		return IsRequired.Test(*value)
	})
}

var (
	IsTrue  = boolConstraint("isTrue", false)
	IsFalse = boolConstraint("isFalse", true)
)

func boolConstraint(name string, errorIfTrue bool) Constraint {
	return New(name, nil, func(toTest interface{}) gomerr.Gomerr {
		if ttv, ok := flect.ReadableIndirectValue(toTest); !ok {
			return NotSatisfied(name[2:]) // neither true nor false
		} else if ttv.Kind() != reflect.Bool {
			return gomerr.Unprocessable("test value is not a bool", reflect.TypeOf(toTest))
		} else if ttv.Bool() == errorIfTrue {
			return NotSatisfied(name[2:])
		}
		return nil
	})
}

func True(value *interface{}) Constraint {
	return New("true", value, func(interface{}) gomerr.Gomerr {
		return IsTrue.Test(*value)
	})
}

func False(value *interface{}) Constraint {
	return New("false", value, func(interface{}) gomerr.Gomerr {
		return IsFalse.Test(*value)
	})
}
