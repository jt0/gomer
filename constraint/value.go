package constraint

import (
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

// valueConstraint builds a constraint that reads its input from value at test time rather than from
// the argument passed to Test, which is what the dynamic ($.Field) constraint forms need.
//
// Note that it takes the predicate itself rather than an equivalent constraint to delegate to.
// Delegating would run the inner constraint's Test, which stamps its own name into the
// NotSatisfiedError, and constraint.Test then preserves that innermost name - so notzero($.Other)
// would report whatever IsNotZero is named rather than its own name. Sharing the predicate removes
// that dependency. See TestReportedConstraintName.
func valueConstraint(name string, value *any, test func(toTest any) gomerr.Gomerr) Constraint {
	return New(name, value, func(any) gomerr.Gomerr {
		return test(*value)
	})
}

var (
	IsNil    = New("nil", nil, nilTest(false))
	IsNotNil = New("notNil", nil, nilTest(true))
)

// nilTest builds the predicate behind the nil/notNil constraints and their Nil/NotNil dynamic
// forms. It reads the reference and never dereferences it, so a pointer to a zero value is not
// nil. Compare zeroTest, which does dereference.
func nilTest(errorIfNil bool) func(toTest any) gomerr.Gomerr {
	return func(toTest any) gomerr.Gomerr {
		ttv := reflect.ValueOf(toTest)
		if !ttv.IsValid() {
			if errorIfNil {
				return NotSatisfied(nil)
			}
			return nil
		}
		switch ttv.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
			if ttv.IsNil() == errorIfNil {
				return NotSatisfied(nil)
			}
			return nil
		default:
			return gomerr.Unprocessable("test value is not a nil-able type", reflect.TypeOf(toTest))
		}
	}
}

func Nil(value *any) Constraint {
	return valueConstraint("nil", value, nilTest(false))
}

func NotNil(value *any) Constraint {
	return valueConstraint("notNil", value, nilTest(true))
}

var (
	IsZero    = New("zero", nil, zeroTest(false))
	IsNotZero = New("notZero", nil, zeroTest(true))
)

// zeroTest builds the predicate behind the isZero/isNotZero constraints and their Zero/NotZero
// dynamic forms. Compare nilTest: this one looks through pointers to ask about the value itself.
func zeroTest(errorIfZero bool) func(toTest any) gomerr.Gomerr {
	return func(toTest any) gomerr.Gomerr {
		if isZeroValue(toTest) == errorIfZero {
			return NotSatisfied(nil)
		}
		return nil
	}
}

// isZeroValue reports whether toTest holds no readable value (an untyped nil or a nil pointer) or
// the value it dereferences to is the zero value for its type. Unlike nilTest, which asks whether a
// reference itself is nil, this looks through pointers: a non-nil pointer to a zero value is zero.
func isZeroValue(toTest any) bool {
	ttv, ok := flect.ReadableIndirectValue(toTest)
	return !ok || ttv.IsZero()
}

func Zero(value *any) Constraint {
	return valueConstraint("zero", value, zeroTest(false))
}

func NotZero(value *any) Constraint {
	return valueConstraint("notZero", value, zeroTest(true))
}

// IsRequired is an alias for IsNotZero, reported under its own name. Note that it is therefore not
// a presence check: a zero-but-meaningful value (0, false, "") does not satisfy it. To require only
// that a value was supplied, leaving zero values valid, use IsNotNil on a pointer field.
var IsRequired = New("required", nil, zeroTest(true))

func Required(value *any) Constraint {
	return valueConstraint("required", value, zeroTest(true))
}

var (
	IsTrue  = New("true", nil, boolTest(false))
	IsFalse = New("false", nil, boolTest(true))
)

// boolTest builds the predicate behind the isTrue/isFalse constraints and their True/False dynamic
// forms. A value that is neither (because there is nothing to read) satisfies neither.
func boolTest(errorIfTrue bool) func(toTest any) gomerr.Gomerr {
	return func(toTest any) gomerr.Gomerr {
		if ttv, ok := flect.ReadableIndirectValue(toTest); !ok {
			return NotSatisfied(nil) // neither true nor false
		} else if ttv.Kind() != reflect.Bool {
			return gomerr.Unprocessable("test value is not a bool", reflect.TypeOf(toTest))
		} else if ttv.Bool() == errorIfTrue {
			return NotSatisfied(nil)
		}
		return nil
	}
}

func True(value *any) Constraint {
	return valueConstraint("true", value, boolTest(false))
}

func False(value *any) Constraint {
	return valueConstraint("false", value, boolTest(true))
}
