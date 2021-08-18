package constraint

import (
	"fmt"
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

// Length determines whether the value's length is either between (inclusively) two provided values (a min and max) or a
// single value (internally: min = max). This tests for min <= len(value) <= max. The value's type can be one of Array,
// Chan, Map, Slice, or String. Any other type will result in a false value from the constraint. If min is greater than
// max or min is less than 0, this will return a Fail() constraint.
func Length(values ...*uint64) Constraint {
	switch len(values) {
	case 1:
		return length("LengthEquals", *values[0], values[0], values[0])
	case 2:
		return length("LengthBetween", []interface{}{*values[0], *values[1]}, values[0], values[1])
	default:
		return Fail(fmt.Sprintf("'Length' constraint requires 1 or 2 input values, received %d", len(values)))
	}
}

// MinLength determines whether the value's length is greater than or equal to the min value provided.
// Stated explicitly, this tests for min <= len(value). The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MinLength(min *uint64) Constraint {
	return length("LengthMin", min, min, nil)
}

// MaxLength determines whether the value's length is less than or equal to the max value provided.
// Stated explicitly, this tests for len(value) <= max. The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MaxLength(max *uint64) Constraint {
	return length("LengthMax", max, nil, max)
}

var (
	zero = uint64(0)
	one  = uint64(1)

	Empty    = length("Empty", nil, nil, &zero)
	NonEmpty = length("NonEmpty", nil, &one, nil)

	zeroLength = reflect.ValueOf([]interface{}{})
)

func length(lengthType string, lengthParams interface{}, min, max *uint64) Constraint {
	return New(lengthType, lengthParams, func(toTest interface{}) gomerr.Gomerr {
		ttv, isNil := indirectValueOf(toTest)
		if isNil {
			ttv = zeroLength
		}

		if !lenable(ttv.Kind()) {
			return gomerr.Unprocessable("Test value must be one of Array, Chan, Map, Slice, or String (or pointer to one of these)", toTest)
		}

		ttLen := uint64(ttv.Len())
		if !(min == nil || ttLen >= *min) || !(max == nil || ttLen <= *max) {
			return NotSatisfied(toTest)
		}

		return nil
	})
}

func lenable(kind reflect.Kind) bool {
	return kind == reflect.Array || kind == reflect.Chan || kind == reflect.Map || kind == reflect.Slice || kind == reflect.String
}
