package constraint

import (
	"fmt"
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

// Length determines whether the value's length is either between (inclusively) two provided values (a min and max) or a
// single value (internally: min = max). This tests for min <= len(value) <= max. The value's type can be one of Array,
// Chan, Map, Slice, or String. Any other type will result in a false value from the constraint. If min is greater than
// max or min is less than 0, this will return a Fail() constraint.
func Length(values ...*uint64) Constraint {
	switch len(values) {
	case 1:
		return length("lengthEquals", *values[0], values[0], values[0])
	case 2:
		if values[0] != nil {
			if values[1] != nil {
				return length("lengthBetween", []any{*values[0], *values[1]}, values[0], values[1])
			} else {
				return MinLength(values[0])
			}
		} else if values[1] != nil {
			return MaxLength(values[1])
		}
		fallthrough
	default:
		return ConfigurationError(fmt.Sprintf("'length' constraint requires 1 or 2 non-nil input values, received %d", len(values)))
	}
}

// MinLength determines whether the value's length is greater than or equal to the min value provided.
// Stated explicitly, this tests for min <= len(value). The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MinLength(min *uint64) Constraint {
	return length("lengthMin", min, min, nil)
}

// MaxLength determines whether the value's length is less than or equal to the max value provided.
// Stated explicitly, this tests for len(value) <= max. The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MaxLength(max *uint64) Constraint {
	return length("lengthMax", max, nil, max)
}

var (
	uintZero = uint64(0)
	uintOne  = uint64(1)

	Empty    = length("empty", nil, nil, &uintZero)
	NonEmpty = length("nonEmpty", nil, &uintOne, nil)

	zeroLength = reflect.ValueOf([]any{})
)

func length(lengthType string, lengthParams any, min, max *uint64) Constraint {
	return New(lengthType, lengthParams, func(toTest any) gomerr.Gomerr {
		ttv, ok := flect.ReadableIndirectValue(toTest)
		if !ok {
			ttv = zeroLength
		}

		if !lenable(ttv.Kind()) {
			return gomerr.Unprocessable("test value must be one of Array, Chan, Map, Slice, or String (or pointer to one of these)", toTest)
		}

		ttLen := uint64(ttv.Len())
		if !(min == nil || ttLen >= *min) || !(max == nil || ttLen <= *max) {
			return NotSatisfied(ttv.Len())
		}

		return nil
	})
}

func lenable(kind reflect.Kind) bool {
	return kind == reflect.Array || kind == reflect.Chan || kind == reflect.Map || kind == reflect.Slice || kind == reflect.String
}
