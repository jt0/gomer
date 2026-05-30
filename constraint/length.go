package constraint

import (
	"fmt"
	"reflect"
	"strconv"

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
		return And(MinLength(values[0]), MaxLength(values[0]))
	case 2:
		if values[0] != nil {
			if values[1] != nil {
				return And(MinLength(values[0]), MaxLength(values[1]))
			}
			return MinLength(values[0])
		} else if values[1] != nil {
			return MaxLength(values[1])
		}
		fallthrough
	default:
		return ConfigurationError(fmt.Sprintf("'length' constraint requires 1 or 2 non-nil input values, received %d", len(values)))
	}
}

// MinLength determines whether the value's length is greater than or equal to the min value provided.
func MinLength(min *uint64) Constraint {
	return New("minLength", min, func(toTest any) gomerr.Gomerr {
		ttv, ok := flect.ReadableIndirectValue(toTest)
		if !ok {
			ttv = zeroLength
		}
		if !lenable(ttv.Kind()) {
			return gomerr.Unprocessable("test value must be one of Array, Chan, Map, Slice, or String (or pointer to one of these)", toTest)
		}
		if min != nil && uint64(ttv.Len()) < *min {
			return NotSatisfied(ttv.Len()).AddAttributes("constraint", "minLength", "expected", strconv.FormatUint(*min, 10))
		}
		return nil
	})
}

// MaxLength determines whether the value's length is less than or equal to the max value provided.
func MaxLength(max *uint64) Constraint {
	return New("maxLength", max, func(toTest any) gomerr.Gomerr {
		ttv, ok := flect.ReadableIndirectValue(toTest)
		if !ok {
			ttv = zeroLength
		}
		if !lenable(ttv.Kind()) {
			return gomerr.Unprocessable("test value must be one of Array, Chan, Map, Slice, or String (or pointer to one of these)", toTest)
		}
		if max != nil && uint64(ttv.Len()) > *max {
			return NotSatisfied(ttv.Len()).AddAttributes("constraint", "maxLength", "expected", strconv.FormatUint(*max, 10))
		}
		return nil
	})
}

var (
	uintZero = uint64(0)
	uintOne  = uint64(1)

	Empty    = MaxLength(&uintZero)
	NonEmpty = MinLength(&uintOne)

	zeroLength = reflect.ValueOf([]any{})
)

func lenable(kind reflect.Kind) bool {
	return kind == reflect.Array || kind == reflect.Chan || kind == reflect.Map || kind == reflect.Slice || kind == reflect.String
}
