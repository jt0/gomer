package constraint

import (
	"fmt"
	"reflect"
)

// Length determines whether the value's length is either between (inclusively) two provided values (a min and max) or a
// single value (internally: min = max). This tests for min <= len(value) <= max. The value's type can be one of Array,
// Chan, Map, Slice, or String. Any other type will result in a false value from the constraint. If min is greater than
// max or min is less than 0, this will return a Fail() constraint.
func Length(values ...uint) Constraint {
	var min, max uint
	switch len(values) {
	case 1:
		min, max = values[0], values[0]
	case 2:
		min, max = values[0], values[1]
	default:
		return Fail(fmt.Sprintf("'Length' constraint requires 1 or 2 input values, received %d", len(values)))
	}

	return length(&min, &max)
}

// MinLength determines whether the value's length is greater than or equal to the min value provided.
// Stated explicitly, this tests for min <= len(value). The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MinLength(min uint) Constraint {
	return length(&min, nil)
}

// MaxLength determines whether the value's length is less than or equal to the max value provided.
// Stated explicitly, this tests for len(value) <= max. The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MaxLength(max uint) Constraint {
	return length(nil, &max)
}

var Empty = func() Constraint {
	c := MaxLength(0).(*constraint)
	c.type_ = "Empty"
	return c
}()

var NonEmpty = func() Constraint {
	c := MinLength(1).(*constraint)
	c.type_ = "NonEmpty"
	return c
}()

func length(min, max *uint) Constraint {
	var constraintType string
	var value interface{}
	if min != nil {
		if max != nil {
			if *min == *max {
				constraintType = "LengthEquals"
				value = *min
			} else if *min > *max {
				return Fail(fmt.Sprintf("'Length': min (%d) cannot be greater than max (%d)", *min, *max))
			} else {
				constraintType = "LengthBetween"
				value = []uint{*min, *max}
			}
		} else if *min < 0 {
			return Fail(fmt.Sprintf("'Length': min (%d) cannot be less than zero", *min))
		} else {
			constraintType = "LengthMin"
			value = *min
		}
	} else if max != nil {
		constraintType = "LengthMax"
		value = *max
	} else {
		return Fail("'Length' unexpectedly received two nil pointers")
	}

	return &constraint{constraintType, value, func(toTest interface{}) bool {
		ttv := reflect.ValueOf(toTest)
		if ttv.Kind() == reflect.Ptr {
			if ttv.IsNil() {
				return lenable(ttv.Type().Elem().Kind()) && min != nil && *min == 0
			}
			ttv = ttv.Elem()
		}

		if !lenable(ttv.Kind()) {
			return false
		}
		ttLen := uint(ttv.Len())

		return (min == nil || ttLen >= *min) && (max == nil || ttLen <= *max)
	}}
}

//goland:noinspection SpellCheckingInspection
func lenable(kind reflect.Kind) bool {
	return kind == reflect.Array || kind == reflect.Chan || kind == reflect.Map || kind == reflect.Slice || kind == reflect.String
}
