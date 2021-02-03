package constraint

import (
	"fmt"
	"reflect"
)

// Length determines whether the value's length is either between (inclusively) two provided values (a min and max) or a
// single value (internally: min = max). This tests for min <= len(value) <= max. The value's type can be one of Array,
// Chan, Map, Slice, or String. Any other type will result in a false value from the constraint. If min is greater than
// max or min is less than 0, this is equivalent to calling Invalid().
func Length(values ...int) *constraint {
	var min, max int
	switch len(values) {
	case 1:
		min, max = values[0], values[0]
	case 2:
		min, max = values[0], values[1]
	default:
		return invalid().setDetails("Replaced", "true", "Reason", fmt.Sprintf("Length requires 1 or 2 input values, received %d", len(values)))
	}

	if min < 0 {
		return invalid().setDetails("Replaced", "true", "Reason", fmt.Sprintf("Length values must be non-negative, received %d", min))
	} else if min > max {
		return invalid().setDetails("Replaced", "true", "Reason", fmt.Sprintf("Length(%d, %d) invalid, min > max", min, max))
	}

	return length(&min, &max, "len")
}

// MinLength determines whether the value's length is greater than or equal to the min value provided.
// Stated explicitly, this tests for min <= len(value). The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MinLength(min int) *constraint {
	return length(&min, nil, "minlen")
}

// MaxLength determines whether the value's length is less than or equal to the max value provided.
// Stated explicitly, this tests for len(value) <= max. The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MaxLength(max int) *constraint {
	return length(nil, &max, "maxlen")
}

var Empty = MaxLength(0)

func length(min, max *int, lookupName string) *constraint {
	details := []interface{}{LookupName, lookupName}

	if min != nil && min == max {
		details = append(details, "Length", *min)
	} else {
		if min != nil {
			details = append(details, "MinLength", *min)
		}
		if max != nil {
			details = append(details, "MaxLength", *max)
		}
	}

	return (&constraint{test: func(value interface{}) bool {
		vv := reflect.ValueOf(value)

		if vv.Kind() == reflect.Ptr {
			if vv.IsNil() {
				return lenable(vv.Type().Elem().Kind()) && min != nil && *min == 0
			}

			vv = vv.Elem()
		}

		if lenable(vv.Kind()) {
			length := vv.Len()
			return (min == nil || length >= *min) && (max == nil || length <= *max)
		}

		return false
	}}).setDetails(details...)
}

//goland:noinspection SpellCheckingInspection
func lenable(kind reflect.Kind) bool {
	return kind == reflect.Array || kind == reflect.Chan || kind == reflect.Map || kind == reflect.Slice || kind == reflect.String
}
