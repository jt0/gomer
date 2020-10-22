package constraint

import (
	"fmt"
	"reflect"
)

// Length determines whether the value's length is (inclusively) between the min and max values provided.
// Stated explicitly, this tests for min <= len(value) <= max. The value's type can be one of Array,
// Chan, Map, Slice, or String. Any other type will result in a false value from the constraint. If min is > max,
// this is equivalent to calling Invalid().
func Length(min, max int) Constrainer {
	if min > max {
		return invalid().setDetails("Replaced", "true", "Reason", fmt.Sprintf("Length(%d, %d) invalid, min > max", min, max))
	}

	return length(&min, &max, "len")
}

// MinLength determines whether the value's length is greater than or equal to the min value provided.
// Stated explicitly, this tests for min <= len(value). The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MinLength(min int) Constrainer {
	return length(&min, nil, "minlen")
}

// MaxLength determines whether the value's length is less than or equal to the max value provided.
// Stated explicitly, this tests for len(value) <= max. The value's type can be one of Array, Chan,
// Map, Slice, or String. Any other type will result in a false value from the constraint.
func MaxLength(max int) Constrainer {
	return length(nil, &max, "maxlen")
}

var Empty = MaxLength(0)

func length(min, max *int, lookupName string) Constrainer {
	details := make([]interface{}, 0, 4)
	if min != nil {
		details = append(details, "MinLength", *min)
	}
	if max != nil {
		details = append(details, "MaxLength", *max)
	}
	details = append(details, LookupName, lookupName)

	return Constrainer{test: func(value interface{}) bool {
		vv := reflect.ValueOf(value)

		switch vv.Kind() {
		case reflect.Ptr:
			return vv.IsNil() && min != nil && *min == 0 // len(nil) == 0
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice, reflect.String:
			length := vv.Len()
			return (min == nil || length >= *min) && (max == nil || length <= *max)
		default:
			return false
		}
	}}.setDetails(details...)
}
