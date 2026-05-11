package constraint

import (
	"reflect"
	"strings"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

// Field compares a struct field's value against either a static value or another field's value
// using a standard comparison operator. Both the field and compareTo parameters accept dynamic
// references ($.FieldName) or static values.
//
// Supported comparison operators: eq, neq, gt, gte, lt, lte
// Supported types: strings (including typedefs), integers, unsigned integers, floats, and bools.
//
// Usage in struct tags:
//
//	// CreateMode is required when AccessMode is "read_write"
//	CreateMode *string `validate:"or(field($.AccessMode,neq,read_write),required)"`
//
//	// Max must be >= Min (field-to-field comparison)
//	Max int `validate:"field($.Min,lte,$.Max)"`
//
//	// Name is required when Enabled is true
//	Name string `validate:"or(field($.Enabled,eq,false),required)"`
func Field(field *any, comparisonType ComparisonType, compareTo *any) Constraint {
	comparisonType = strings.ToLower(comparisonType)
	comparator, ok := reflectComparators[comparisonType]
	if !ok {
		return ConfigurationError("unrecognized comparison type: " + comparisonType)
	}

	return New("fieldTest_"+comparisonType, []any{field, compareTo}, func(any) gomerr.Gomerr {
		fv, fOk := flect.ReadableIndirectValue(*field)
		cv, cOk := flect.ReadableIndirectValue(*compareTo)

		if !fOk && !cOk {
			if comparisonType == EQ {
				return nil
			}
			return NotSatisfied(*field)
		}
		if !fOk || !cOk {
			if comparisonType == NEQ {
				return nil
			}
			return NotSatisfied(*field)
		}

		if fv.Type() != cv.Type() {
			var ge gomerr.Gomerr
			cv, ge = coerceToType(fv, cv)
			if ge != nil {
				return ge
			}
		}

		if !comparator(fv, cv) {
			return NotSatisfied(*field)
		}
		return nil
	})
}

var reflectComparators = map[ComparisonType]func(a, b reflect.Value) bool{
	EQ:  func(a, b reflect.Value) bool { return a.Interface() == b.Interface() },
	NEQ: func(a, b reflect.Value) bool { return a.Interface() != b.Interface() },
	GT:  func(a, b reflect.Value) bool { return compareOrdered(a, b) > 0 },
	GTE: func(a, b reflect.Value) bool { return compareOrdered(a, b) >= 0 },
	LT:  func(a, b reflect.Value) bool { return compareOrdered(a, b) < 0 },
	LTE: func(a, b reflect.Value) bool { return compareOrdered(a, b) <= 0 },
}

func compareOrdered(a, b reflect.Value) int {
	switch a.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		av, bv := a.Int(), b.Int()
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		av, bv := a.Uint(), b.Uint()
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case reflect.Float32, reflect.Float64:
		av, bv := a.Float(), b.Float()
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case reflect.String:
		av, bv := a.String(), b.String()
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	default:
		return 0
	}
}
