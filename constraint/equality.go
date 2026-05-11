package constraint

import (
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

func Equals(value *any) Constraint {
	return New("equals", value, func(toTest any) gomerr.Gomerr {
		tt, ok := flect.IndirectInterface(toTest)
		if !ok {
			return NotSatisfied(tt)
		}
		compareTo := *value
		if !equalCoerced(tt, compareTo) {
			return NotSatisfied(tt)
		}
		return nil
	})
}

func NotEquals(value *any) Constraint {
	return New("notEquals", value, func(toTest any) gomerr.Gomerr {
		tt, ok := flect.IndirectInterface(toTest)
		if !ok || equalCoerced(tt, *value) {
			return NotSatisfied(tt)
		}
		return nil
	})
}

// equalCoerced compares two values, coercing b to a's type if they differ.
func equalCoerced(a, b any) bool {
	av, aOk := flect.ReadableIndirectValue(a)
	bv, bOk := flect.ReadableIndirectValue(b)
	if !aOk || !bOk {
		return !aOk && !bOk
	}
	bv, _ = coerceToType(av, bv)
	return av.Interface() == bv.Interface()
}

// coerceToType coerces b to match a's type using flect.SetValue. Returns the coerced value
// and any error. If types already match, b is returned unchanged.
func coerceToType(a, b reflect.Value) (reflect.Value, gomerr.Gomerr) {
	if a.Type() == b.Type() {
		return b, nil
	}
	coerced := reflect.New(a.Type()).Elem()
	if ge := flect.SetValue(coerced, b.Interface()); ge != nil {
		return b, ge
	}
	return coerced, nil
}

func OneOf(values ...any) Constraint {
	if len(values) == 0 {
		return ConfigurationError("oneOf constraint defined without values")
	}
	valuesType := reflect.TypeOf(values[0])

	return New("oneOf", values, func(toTest any) gomerr.Gomerr {
		if ttv, ok := flect.ReadableIndirectValue(toTest); !ok {
			return NotSatisfied(toTest)
		} else if !ttv.CanConvert(valuesType) {
			return NotSatisfied(toTest)
		} else {
			tti := ttv.Convert(valuesType).Interface()
			for _, value := range values {
				if tti == value {
					return nil
				}
			}
			return NotSatisfied(tti)
		}
	})
}
