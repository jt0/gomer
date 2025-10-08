package constraint

import (
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

func Equals(value interface{}) Constraint {
	return New("equals", value, func(toTest interface{}) gomerr.Gomerr {
		if tt, ok := flect.IndirectInterface(toTest); !ok || tt != value {
			return NotSatisfied(tt)
		}
		return nil
	})
}

func NotEquals(value interface{}) Constraint {
	return New("notEquals", value, func(toTest interface{}) gomerr.Gomerr {
		if tt, ok := flect.IndirectInterface(toTest); !ok || tt == value {
			return NotSatisfied(tt)
		}
		return nil
	})
}

func OneOf(values ...interface{}) Constraint {
	if len(values) == 0 {
		panic(gomerr.Configuration("oneOf constraint defined without values"))
	}
	valuesType := reflect.TypeOf(values[0])

	return New("oneOf", values, func(toTest interface{}) gomerr.Gomerr {
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
