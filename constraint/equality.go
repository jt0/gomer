package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

func Equals(value interface{}) Constraint {
	return New("Equals", value, func(toTest interface{}) gomerr.Gomerr {
		tt, isNil := indirect(toTest)
		if isNil || tt != value {
			return NotSatisfied(toTest)
		}
		return nil
	})
}

func NotEquals(value interface{}) Constraint {
	return New("NotEquals", value, func(toTest interface{}) gomerr.Gomerr {
		tt, isNil := indirect(toTest)
		if isNil || tt == value {
			return NotSatisfied(toTest)
		}
		return nil
	})
}

func OneOf(values ...interface{}) Constraint {
	return New("OneOf", values, func(toTest interface{}) gomerr.Gomerr {
		tt, isNil := indirect(toTest)
		if !isNil {
			for _, value := range values {
				if tt == value {
					return nil
				}
			}
		}
		return NotSatisfied(toTest)
	})
}
