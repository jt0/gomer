package constraint

import (
	"reflect"
)

func Equals(value interface{}) Constraint {
	return &constraint{"Equals", value, func(toTest interface{}) bool {
		return value == toTest
	}}
}

func NotEquals(value interface{}) Constraint {
	return &constraint{"NotEquals", value, func(toTest interface{}) bool {
		return value != toTest
	}}
}

func OneOf(values ...interface{}) Constraint {
	return &constraint{"OneOf", values, func(toTest interface{}) bool {
		for _, value := range values {
			if toTest == value {
				return true
			}
		}
		return false
	}}
}

func TypeOf(value interface{}) Constraint {
	t, ok := value.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(value)
	}

	return &constraint{"TypeOf", value, func(toTest interface{}) bool {
		return reflect.TypeOf(toTest) == t
	}}
}
