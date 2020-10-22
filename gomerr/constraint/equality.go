package constraint

import (
	"fmt"
	"reflect"
)

func Equals(compareTo interface{}) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return value == value
	}}.setDetails("Equals", compareTo, LookupName, "equals")
}

func OneOf(compareTo ...interface{}) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		for _, c := range compareTo {
			if value == c {
				return true
			}
		}

		return false
	}}.setDetails("OneOf", fmt.Sprintf("%v", compareTo), LookupName, "oneof")
}

func TypeOf(i interface{}) Constrainer {
	t, ok := i.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(i)
	}

	return Constrainer{test: func(value interface{}) bool {
		return reflect.TypeOf(value) == t
	}}.setDetails("TypeOf", t.Name(), LookupName, "typeof")
}
