package constraint

import (
	"fmt"
	"reflect"
)

func Equals(compareTo interface{}) *constraint {
	return (&constraint{test: func(value interface{}) bool {
		return value == compareTo
	}}).setDetails("Equals", compareTo, LookupName, "equals")
}

func NotEquals(compareTo interface{}) *constraint {
	return (&constraint{test: func(value interface{}) bool {
		return value != compareTo
	}}).setDetails("NotEquals", compareTo, LookupName, "notequals")
}

func OneOf(compareTo ...interface{}) *constraint {
	return (&constraint{test: func(value interface{}) bool {
		for _, c := range compareTo {
			if value == c {
				return true
			}
		}

		return false
	}}).setDetails("OneOf", fmt.Sprintf("%v", compareTo), LookupName, "oneof")
}

func TypeOf(i interface{}) *constraint {
	t, ok := i.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(i)
	}

	return (&constraint{test: func(value interface{}) bool {
		return reflect.TypeOf(value) == t
	}}).setDetails("TypeOf", t.Name(), LookupName, "typeof")
}
