package constraint

import (
	"reflect"
	"strings"
	"time"

	"github.com/jt0/gomer/gomerr"
)

type ComparisonType = string

const (
	EQ  ComparisonType = "EQ"
	NEQ                = "NEQ"
	GT                 = "GT"
	GTE                = "GTE"
	LT                 = "LT"
	LTE                = "LTE"
)

func Gte(ft reflect.Type, compareTo *interface{}) {

}

// IntCompare compares the tested value to compareTo. While compareTo is an int64, the tested value can be any of the
// integer types (e.g. int, int16, etc). If the tested value is not an integer type, the constraint will fail.
func IntCompare(comparisonType ComparisonType, compareTo *int64) Constraint {
	comparisonType = strings.ToUpper(comparisonType)
	comparator, ok := intComparators[comparisonType]
	if !ok {
		panic("Unrecognized comparison type: " + comparisonType)
	}

	return New("Int"+comparisonType, compareTo, func(toTest interface{}) (ge gomerr.Gomerr) {
		if compareTo == nil {
			return nil
		}

		ttv, isNil := indirectValueOf(toTest)
		if isNil {
			return nil // should be NotSatisfied?
		}

		defer func() {
			if r := recover(); r != nil {
				ge = gomerr.Unprocessable("toTest is not an int (or *int)", toTest)
			}
		}()

		if !comparator(ttv.Int(), *compareTo) {
			return NotSatisfied(toTest)
		}

		return nil
	})
}

// IntBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func IntBetween(lower, upper *int64) Constraint {
	return And(IntCompare(GTE, lower), IntCompare(LTE, upper))
}

var intComparators = map[ComparisonType]func(int64, int64) bool{
	EQ:  func(value, compareTo int64) bool { return value == compareTo },
	NEQ: func(value, compareTo int64) bool { return value != compareTo },
	GT:  func(value, compareTo int64) bool { return value > compareTo },
	GTE: func(value, compareTo int64) bool { return value >= compareTo },
	LT:  func(value, compareTo int64) bool { return value < compareTo },
	LTE: func(value, compareTo int64) bool { return value <= compareTo },
}

// UintCompare compares a tested value to compareTo. While compareTo is an uint64, the tested value can be any of the
// unsigned integer types (e.g. uint, uint16, etc). If the tested value is not an unsigned integer type, the constraint
// will fail.
func UintCompare(comparisonType ComparisonType, compareTo *uint64) Constraint {
	comparisonType = strings.ToUpper(comparisonType)
	comparator, ok := uintComparators[comparisonType]
	if !ok {
		panic("Unrecognized comparison type: " + comparisonType)
	}

	return New("Uint"+comparisonType, compareTo, func(toTest interface{}) (ge gomerr.Gomerr) {
		if compareTo == nil {
			return nil
		}

		ttv, isNil := indirectValueOf(toTest)
		if isNil {
			return nil // should be NotSatisfied?
		}

		defer func() {
			if r := recover(); r != nil {
				ge = gomerr.Unprocessable("toTest is not a uint (or *uint)", toTest)
			}
		}()

		if !comparator(ttv.Uint(), *compareTo) {
			return NotSatisfied(toTest)
		}

		return nil
	})
}

// UintBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func UintBetween(lower, upper *uint64) Constraint {
	return And(UintCompare(GTE, lower), UintCompare(LTE, upper))
}

var uintComparators = map[ComparisonType]func(uint64, uint64) bool{
	EQ:  func(value, compareTo uint64) bool { return value == compareTo },
	NEQ: func(value, compareTo uint64) bool { return value != compareTo },
	GT:  func(value, compareTo uint64) bool { return value > compareTo },
	GTE: func(value, compareTo uint64) bool { return value >= compareTo },
	LT:  func(value, compareTo uint64) bool { return value < compareTo },
	LTE: func(value, compareTo uint64) bool { return value <= compareTo },
}

// FloatCompare compares a tested value to compareTo. While compareTo is an float64, the tested value can be either
// float32/float64. If the value is not a float type, the constraint will fail.
func FloatCompare(comparisonType ComparisonType, compareTo *float64) Constraint {
	comparisonType = strings.ToUpper(comparisonType)
	comparator, ok := floatComparators[comparisonType]
	if !ok {
		panic("Unrecognized comparison type: " + comparisonType)
	}

	return New("Float"+comparisonType, compareTo, func(toTest interface{}) (ge gomerr.Gomerr) {
		if compareTo == nil {
			return nil
		}

		ttv, isNil := indirectValueOf(toTest)
		if isNil {
			return nil // should be NotSatisfied?
		}

		defer func() {
			if r := recover(); r != nil {
				ge = gomerr.Unprocessable("toTest is not a float (or *float)", toTest)
			}
		}()

		if !comparator(ttv.Float(), *compareTo) {
			return NotSatisfied(toTest)
		}

		return nil
	})
}

// FloatBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func FloatBetween(lower, upper *float64) Constraint {
	return And(FloatCompare(GTE, lower), FloatCompare(LTE, upper))
}

var floatComparators = map[ComparisonType]func(float64, float64) bool{
	EQ:  func(value, compareTo float64) bool { return value == compareTo },
	NEQ: func(value, compareTo float64) bool { return value != compareTo },
	GT:  func(value, compareTo float64) bool { return value > compareTo },
	GTE: func(value, compareTo float64) bool { return value >= compareTo },
	LT:  func(value, compareTo float64) bool { return value < compareTo },
	LTE: func(value, compareTo float64) bool { return value <= compareTo },
}

// TimeCompare compares a tested value to compareTo. If the tested value is not a time.Time, the constraint will fail.
func TimeCompare(comparisonType ComparisonType, compareTo *time.Time) Constraint {
	comparisonType = strings.ToUpper(comparisonType)
	comparator, ok := timeComparators[comparisonType]
	if !ok {
		panic("Unrecognized comparison type: " + comparisonType)
	}

	return New("Time"+comparisonType, compareTo, func(toTest interface{}) (ge gomerr.Gomerr) {
		if compareTo == nil {
			return nil
		}

		ttv, isNil := indirectValueOf(toTest)
		if isNil {
			return nil // should be NotSatisfied?
		}

		defer func() {
			if r := recover(); r != nil {
				ge = gomerr.Unprocessable("toTest is not a time.Time (or *time.Time)", toTest)
			}
		}()

		if !comparator(ttv.Interface().(time.Time), *compareTo) {
			return NotSatisfied(toTest)
		}

		return nil
	})
}

// TimeBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func TimeBetween(lower, upper *time.Time) Constraint {
	return And(TimeCompare(GTE, lower), TimeCompare(LTE, upper))
}

var timeComparators = map[ComparisonType]func(time.Time, time.Time) bool{
	EQ:  func(value, compareTo time.Time) bool { return value.Equal(compareTo) },
	NEQ: func(value, compareTo time.Time) bool { return !value.Equal(compareTo) },
	GT:  func(value, compareTo time.Time) bool { return value.After(compareTo) },
	GTE: func(value, compareTo time.Time) bool { return value.After(compareTo) || value.Equal(compareTo) },
	LT:  func(value, compareTo time.Time) bool { return value.Before(compareTo) },
	LTE: func(value, compareTo time.Time) bool { return value.Before(compareTo) || value.Equal(compareTo) },
}
