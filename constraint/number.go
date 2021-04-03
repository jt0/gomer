package constraint

import (
	"reflect"
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

// IntCompare compares the tested value to compareTo. While compareTo is an int64, the
// tested value can be any of the integer types (e.g. int, int16, etc). If the tested value is not
// an integer type, the constraint will return false.
func IntCompare(comparisonType ComparisonType, compareTo int64) *constraint {
	return &constraint{"Int_" + comparisonType, compareTo, func(toTest interface{}) bool {
		switch tt := toTest.(type) {
		case int:
			return intComparators[comparisonType](int64(tt), compareTo)
		case int8:
			return intComparators[comparisonType](int64(tt), compareTo)
		case int16:
			return intComparators[comparisonType](int64(tt), compareTo)
		case int32:
			return intComparators[comparisonType](int64(tt), compareTo)
		case int64:
			return intComparators[comparisonType](tt, compareTo)
		default:
			ttv := reflect.ValueOf(toTest) // ignore nil; can be marked as required if needed
			return ttv.Kind() == reflect.Ptr && ttv.IsNil()
		}
	}}
}

// IntBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func IntBetween(lower, upper int64) *constraint {
	return And(IntCompare(GTE, lower), IntCompare(LTE, upper)).(*constraint)
}

var intComparators = map[ComparisonType]func(int64, int64) bool{
	EQ:  func(value, compareTo int64) bool { return value == compareTo },
	NEQ: func(value, compareTo int64) bool { return value != compareTo },
	GT:  func(value, compareTo int64) bool { return value > compareTo },
	GTE: func(value, compareTo int64) bool { return value >= compareTo },
	LT:  func(value, compareTo int64) bool { return value < compareTo },
	LTE: func(value, compareTo int64) bool { return value <= compareTo },
}

// UintCompare compares a tested value to compareTo. While compareTo is an uint64, the
// tested value can be any of the unsigned integer types (e.g. uint, uint16, etc). If the tested value
// is not an unsigned integer type, the constraint will return false.
func UintCompare(comparisonType ComparisonType, compareTo uint64) *constraint {
	return &constraint{"Uint_" + comparisonType, compareTo, func(toTest interface{}) bool {
		switch tt := toTest.(type) {
		case uint:
			return uintComparators[comparisonType](uint64(tt), compareTo)
		case uint8:
			return uintComparators[comparisonType](uint64(tt), compareTo)
		case uint16:
			return uintComparators[comparisonType](uint64(tt), compareTo)
		case uint32:
			return uintComparators[comparisonType](uint64(tt), compareTo)
		case uint64:
			return uintComparators[comparisonType](tt, compareTo)
		default:
			ttv := reflect.ValueOf(toTest) // ignore nil; can be marked as required if needed
			return ttv.Kind() == reflect.Ptr && ttv.IsNil()
		}
	}}
}

// UintBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func UintBetween(lower, upper uint64) *constraint {
	return And(UintCompare(GTE, lower), UintCompare(LTE, upper)).(*constraint)
}

var uintComparators = map[ComparisonType]func(uint64, uint64) bool{
	EQ:  func(value, compareTo uint64) bool { return value == compareTo },
	NEQ: func(value, compareTo uint64) bool { return value != compareTo },
	GT:  func(value, compareTo uint64) bool { return value > compareTo },
	GTE: func(value, compareTo uint64) bool { return value >= compareTo },
	LT:  func(value, compareTo uint64) bool { return value < compareTo },
	LTE: func(value, compareTo uint64) bool { return value <= compareTo },
}

// FloatCompare compares a tested value to compareTo. While compareTo is an float64, the
// tested value can be either float32/float64. If the value is not a float type, the constraint will return false.
func FloatCompare(comparisonType ComparisonType, compareTo float64) *constraint {
	return &constraint{"Float_" + comparisonType, compareTo, func(toTest interface{}) bool {
		switch tt := toTest.(type) {
		case float32:
			return floatComparators[comparisonType](float64(tt), compareTo)
		case float64:
			return floatComparators[comparisonType](tt, compareTo)
		default:
			ttv := reflect.ValueOf(toTest) // ignore nil; can be marked as required if needed
			return ttv.Kind() == reflect.Ptr && ttv.IsNil()
		}
	}}
}

// FloatBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func FloatBetween(lower, upper float64) *constraint {
	return And(FloatCompare(GTE, lower), FloatCompare(LTE, upper)).(*constraint)
}

var floatComparators = map[ComparisonType]func(float64, float64) bool{
	EQ:  func(value, compareTo float64) bool { return value == compareTo },
	NEQ: func(value, compareTo float64) bool { return value != compareTo },
	GT:  func(value, compareTo float64) bool { return value > compareTo },
	GTE: func(value, compareTo float64) bool { return value >= compareTo },
	LT:  func(value, compareTo float64) bool { return value < compareTo },
	LTE: func(value, compareTo float64) bool { return value <= compareTo },
}
