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
	return (&constraint{test: func(value interface{}) bool {
		switch vt := value.(type) {
		case int:
			return intComparators[comparisonType](int64(vt), compareTo)
		case int8:
			return intComparators[comparisonType](int64(vt), compareTo)
		case int16:
			return intComparators[comparisonType](int64(vt), compareTo)
		case int32:
			return intComparators[comparisonType](int64(vt), compareTo)
		case int64:
			return intComparators[comparisonType](vt, compareTo)
		default:
			vv := reflect.ValueOf(value) // ignore nil; can be marked as required if needed
			return vv.Kind() == reflect.Ptr && vv.IsNil()
		}
	}}).setDetails(comparisonType, compareTo, TagStructName, "int")
}

// IntBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func IntBetween(lower, upper int64) *constraint {
	return And(IntCompare(GTE, lower), IntCompare(LTE, upper)).(*constraint).setDetails(TagStructName, "intbetween")
}

var intComparators = map[ComparisonType]func(value, compareTo int64) bool{
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
	return (&constraint{test: func(value interface{}) bool {
		switch vt := value.(type) {
		case uint:
			return uintComparators[comparisonType](uint64(vt), compareTo)
		case uint8:
			return uintComparators[comparisonType](uint64(vt), compareTo)
		case uint16:
			return uintComparators[comparisonType](uint64(vt), compareTo)
		case uint32:
			return uintComparators[comparisonType](uint64(vt), compareTo)
		case uint64:
			return uintComparators[comparisonType](vt, compareTo)
		default:
			vv := reflect.ValueOf(value) // ignore nil; can be marked as required if needed
			return vv.Kind() == reflect.Ptr && vv.IsNil()
		}
	}}).setDetails(comparisonType, compareTo, TagStructName, "uint")
}

// UintBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func UintBetween(lower, upper uint64) *constraint {
	return And(UintCompare(GTE, lower), UintCompare(LTE, upper)).(*constraint).setDetails(TagStructName, "uintbetween")
}

var uintComparators = map[ComparisonType]func(value, compareTo uint64) bool{
	EQ:  func(value, compareTo uint64) bool { return value == compareTo },
	NEQ: func(value, compareTo uint64) bool { return value != compareTo },
	GT:  func(value, compareTo uint64) bool { return value > compareTo },
	GTE: func(value, compareTo uint64) bool { return value >= compareTo },
	LT:  func(value, compareTo uint64) bool { return value < compareTo },
	LTE: func(value, compareTo uint64) bool { return value <= compareTo },
}

func ByteCompare(comparisonType ComparisonType, compareTo byte) *constraint {
	return UintCompare(comparisonType, uint64(compareTo)).setDetails(TagStructName, "byte")
}

func ByteBetween(lower, upper byte) *constraint {
	return UintBetween(uint64(lower), uint64(upper)).setDetails(TagStructName, "bytebetween")
}

// FloatCompare compares a tested value to compareTo. While compareTo is an float64, the
// tested value can be either float32/float64. If the value is not a float type, the constraint will return false.
func FloatCompare(comparisonType ComparisonType, compareTo float64) *constraint {
	return (&constraint{test: func(value interface{}) bool {
		switch vt := value.(type) {
		case float32:
			return floatComparators[comparisonType](float64(vt), compareTo)
		case float64:
			return floatComparators[comparisonType](vt, compareTo)
		default:
			vv := reflect.ValueOf(value) // ignore nil; can be marked as required if needed
			return vv.Kind() == reflect.Ptr && vv.IsNil()
		}
	}}).setDetails(string(comparisonType), compareTo, TagStructName, "float")
}

// FloatBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func FloatBetween(lower, upper float64) *constraint {
	return And(FloatCompare(GTE, lower), FloatCompare(LTE, upper)).(*constraint).setDetails(TagStructName, "floatbetween")
}

var floatComparators = map[ComparisonType]func(value, compareTo float64) bool{
	EQ:  func(value, compareTo float64) bool { return value == compareTo },
	NEQ: func(value, compareTo float64) bool { return value != compareTo },
	GT:  func(value, compareTo float64) bool { return value > compareTo },
	GTE: func(value, compareTo float64) bool { return value >= compareTo },
	LT:  func(value, compareTo float64) bool { return value < compareTo },
	LTE: func(value, compareTo float64) bool { return value <= compareTo },
}
