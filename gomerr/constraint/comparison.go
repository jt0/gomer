package constraint

type ComparisonType string

const (
	EQ  ComparisonType = "Equals"
	NEQ                = "NotEquals"
	GT                 = "GreaterThan"
	GTE                = "GreaterThanOrEquals"
	LT                 = "LessThan"
	LTE                = "LessThanOrEquals"
)

// IntCompare compares the tested value to compareTo. While compareTo is an int64, the
// tested value can be any of the integer types (e.g. int, int16, etc). If the tested value is not
// an integer type, the constraint will return false.
func IntCompare(comparisonType ComparisonType, compareTo int64) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		switch value.(type) {
		case int:
			return intComparators[comparisonType](int64(value.(int)), compareTo)
		case int8:
			return intComparators[comparisonType](int64(value.(int8)), compareTo)
		case int16:
			return intComparators[comparisonType](int64(value.(int16)), compareTo)
		case int32:
			return intComparators[comparisonType](int64(value.(int32)), compareTo)
		case int64:
			return intComparators[comparisonType](value.(int64), compareTo)
		default:
			return false
		}
	}}.setDetails(comparisonType, compareTo)
}

// IntBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func IntBetween(lower, upper int64) Constrainer {
	return And(IntCompare(GTE, lower), IntCompare(LTE, upper))
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
func UintCompare(comparisonType ComparisonType, compareTo uint64) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		switch value.(type) {
		case uint:
			return uintComparators[comparisonType](uint64(value.(uint)), compareTo)
		case uint8:
			return uintComparators[comparisonType](uint64(value.(uint8)), compareTo)
		case uint16:
			return uintComparators[comparisonType](uint64(value.(uint16)), compareTo)
		case uint32:
			return uintComparators[comparisonType](uint64(value.(uint32)), compareTo)
		case uint64:
			return uintComparators[comparisonType](value.(uint64), compareTo)
		default:
			return false
		}
	}}.setDetails(comparisonType, compareTo)
}

// UintBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func UintBetween(lower, upper uint64) Constrainer {
	return And(UintCompare(GTE, lower), UintCompare(LTE, upper))
}

var uintComparators = map[ComparisonType]func(value, compareTo uint64) bool{
	EQ:  func(value, compareTo uint64) bool { return value == compareTo },
	NEQ: func(value, compareTo uint64) bool { return value != compareTo },
	GT:  func(value, compareTo uint64) bool { return value > compareTo },
	GTE: func(value, compareTo uint64) bool { return value >= compareTo },
	LT:  func(value, compareTo uint64) bool { return value < compareTo },
	LTE: func(value, compareTo uint64) bool { return value <= compareTo },
}

func ByteCompare(comparisonType ComparisonType, compareTo byte) Constrainer {
	return UintCompare(comparisonType, uint64(compareTo))
}

func ByteBetween(lower, upper byte) Constrainer {
	return UintBetween(uint64(lower), uint64(upper))
}

// FloatCompare compares a tested value to compareTo. While compareTo is an float64, the
// tested value can be either float32/float64. If the value is not a float type, the constraint will return false.
func FloatCompare(comparisonType ComparisonType, compareTo float64) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		switch value.(type) {
		case float32:
			return floatComparators[comparisonType](float64(value.(float32)), compareTo)
		case float64:
			return floatComparators[comparisonType](value.(float64), compareTo)
		default:
			return false
		}
	}}.setDetails(comparisonType, compareTo)
}

// FloatBetween determines whether the provided value is (inclusively) between the lower and upper values provided.
// Stated explicitly, this tests for lower <= value <= upper.
func FloatBetween(lower, upper float64) Constrainer {
	return And(FloatCompare(GTE, lower), FloatCompare(LTE, upper))
}

var floatComparators = map[ComparisonType]func(value, compareTo float64) bool{
	EQ:  func(value, compareTo float64) bool { return value == compareTo },
	NEQ: func(value, compareTo float64) bool { return value != compareTo },
	GT:  func(value, compareTo float64) bool { return value > compareTo },
	GTE: func(value, compareTo float64) bool { return value >= compareTo },
	LT:  func(value, compareTo float64) bool { return value < compareTo },
	LTE: func(value, compareTo float64) bool { return value <= compareTo },
}
