package util

type Amount interface {
	Increment(Amount) Amount
	Decrement(Amount) Amount
	Equals(Amount) bool
	Exceeds(Amount) bool
	Measure() Measure
	String() string
}

type Measure string
