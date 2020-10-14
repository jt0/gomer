package limit

type Amount interface {
	Increment(Amount) Amount
	Decrement(Amount) Amount
	Equals(Amount) bool
	Exceeds(Amount) bool
	Zero() Amount
	Measure() Measure

	convert(amount) Amount
	amount() amount
}

type Measure string
