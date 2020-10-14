package limit

type unknown struct{}

var Unknown = unknown{}

const unknownMeasure = "Unknown"

func (unknown) Increment(Amount) Amount {
	return Unknown
}

func (unknown) Decrement(Amount) Amount {
	return Unknown
}

func (unknown) Equals(Amount) bool {
	return false
}

func (unknown) Exceeds(Amount) bool {
	return false
}

func (unknown) Zero() Amount {
	return Unknown
}

func (unknown) Measure() Measure {
	return unknownMeasure
}

func (unknown) convert(amount) Amount {
	return Unknown
}

func (unknown) amount() amount {
	return 0
}
