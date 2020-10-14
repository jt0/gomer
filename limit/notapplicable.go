package limit

type notApplicable struct{}

var NotApplicable = notApplicable{}

func (notApplicable) Increment(Amount) Amount {
	return NotApplicable
}

func (notApplicable) Decrement(Amount) Amount {
	return NotApplicable
}

func (notApplicable) Equals(Amount) bool {
	return false
}

func (notApplicable) Exceeds(Amount) bool {
	return false
}

func (notApplicable) Zero() Amount {
	return NotApplicable
}

func (notApplicable) Measure() Measure {
	const notApplicableMeasure = "NotApplicable"
	return notApplicableMeasure
}

func (notApplicable) convert(amount) Amount {
	return NotApplicable
}

func (notApplicable) amount() amount {
	return 0
}
