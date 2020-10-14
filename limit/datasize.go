package limit

type DataSize amount

func (d DataSize) Increment(amount Amount) Amount {
	return d + amount.(DataSize)
}

func (d DataSize) Decrement(amount Amount) Amount {
	return d - amount.(DataSize)
}

func (d DataSize) Equals(amount Amount) bool {
	return d == amount.(DataSize)
}

func (d DataSize) Exceeds(amount Amount) bool {
	return d > amount.(DataSize)
}

func (d DataSize) Zero() Amount {
	const zeroBytes DataSize = iota
	return zeroBytes
}

func (d DataSize) Measure() Measure {
	const measure = "Bytes"
	return measure
}

func (d DataSize) convert(a amount) Amount {
	return DataSize(a)
}

func (d DataSize) amount() amount {
	return amount(d)
}
