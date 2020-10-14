package limit

type Count amount

func (c Count) Increment(amount Amount) Amount {
	return c + amount.(Count)
}

func (c Count) Decrement(amount Amount) Amount {
	return c - amount.(Count)
}

func (c Count) Equals(amount Amount) bool {
	return c == amount.(Count)
}

func (c Count) Exceeds(amount Amount) bool {
	return c > amount.(Count)
}

func (c Count) Zero() Amount {
	const zeroCount Count = iota
	return zeroCount
}

func (c Count) Measure() Measure {
	const measure = "Count"
	return measure
}

func (c Count) convert(a amount) Amount {
	return Count(a)
}

func (c Count) amount() amount {
	return amount(c)
}
