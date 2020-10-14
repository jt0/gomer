package constraint

func And(operands ...Constrainer) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		for _, operand := range operands {
			if !operand.test(value) {
				return false
			}
		}
		return true
	}}.setDetails("AND", operands)
}

func Or(operands ...Constrainer) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		for _, operand := range operands {
			if operand.test(value) {
				return true
			}
		}
		return false
	}}.setDetails("OR", operands)
}

func Not(operand Constrainer) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return !operand.test(value)
	}}.setDetails("NOT", operand)
}
