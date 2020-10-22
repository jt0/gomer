package constraint

func And(operands ...Constrainer) Constrainer {
	switch len(operands) {
	case 0:
		return Invalid
	case 1:
		return operands[0]
	}

	return Constrainer{test: func(value interface{}) bool {
		for _, operand := range operands {
			if !operand.test(value) {
				return false
			}
		}
		return true
	}}.setDetails("And", operandDetails(operands), LookupName, "and")
}

func Or(operands ...Constrainer) Constrainer {
	switch len(operands) {
	case 0:
		return Invalid
	case 1:
		return operands[0]
	}

	return Constrainer{test: func(value interface{}) bool {
		for _, operand := range operands {
			if operand.test(value) {
				return true
			}
		}
		return false
	}}.setDetails("Or", operandDetails(operands), LookupName, "or")
}

func Not(operand Constrainer) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return !operand.test(value)
	}}.setDetails("Not", operand.details, LookupName, "not")
}

func operandDetails(operands []Constrainer) []map[string]interface{} {
	operandDetails := make([]map[string]interface{}, len(operands))
	for i, operand := range operands {
		operandDetails[i] = operand.details
	}
	return operandDetails
}
