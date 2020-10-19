package constraint

func And(operands ...Constrainer) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		for _, operand := range operands {
			if !operand.test(value) {
				return false
			}
		}
		return true
	}}.setDetails("AND", operandDetails(operands))
}

func Or(operands ...Constrainer) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		for _, operand := range operands {
			if operand.test(value) {
				return true
			}
		}
		return false
	}}.setDetails("OR", operandDetails(operands))
}

func Not(operand Constrainer) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return !operand.test(value)
	}}.setDetails("NOT", operand.details)
}

func operandDetails(operands []Constrainer) []map[string]interface{} {
	operandDetails := make([]map[string]interface{}, len(operands))
	for i, operand := range operands {
		operandDetails[i] = operand.details
	}
	return operandDetails
}
