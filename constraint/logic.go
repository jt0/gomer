package constraint

func And(operands ...Constraint) Constraint {
	switch len(operands) {
	case 0:
		return Invalid
	case 1:
		return operands[0]
	}

	return (&constraint{test: func(value interface{}) bool {
		for _, operand := range operands {
			if !operand.Test(value) {
				return false
			}
		}
		return true
	}}).setDetails("And", operandDetails(operands), TagStructName, "and")
}

func Or(operands ...Constraint) Constraint {
	switch len(operands) {
	case 0:
		return Invalid
	case 1:
		return operands[0]
	}

	return (&constraint{test: func(value interface{}) bool {
		for _, operand := range operands {
			if operand.Test(value) {
				return true
			}
		}
		return false
	}}).setDetails("Or", operandDetails(operands), TagStructName, "or")
}

func Not(operand Constraint) Constraint {
	return (&constraint{test: func(value interface{}) bool {
		return !operand.Test(value)
	}}).setDetails("Not", operand.Details(), TagStructName, "not")
}

func operandDetails(operands []Constraint) []map[string]interface{} {
	operandDetails := make([]map[string]interface{}, len(operands))
	for i, operand := range operands {
		operandDetails[i] = operand.Details()
	}
	return operandDetails
}
