package constraint

import (
	"strings"
)

type logicalOp = string

const (
	andOp logicalOp = "And"
	orOp  logicalOp = "Or"
	notOp logicalOp = "Not"
	noOp  logicalOp = ""

	lcAndOp = "and"
	lcOrOp  = "or"
	lcNotOp = "not"
)

func And(operands ...Constraint) Constraint {
	switch len(operands) {
	case 0:
		return Fail("'And' constraint requires at least one operand")
	case 1:
		return operands[0]
	}

	return New(andOp, operands, func(toTest interface{}) bool {
		for _, operand := range operands {
			if !operand.Test(toTest) {
				return false
			}
		}
		return true
	}, logicalOpString(andOp, operands...))
}

func Or(operands ...Constraint) Constraint {
	switch len(operands) {
	case 0:
		return Fail("'Or' constraint requires at least one operand")
	case 1:
		return operands[0]
	}

	return New(orOp, operands, func(toTest interface{}) bool {
		for _, operand := range operands {
			if operand.Test(toTest) {
				return true
			}
		}
		return false
	}, logicalOpString(orOp, operands...))
}

func Not(operand Constraint) Constraint {
	return New(notOp, operand, func(toTest interface{}) bool {
		return !operand.Test(toTest)
	}, logicalOpString(andOp, operand))
}

func logicalOpString(op logicalOp, operands ...Constraint) string {
	var os []string
	for _, operand := range operands {
		os = append(os, operand.String())
	}

	return op+"("+strings.Join(os, ", ")+")"
}
