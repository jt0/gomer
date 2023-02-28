package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

type logicOp = string

const (
	andOp logicOp = "And"
	orOp  logicOp = "Or"
	notOp logicOp = "Not"
	none  logicOp = ""

	lcAndOp = "and"
	lcOrOp  = "or"
	lcNotOp = "not"
)

type logicParams []Constraint

func (lp logicParams) String() string {
	var result string
	for _, operand := range lp { // each logic function has at least one operand
		result = result + operand.String() + ", "
	}
	return result[:len(result)-2] // drop the last two chars
}

func And(constraints ...Constraint) Constraint {
	switch len(constraints) {
	case 0:
		panic("'And' requires at least one constraint")
	case 1:
		return constraints[0]
	}

	return dynamicIfNeeded(New(andOp, logicParams(constraints), func(toTest interface{}) gomerr.Gomerr {
		for _, operand := range constraints {
			if ge := operand.Test(toTest); ge != nil {
				if nse, ok := ge.(*NotSatisfiedError); ok {
					if nse.Constraint == nil {
						nse.Constraint = operand
					}
				} else {
					if _, ok = ge.AttributeLookup("Constraint"); !ok {
						ge = ge.AddAttribute("Constraint", operand)
					}
				}
				return ge
			}
		}
		return nil
	}), constraints...)
}

func Or(constraints ...Constraint) Constraint {
	switch len(constraints) {
	case 0:
		panic("'Or' requires at least one constraint")
	case 1:
		return constraints[0]
	}

	return dynamicIfNeeded(New(orOp, logicParams(constraints), func(toTest interface{}) gomerr.Gomerr {
		var errors []gomerr.Gomerr
		for _, operand := range constraints {
			ge := operand.Test(toTest)
			if ge == nil {
				return nil // any success results in success
			}

			if nse, ok := ge.(*NotSatisfiedError); ok {
				if nse.Constraint == nil {
					nse.Constraint = operand
				} else if nse.Constraint.Type() == "IsNil" || nse.Constraint.Type() == "IsZero" {
					// "or(nil,...)" or "or(zero,...)" is a pattern to bypass the remainder of the constraints if the
					// field is optional. If toTest is not nil, we don't need to include this "failed" constraint in
					// error(s) we might return.
					continue
				} else if _, isDynamicConstraint := operand.(*dynamicConstraint); isDynamicConstraint {
					nse.Constraint = operand
				}
			} else {
				if _, ok = ge.AttributeLookup("Constraint"); !ok {
					ge = ge.AddAttribute("Constraint", operand)
				}
			}

			errors = append(errors, ge)
		}
		return gomerr.Batcher(errors)
	}), constraints...)
}

func Not(constraint Constraint) Constraint {
	return dynamicIfNeeded(New(notOp, constraint, func(toTest interface{}) gomerr.Gomerr {
		if ge := constraint.Test(toTest); ge == nil {
			return NotSatisfied(toTest) // TODO:p1 ensure .String() captures what is "Not"ed
		}
		return nil
	}), constraint)
}
