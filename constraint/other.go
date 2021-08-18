package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

var Required = New("Required", nil, func(toTest interface{}) gomerr.Gomerr {
	ttv, isNil := indirectValueOf(toTest)
	if isNil || ttv.IsZero() {
		return NotSatisfied(toTest)
	}
	return nil
})

func Success(msg string) Constraint {
	return New("Success: "+msg, nil, func(interface{}) gomerr.Gomerr {
		return nil
	})
}

func Fail(msg string) Constraint {
	return New(msg, nil, func(toTest interface{}) gomerr.Gomerr {
		return NotSatisfied(toTest)
	})
}
