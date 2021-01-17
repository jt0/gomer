package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

type NotSatisfiedError struct {
	gomerr.Gomerr
	Constraint map[string]interface{}
	On         string
	Value      interface{} `gomerr:"include_type"`
}

func NotSatisfied(constraint Constraint, on string, value interface{}) *NotSatisfiedError {
	return gomerr.Build(new(NotSatisfiedError), constraint.Details(), on, value).(*NotSatisfiedError)
}
