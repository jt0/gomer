package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

type NotSatisfiedError struct {
	gomerr.Gomerr
	Target     string
	Constraint Constraint
	Value      interface{}
}

func NotSatisfied(target string, constraint Constraint, value interface{}) *NotSatisfiedError {
	return gomerr.Build(new(NotSatisfiedError), target, constraint, value).(*NotSatisfiedError)
}
