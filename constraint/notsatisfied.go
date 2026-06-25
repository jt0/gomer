package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

type NotSatisfiedError struct {
	gomerr.Gomerr
	ToTest     any
	Target     string
	Constraint Constraint
}

func NotSatisfied(toTest any) *NotSatisfiedError {
	return gomerr.Build(new(NotSatisfiedError), toTest).(*NotSatisfiedError)
}
