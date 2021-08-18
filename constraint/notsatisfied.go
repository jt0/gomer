package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

type NotSatisfiedError struct {
	gomerr.Gomerr
	ToTest     interface{} `gomerr:"include_type"` // Needs to be first (after gomerr.Gomerr) to populate properly via gomerr.Build()
	Target     string
	Constraint Constraint
}

func NotSatisfied(toTest interface{}) *NotSatisfiedError {
	return gomerr.Build(new(NotSatisfiedError), toTest).(*NotSatisfiedError)
}
