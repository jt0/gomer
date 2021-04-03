package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

type NotSatisfiedError struct {
	gomerr.Gomerr

	Condition Condition
	Provided  interface{}
}

func NotSatisfied(condition Condition, provided interface{}) *NotSatisfiedError {
	return gomerr.Build(new(NotSatisfiedError), condition, provided).(*NotSatisfiedError)
}
