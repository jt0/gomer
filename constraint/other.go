package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

func Success(msg string) Constraint {
	return New("success: "+msg, nil, func(interface{}) gomerr.Gomerr {
		return nil
	})
}

func Fail(msg string) Constraint {
	return New(msg, nil, func(toTest interface{}) gomerr.Gomerr {
		return NotSatisfied(toTest)
	})
}

func ConfigurationError(problem string) Constraint {
	return New(problem, nil, func(toTest interface{}) gomerr.Gomerr {
		return gomerr.Configuration(problem)
	})
}
