package gomerr

import (
	"github.com/jt0/gomer/gomerr/constraint"
)

type UnprocessableError struct {
	Gomerr
	Name       string
	Value      interface{} `gomerr:"include_type"`
	Constraint constraint.Constraint
}

func Unprocessable(name string, value interface{}, constraint constraint.Constraint) *UnprocessableError {
	return Build(new(UnprocessableError), name, value, constraint).(*UnprocessableError)
}
