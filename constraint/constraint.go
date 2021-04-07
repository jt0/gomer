package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

type Constraint interface {
	Type() string
	Value() interface{}
	Test(value interface{}) bool
	Validate(target string, value interface{}) gomerr.Gomerr
}

func New(constraintType string, value interface{}, testFunction func(toTest interface{}) bool) Constraint {
	return &constraint{constraintType, value, testFunction}
}

type constraint struct {
	type_ string
	value interface{}
	test  func(interface{}) bool
}

func (c *constraint) Type() string {
	return c.type_
}

func (c *constraint) Value() interface{} {
	return c.value
}

func (c *constraint) Test(value interface{}) bool {
	return c.test(value)
}

func (c *constraint) Validate(target string, value interface{}) gomerr.Gomerr {
	if !c.Test(value) {
		return gomerr.Build(new(NotSatisfiedError), target, c, value).(*NotSatisfiedError)
	}

	return nil
}
