package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

type Condition struct {
	Target     string
	Constraint Constraint
}

func (c *Condition) Validate(toTest interface{}) gomerr.Gomerr {
	if !c.Constraint.Test(toTest) {
		return gomerr.Build(new(NotSatisfiedError), c, toTest).(*NotSatisfiedError)
	}

	return nil
}

type Constraint interface {
	Type() string
	Value() interface{}
	Test(interface{}) bool
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
