package constraint

import (
	"fmt"

	"github.com/jt0/gomer/gomerr"
)

type Constraint interface {
	Type() string
	Value() interface{}
	Test(value interface{}) bool
	Validate(target string, value interface{}) gomerr.Gomerr
	fmt.Stringer
}

func New(constraintType string, value interface{}, testFunction func(toTest interface{}) bool, optionalStringVal ...string) Constraint {
	var stringVal string
	if len(optionalStringVal) > 0 {
		stringVal = optionalStringVal[0]
	}
	return &constraint{constraintType, value, testFunction, stringVal}
}

type constraint struct {
	type_     string
	value     interface{}
	test      func(interface{}) bool
	stringVal string
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

func (c *constraint) String() string {
	if c.stringVal != "" {
		return c.stringVal
	}

	if c.value == nil {
		return c.type_
	}

	return fmt.Sprintf("%s(%v)", c.type_, c.value)
}
