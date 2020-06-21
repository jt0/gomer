package constraint

import (
	"encoding/base64"
	"reflect"
	"regexp"
)

type Constraint interface {
	//fmt.Stringer
	Evaluate(constrained interface{}) bool
}

type exactlyOnceConstraint struct {
}

func ExactlyOnce() Constraint {
	return &exactlyOnceConstraint{}
}

func (c *exactlyOnceConstraint) Evaluate(constrained interface{}) bool {
	ct := reflect.TypeOf(constrained)
	return ct != nil && (ct.Kind() != reflect.Array || ct.Len() == 1)
}

type nonZeroConstraint struct {
}

func NonZero() Constraint {
	return &nonZeroConstraint{}
}

func (c *nonZeroConstraint) Evaluate(constrained interface{}) bool {
	return !reflect.ValueOf(constrained).IsZero()
}

type lengthConstraint struct {
	min int
	max int
}

func Length(min, max int) Constraint {
	return &lengthConstraint{min, max}
}

func (c *lengthConstraint) Evaluate(interface{}) bool {
	return false
}

type valuesConstraint struct {
	values []interface{}
}

func Values(values ...interface{}) Constraint {
	return &valuesConstraint{values}
}

func (c *valuesConstraint) Evaluate(i interface{}) bool {
	for _, v := range c.values {
		if i == v {
			return true
		}
	}

	return false
}

type typeOfConstraint struct {
	type_ reflect.Type
}

func TypeOf(i interface{}) Constraint {
	t, ok := i.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(i)
	}
	return &typeOfConstraint{t}
}

func (c *typeOfConstraint) Evaluate(constrained interface{}) bool {
	return reflect.TypeOf(constrained) == c.type_
}

type functionConstraint struct {
	function func(interface{}) bool
}

func Function(fn func(interface{}) bool) Constraint {
	return &functionConstraint{fn}
}

func (c *functionConstraint) Evaluate(constrained interface{}) bool {
	return c.function(constrained)
}

type base64Constraint struct {
}

func Base64() Constraint {
	return &base64Constraint{}
}

func (c *base64Constraint) Evaluate(constrained interface{}) bool {
	s, ok := constrained.(string)
	if !ok {
		return false
	}

	_, err := base64.RawURLEncoding.DecodeString(s)

	return err != nil
}

type regexpConstraint struct {
	regexp *regexp.Regexp
}

func Regexp(regexp *regexp.Regexp) Constraint {
	return &regexpConstraint{regexp}
}

func (c *regexpConstraint) Evaluate(constrained interface{}) bool {
	s, ok := constrained.(string)
	if !ok {
		return false
	}

	return c.regexp.MatchString(s)
}
