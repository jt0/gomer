package constraint

import (
	"fmt"
	"os"

	"github.com/jt0/gomer/gomerr"
)

const (
	LookupName = "LookupName"
)

type Constraint interface {
	Validate(interface{}) gomerr.Gomerr
	Test(interface{}) bool
	On(string) Constraint
	Details() map[string]interface{}
}

func NewType(test func(value interface{}) bool, detailParts ...interface{}) Constraint {
	return (&constraint{test: test}).setDetails(detailParts...)
}

type constraint struct {
	test    func(value interface{}) bool
	on      string
	details map[string]interface{}
}

func (c *constraint) Validate(value interface{}) gomerr.Gomerr {
	if !c.test(value) {
		return gomerr.Build(new(NotSatisfiedError), c.details, c.on, value).(*NotSatisfiedError)
	}

	return nil
}

func (c *constraint) Test(value interface{}) bool {
	return c.test(value)
}

func (c *constraint) On(on string) Constraint {
	c.on = on
	return c
}

func (c *constraint) Details() map[string]interface{} {
	return c.details
}

func (c *constraint) setDetails(detailParts ...interface{}) *constraint {
	numParts := len(detailParts)

	if numParts%2 != 0 {
		_, _ = fmt.Fprintf(os.Stderr, "Odd number of elements. Expecting key/value pairs so ignoring last one: %v\n", detailParts[len(detailParts)-1])
	}

	if c.details == nil {
		c.details = make(map[string]interface{}, numParts/2)
	}
	for i := 0; i < numParts; i += 2 {
		key, ok := detailParts[i].(string)
		if !ok {
			_, _ = fmt.Fprintf(os.Stderr, "Skipping pair with non-string key value: %v\n", detailParts[i])
		} else {
			c.details[key] = detailParts[i+1]
		}
	}

	return c
}
