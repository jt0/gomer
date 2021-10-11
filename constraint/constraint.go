package constraint

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jt0/gomer/gomerr"
)

type Constraint interface {
	Type() string
	Parameters() interface{}
	Validate(target string, toTest interface{}) gomerr.Gomerr
	Test(toTest interface{}) gomerr.Gomerr
	String() string
}

func New(constraintType string, constraintParameters interface{}, testFn func(toTest interface{}) gomerr.Gomerr) Constraint {
	return &constraint{constraintType, constraintParameters, testFn}
}

type constraint struct {
	type_  string
	params interface{}
	testFn func(toTest interface{}) gomerr.Gomerr
}

func (c *constraint) Type() string {
	return c.type_
}

func (c *constraint) Parameters() interface{} {
	return c.params
}

func (c *constraint) Validate(target string, toTest interface{}) gomerr.Gomerr {
	ge := c.Test(toTest)
	if ge == nil {
		return nil
	}

	if be, ok := ge.(*gomerr.BatchError); ok {
		return c.batchUpdateTarget(target, be)
	}

	return c.updateTarget(target, ge)
}

func (c *constraint) batchUpdateTarget(target string, be *gomerr.BatchError) *gomerr.BatchError {
	errors := be.Errors()
	for i, ge := range errors {
		if ibe, ok := ge.(*gomerr.BatchError); ok {
			errors[i] = c.batchUpdateTarget(target, ibe)
		} else {
			errors[i] = c.updateTarget(target, ge)
		}
	}
	return be
}

func (c *constraint) updateTarget(validationTarget string, ge gomerr.Gomerr) gomerr.Gomerr {
	var target string
	nse, isNse := ge.(*NotSatisfiedError)
	if isNse {
		target = nse.Target
	} else if ta, ok := ge.AttributeLookup("Target"); ok {
		target = ta.(string)
	} // else target == ""

	if validationTarget == "" {
		validationTarget = "\"\"" // Used to indicate an empty value. Unlikely to happen much in practice.
	}

	if target == "" {
		target = validationTarget
	} else if target[0] == '[' {
		target = validationTarget + target
	} else {
		target = validationTarget + "." + target
	}

	if !isNse {
		return ge.AddAttribute("Target", target)
	}

	nse.Target = target

	return nse
}

func (c *constraint) Test(toTest interface{}) gomerr.Gomerr {
	ge := c.testFn(toTest)
	if nse, ok := ge.(*NotSatisfiedError); ok {
		if nse.Constraint == nil { // keep the most specific constraint error
			nse.Constraint = c
		}
	}
	return ge
}

func (c *constraint) String() string {
	if c.params == nil {
		return c.Type()
	} else {
		return fmt.Sprintf("%s(%s)", c.type_, parametersToString(reflect.ValueOf(c.params)))
	}
}

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()

func parametersToString(pv reflect.Value) string {
	if !pv.IsValid() {
		return "<invalid>"
	}

	switch pv.Kind() {
	case reflect.Ptr:
		if pv.IsNil() {
			return "<nil>"
		}
		return parametersToString(pv.Elem())
	case reflect.Array, reflect.Slice:
		pvLen := pv.Len()
		ss := make([]string, pvLen)
		for i := 0; i < pvLen; i++ {
			ss[i] = parametersToString(pv.Index(i))
		}
		return strings.Join(ss, ", ")
	case reflect.Struct:
		if pv.Type() == timeType {
			return pv.Interface().(time.Time).Format(time.RFC3339)
		}
		fallthrough
	default:
		return fmt.Sprintf("%v", pv)
	}
}

// static location -> $.SomeField --> sv.FieldByName().Interface()
// dynamic location -> $.MyFunction() --> sv.MethodByName()....
// Some other function(?) -> $$SomeFunction
// at `apply` need to get the value of what the constraint is going to use to check
type dynamicConstraint struct {
	Constraint
	dynamicValues map[string]reflect.Value
}

func dynamicIfNeeded(newConstraint Constraint, constraints ...Constraint) Constraint {
	collectedDynamicValues := make(map[string]reflect.Value)
	for _, c := range constraints {
		if dc, ok := c.(*dynamicConstraint); ok {
			for k, v := range dc.dynamicValues {
				if dv, exists := collectedDynamicValues[k]; exists && dv != v {
					panic("duplicate key for dynamic attributes: " + k) // can these be merged somehow?
				}
				collectedDynamicValues[k] = v
			}
		}
	}

	if len(collectedDynamicValues) > 0 {
		return &dynamicConstraint{newConstraint, collectedDynamicValues}
	}

	return newConstraint
}
