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
	Parameters() any
	Validate(target string, toTest any) gomerr.Gomerr
	Test(toTest any) gomerr.Gomerr
	String() string
}

func New(constraintType string, constraintParameters any, testFn func(toTest any) gomerr.Gomerr) Constraint {
	return &constraint{constraintType, constraintParameters, testFn}
}

type constraint struct {
	type_  string
	params any
	testFn func(toTest any) gomerr.Gomerr
}

func (c *constraint) Type() string {
	return c.type_
}

func (c *constraint) Parameters() any {
	return c.params
}

func (c *constraint) Validate(target string, toTest any) gomerr.Gomerr {
	ge := c.Test(toTest)
	if ge == nil {
		return nil
	}

	if be := gomerr.ErrorAs[*gomerr.BatchError](ge); be != nil {
		return c.batchUpdateTarget(target, be)
	}

	return c.updateTarget(target, ge)
}

func (c *constraint) batchUpdateTarget(target string, be *gomerr.BatchError) *gomerr.BatchError {
	errors := be.Errors()
	for i, ge := range errors {
		if ibe := gomerr.ErrorAs[*gomerr.BatchError](ge); ibe != nil {
			errors[i] = c.batchUpdateTarget(target, ibe)
		} else {
			errors[i] = c.updateTarget(target, ge)
		}
	}
	return be
}

func (c *constraint) updateTarget(validationTarget string, ge gomerr.Gomerr) gomerr.Gomerr {
	var target string
	nse := gomerr.ErrorAs[*NotSatisfiedError](ge)
	if nse != nil {
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

	if nse == nil {
		return ge.ReplaceAttribute("Target", target)
	}

	nse.Target = target

	return nse
}

func (c *constraint) Test(toTest any) gomerr.Gomerr {
	ge := c.testFn(toTest)
	if nse := gomerr.ErrorAs[*NotSatisfiedError](ge); nse != nil && nse.Constraint == nil {
		nse.Constraint = c // set only if nil to keep the most specific constraint error
	}
	return ge
}

func (c *constraint) String() string {
	if c.params == nil {
		return c.Type()
	} else {
		return fmt.Sprintf("%s(%s)", c.type_, parametersToString(c.params))
	}
}

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()

func parametersToString(params any) string {
	var pv reflect.Value
	if c, ok := params.(Constraint); ok {
		return c.String()
	} else if cs, ok := params.([]Constraint); ok {
		var ss []string
		for _, c = range cs {
			ss = append(ss, c.String())
		}
		return strings.Join(ss, ", ")
	} else if pv, ok = params.(reflect.Value); !ok {
		pv = reflect.ValueOf(params)
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
					return ConfigurationError("duplicate key for dynamic attributes: " + k)
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
