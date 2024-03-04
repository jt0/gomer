package constraint

import (
	"reflect"

	"github.com/jt0/gomer/bind"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

var DefaultValidationTool = NewValidationTool(structs.StructTagDirectiveProvider{"validate"})

func Validate(v interface{}, validationTool *structs.Tool, optional ...*structs.ToolContext) gomerr.Gomerr {
	return structs.ApplyTools(v, structs.EnsureContext(optional...), validationTool)
}

func NewValidationTool(dp structs.DirectiveProvider, optional ...TargetNamer) *structs.Tool {
	var targetNamer TargetNamer
	if len(optional) > 0 {
		targetNamer = optional[0]
	}

	tool := structs.NewTool("constraint.ValidationTool", validationApplierProvider{targetNamer}, dp)

	// TODO:p1 revisit - kinda hacky
	built["struct"] = Struct(tool)

	return tool
}

type validationApplierProvider struct {
	targetNamer TargetNamer
}

func (ap validationApplierProvider) Applier(sv reflect.Type, sf reflect.StructField, directive string, _ string) (structs.Applier, gomerr.Gomerr) {
	if directive == "" {
		return nil, nil
	}

	c, ge := constraintFor(directive, none, sf)
	if ge != nil {
		return nil, gomerr.Configuration("Cannot process directive").Wrap(ge).AddAttribute("Directive", directive)
	}

	var target string
	if ap.targetNamer != nil {
		target = ap.targetNamer(sv, sf)
	} else {
		target = sf.Name
	}

	return validationApplier{target, c}, nil
}

// TargetNamer provides an alternative value for NotSatisfiedError.Target if an error occurs. By default, the value
// will be the field name, but one might want to have a camelCase value (using field.CamelCase) or pre-pend a value
// with an underscore include using field.CamelCase to change the casing on a field or pre-pending an underscore if
// desired.
type TargetNamer func(reflect.Type, reflect.StructField) string

// CamelCaseTargetNamer is a common alternative to rendering the field name in case of a validation error.
var CamelCaseTargetNamer = TransformFieldName(bind.CamelCase)

func TransformFieldName(transform func(string) string) TargetNamer {
	return func(_ reflect.Type, sf reflect.StructField) string {
		return transform(sf.Name)
	}
}

type validationApplier struct {
	target     string
	constraint Constraint
}

func (t validationApplier) Apply(sv reflect.Value, fv reflect.Value, _ *structs.ToolContext) gomerr.Gomerr {
	if dc, ok := t.constraint.(*dynamicConstraint); ok {
		for source, dv := range dc.dynamicValues {
			if value, ge := structs.ValueFromStruct(sv, fv, source); ge != nil {
				return gomerr.Configuration("Unable to validate").AddAttributes("Source", source, "Value", value).Wrap(ge)
			} else if ge = flect.SetValue(dv.Elem(), value); ge != nil {
				return gomerr.Configuration("Unable to validate").AddAttributes("Source", source, "Value", value).Wrap(ge)
			}
		}
	}

	if t.target == "_" {
		return t.constraint.Validate(sv.Type().Name(), sv.Interface())
	}

	return t.constraint.Validate(t.target, fv.Interface())
}
