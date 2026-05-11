package constraint

import (
	"reflect"

	"github.com/jt0/gomer/bind"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

var DefaultValidationTool = NewValidationTool(structs.StructTagDirectiveProvider{"validate"})

func Validate(v any, validationTool *structs.Tool, optional ...structs.ToolContext) gomerr.Gomerr {
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
		return nil, gomerr.Configuration("cannot process directive").Wrap(ge).AddAttribute("directive", directive)
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
// will be the field name, but one might want to have a camelCase value or pre-pend an underscore.
type TargetNamer func(reflect.Type, reflect.StructField) string

// CamelCaseTargetNamer is a common alternative to rendering the field name in case of a validation error.
var CamelCaseTargetNamer = TransformFieldName(bind.CamelCaseFn)

func TransformFieldName(transform func(string) string) TargetNamer {
	return func(_ reflect.Type, sf reflect.StructField) string {
		return transform(sf.Name)
	}
}

type validationApplier struct {
	target     string
	constraint Constraint
}

// Apply runs the constraint against the field value fv. If the constraint contains dynamic
// parameters ($.FieldName references), their pointer slots are populated from the enclosing
// struct sv before the constraint's test function executes. This is the point where the
// parse-time allocated pointers receive their runtime values:
//
//  1. structs.ValueFromStruct reads the referenced field's value from the struct.
//  2. flect.SetValue writes it into the pointer (dv.Elem()), coercing types as needed.
//  3. The constraint's test function dereferences the pointer to access the live value.
func (t validationApplier) Apply(sv reflect.Value, fv reflect.Value, _ structs.ToolContext) gomerr.Gomerr {
	if dc, ok := t.constraint.(*dynamicConstraint); ok {
		for source, dv := range dc.dynamicValues {
			if value, ge := structs.ValueFromStruct(sv, fv, source); ge != nil {
				return gomerr.Configuration("unable to validate").AddAttributes("source", source, "value", value).Wrap(ge)
			} else if ge = flect.SetValue(dv.Elem(), value); ge != nil {
				return gomerr.Configuration("unable to validate").AddAttributes("source", source, "value", value).Wrap(ge)
			}
		}
	}

	if t.target == "_" {
		return t.constraint.Validate(sv.Type().Name(), sv.Interface())
	}

	return t.constraint.Validate(t.target, fv.Interface())
}
