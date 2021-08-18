package constraint

import (
	"reflect"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

const DefaultValidationFieldToolName = "validate"

var DefaultValidationFieldTool = NewValidationFieldTool(DefaultValidationFieldToolName)

func NewValidationFieldTool(name string, optionalTargetNamer ...TargetNamer) *fields.FieldTool {
	vft := fields.NewFieldTool(name, func(sv reflect.Type, field reflect.StructField, directive string) (fields.Applier, gomerr.Gomerr) {
		if directive == "" {
			return nil, nil
		}

		c, ge := constraintFor(directive, none, field)
		if ge != nil {
			return nil, gomerr.Configuration("Cannot process directive").Wrap(ge).AddAttribute("Directive", directive)
		}

		var target string
		if len(optionalTargetNamer) > 0 {
			target = optionalTargetNamer[0](sv, field)
		} else {
			target = field.Name
		}

		return validationApplier{target, c}, nil
	})

	built["struct"] = Struct(name)

	return vft
}

// TargetNamer provides an alternative value for NotSatisfiedError.Target if an error occurs. By default, the value
// will be the field name, but one might want to have a camelCase value (using field.CamelCase) or pre-pend a value
// with an underscore include using field.CamelCase to change the casing on a field or pre-pending an underscore if
// desired.
type TargetNamer func(reflect.Type, reflect.StructField) string

// CamelCaseTargetNamer is a common alternative to rendering the field name in case of a validation error.
var CamelCaseTargetNamer = TransformFieldName(fields.CamelCase)

func TransformFieldName(transform func(string) string) TargetNamer {
	return func(_ reflect.Type, sf reflect.StructField) string {
		return transform(sf.Name)
	}
}

type validationApplier struct {
	target     string
	constraint Constraint
}

func (t validationApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, tc fields.ToolContext) gomerr.Gomerr {
	if dc, ok := t.constraint.(*dynamicConstraint); ok {
		for source, dv := range dc.dynamicValues {
			if value, ge := fields.ValueFromStruct(structValue, fieldValue, source); ge != nil {
				return gomerr.Configuration("Unable to validate").AddAttributes("Field", t.target, "Source", source, "Value", value).Wrap(ge)
			} else if ge = flect.SetValue(dv.Elem(), value); ge != nil {
				return gomerr.Configuration("Unable to validate").AddAttributes("Field", t.target, "Source", source, "Value", value).Wrap(ge)
			}
		}
	}

	return t.constraint.Validate(t.target, fieldValue.Interface())
}
