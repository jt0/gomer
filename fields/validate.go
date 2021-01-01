package fields

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
)

var built = map[string]constraint.Constraint{
	"base64":   constraint.Base64,
	"empty":    constraint.Empty,
	"nil":      constraint.Nil,
	"notnil":   constraint.NotNil,
	"required": constraint.Required,
}

var available = map[string]interface{}{
	"and":          constraint.And,
	"endswith":     constraint.EndsWith,
	"equals":       constraint.Equals,
	"float":        constraint.FloatCompare,
	"floatbetween": constraint.FloatBetween,
	"int":          constraint.IntCompare,
	"intbetween":   constraint.IntBetween,
	"len":          constraint.Length,
	"maxlen":       constraint.MaxLength,
	"minlen":       constraint.MinLength,
	"not":          constraint.Not,
	"notequals":    constraint.NotEquals,
	"oneof":        constraint.OneOf,
	"or":           constraint.Or,
	"regexp":       constraint.Regexp,
	"startswith":   constraint.StartsWith,
	"typeof":       constraint.TypeOf,
	"uint":         constraint.UintCompare,
	"uintbetween":  constraint.UintBetween,
}

func RegisterConstraints(constraints map[string]constraint.Constraint) {
	for validationName, c := range constraints {
		if ge := dollarNameConstraint.On("Constraint name").Validate(validationName); ge != nil {
			panic(ge.AddAttribute("Note", "Custom validation names must start with a '$' symbol and between 2 and 64 characters long"))
		}

		built[strings.ToLower(validationName)] = c
	}
}

// contexts match to the mos specific one.
// create:foo(1),len(1,3),required,or($id,regexp('^AppConfig\\.[A-Za-z0-9]{9,40}$')),len(1,3);*:len(1,3)
func (f *field) validateTag(validateTag string) (ge gomerr.Gomerr) {
	if validateTag == "" {
		return nil
	}

	// Convert all escaped semi-colons (i.e. '\;') into ' '. Since we trimmed out spaces above, we can re-use them now as a
	// placeholder which we'll convert back on each section of the tag we process in the loop below.
	validateTag = strings.ReplaceAll(validateTag, "\\;", " ")
	f.constraints = make(map[string]constraint.Constraint)
	for _, contextValidations := range strings.Split(validateTag, ";") {
		// create:len(1,3),required,or[$id,regexp('^AppConfig\\.[A-Za-z0-9]{9,40}$')],len(1,3)
		// convert spaces back to no-longer-escaped semi-colons
		contextValidations = strings.ReplaceAll(contextValidations, " ", ";")

		var context, validations string
		if separatorIndex := strings.Index(contextValidations, ":"); separatorIndex <= 0 {
			context = matchImplicitly
			validations = contextValidations
		} else {
			context = contextValidations[:separatorIndex]
			validations = contextValidations[separatorIndex+1:]
		}

		// context = create; validations = required,or($id,regexp('^AppConfig\\.[A-Za-z0-9]{9,40}$')),len(1,3)
		if f.constraints[context], ge = constraintFor(validations, ""); ge != nil {
			return ge.AddAttributes("Field", f.name, "Validations", validations)
		}
	}

	return nil
}

var emptyConstraint = constraint.NewType(nil)

func constraintFor(validationsString string, logicalOp string) (constraint.Constraint, gomerr.Gomerr) {
	var constraints []constraint.Constraint
	var ovs string

	if built, ok := built[validationsString]; ok {
		if built.Details()[constraint.TagStructName] == logicalOp {
			return built, nil
		}
		constraints = append(constraints, built)
		ovs = logicalOp + "(" + validationsString + ")"
		validationsString = ""
	} else {
		ovs = validationsString
	}

	// Examples:
	//  1. len(1,2)
	//  2. required,len(1,2)
	//  3. or(required,len(1,2))
	for len(validationsString) > 0 {
		openParenIndex := strings.Index(validationsString, "(")
		commaIndex := strings.Index(validationsString, ",")

		if openParenIndex >= 0 && (commaIndex < 0 || commaIndex > openParenIndex) { // true for 1 & 3
			constraintName := strings.ToLower(validationsString[:openParenIndex])
			validations := validationsString[openParenIndex+1:] // '1,2)', 'required,len(1,2))

			constrainer, ge := parameterizedConstraint(constraintName, &validations)
			if ge != nil {
				return emptyConstraint, ge
			}

			constraints = append(constraints, constrainer)
			validationsString = validations
		} else { // true for 2
			var constraintName string
			if commaIndex == 0 {
				validationsString = validationsString[1:] // skip stray commas
				continue
			} else if commaIndex > 0 {
				constraintName = strings.ToLower(validationsString[:commaIndex])
				validationsString = validationsString[commaIndex+1:] // may break if last character is a comma
			} else { // no commas and no parens (otherwise would have gone into the first if clause)
				constraintName = strings.ToLower(validationsString)
				validationsString = ""
			}

			constrainer, ge := constraintFor(constraintName, "")
			if ge != nil {
				return emptyConstraint, ge
			}

			constraints = append(constraints, constrainer)
		}
	}

	switch len(constraints) {
	case 0:
		return emptyConstraint, gomerr.Configuration("No constraints found")
	case 1:
		if logicalOp == "not" {
			not := constraint.Not(constraints[0])
			built[ovs] = not
			return not, nil
		} // ignore other logicalOps since they
		return constraints[0], nil
	default:
		switch logicalOp {
		case "or":
			or := constraint.Or(constraints...)
			built[ovs] = or
			return or, nil
		case "not":
			not := constraint.Not(constraint.And(constraints...))
			built[ovs] = not
			return not, nil
		default:
			and := constraint.And(constraints...)
			built[ovs] = and
			return and, nil
		}
	}
}

// Called w/ first open paren "consumed", e.g. '1,2)', 'required,len(1,2))
func parameterizedConstraint(constraintName string, parenthetical *string) (constraint.Constraint, gomerr.Gomerr) {
	remainder := *parenthetical
	var accumulator int
	for parenCounter := 1; parenCounter != 0; {
		if closeIndex := strings.Index(remainder, ")"); closeIndex < 0 {
			return emptyConstraint, gomerr.Configuration("Unable to find a balanced expression: (" + *parenthetical)
		} else if openIndex := strings.Index(remainder, "("); openIndex >= 0 && openIndex < closeIndex {
			parenCounter++
			accumulator += openIndex + 1
			remainder = remainder[openIndex+1:]
		} else {
			parenCounter--
			accumulator += closeIndex + 1
			remainder = remainder[closeIndex+1:]
		}
	}

	parametersString := (*parenthetical)[:accumulator-1]
	*parenthetical = remainder

	switch constraintName {
	case "and", "or", "not":
		return constraintFor(parametersString, constraintName)
	default:
		return buildConstraint(constraintName, parametersString)
	}
}

func buildConstraint(constraintName, parameterString string) (constraint.Constraint, gomerr.Gomerr) {
	constrainerFunction, ok := available[constraintName]
	if !ok {
		return emptyConstraint, gomerr.Configuration("Unknown validation type: " + constraintName)
	}

	cfv := reflect.ValueOf(constrainerFunction)
	cft := cfv.Type()
	numIn := cft.NumIn()
	isVariadic := cft.IsVariadic()
	if isVariadic {
		numIn -= 1 // we'll handle the last one as a special case
	}

	// find any escaped commas and replace w/ spaces
	parameterString = strings.ReplaceAll(parameterString, "\\,", " ")
	parameters := strings.Split(parameterString, ",")
	parametersLen := len(parameters)
	if isVariadic {
		if parametersLen < numIn {
			return emptyConstraint, gomerr.Configuration(fmt.Sprintf("Expecting at least %d parameters, but found %d in %v", numIn, parametersLen, parameters))
		}
	} else if parametersLen != numIn {
		return emptyConstraint, gomerr.Configuration(fmt.Sprintf("Expecting %d parameters, but found %d in %v", numIn, parametersLen, parameters))
	}

	in := make([]reflect.Value, parametersLen)

	var pIndex int
	for pIndex = 0; pIndex < numIn; pIndex++ {
		parameter := strings.ReplaceAll(parameters[pIndex], " ", ",")
		pElem := reflect.New(cft.In(pIndex)).Elem()
		if ge := flect.SetValue(pElem, parameter); ge != nil {
			return emptyConstraint, gomerr.Configuration(fmt.Sprintf("Unable to set input parameter %d for %s with: %s", pIndex, constraintName, parameter))
		}
		in[pIndex] = pElem
	}

	if isVariadic {
		inType := cft.In(pIndex)
		elemType := inType.Elem()
		for ; pIndex < parametersLen; pIndex++ {
			parameter := strings.ReplaceAll(parameters[pIndex], " ", ",")
			pElem := reflect.New(elemType).Elem()
			if ge := flect.SetValue(pElem, parameter); ge != nil {
				return emptyConstraint, gomerr.Configuration(fmt.Sprintf("Unable to set variadic parameter element for %s with: %s", constraintName, parameter))
			}
			in[pIndex] = pElem
		}
	}

	// The set of "available" values all result in a single response element which is a constraint.constrainer.
	// If something goes sideways, this will panic (and indicates bug in Gomer).

	results := cfv.Call(in)
	constrainer := results[0].Interface().(constraint.Constraint)
	built[constraintName+"("+parameterString+")"] = constrainer

	return constrainer, nil
}

const matchImplicitly = "*"

func (fs *Fields) Validate(v reflect.Value, context string) gomerr.Gomerr {
	var errors []gomerr.Gomerr

	for _, field := range fs.fieldMap {
		if field.constraints == nil || strings.Contains(field.location, ".") { // TODO: handle nested/embedded structs
			continue
		}

		c, ok := field.constraints[context]
		if !ok {
			if c, ok = field.constraints[matchImplicitly]; !ok {
				continue
			}
		}

		fv := v.FieldByName(field.name)
		if !fv.IsValid() {
			continue
		}

		if fv.Kind() == reflect.Ptr && !fv.IsZero() {
			fv = fv.Elem()
		}

		fvi := fv.Interface()
		if ge := c.Validate(fvi); ge != nil {
			errors = append(errors, ge.AddAttribute("field.name", field.name))
		}
	}

	return gomerr.Batcher(errors)
}
