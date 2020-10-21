package fields

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
)

var built = map[string]constraint.Constrainer{
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
	"oneof":        constraint.OneOf,
	"or":           constraint.Or,
	"regexp":       constraint.RegexpMust,
	"startswith":   constraint.StartsWith,
	"typeof":       constraint.TypeOf,
	"uint":         constraint.UintCompare,
	"uintbetween":  constraint.UintBetween,
}

func RegisterConstraints(constraints map[string]constraint.Constrainer) {
	for validationName, constrainer := range constraints {
		if ge := gomerr.Test("Custom validation names must start with a '$' symbol and between 2 and 64 characters long", validationName, dollarNameConstraint); ge != nil {
			panic(ge)
		}

		built[strings.ToLower(validationName)] = constrainer
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
	f.constraints = make(map[string]constraint.Constrainer)
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
		if f.constraints[context], ge = constrainerFor(validations, ""); ge != nil {
			return ge.AddAttributes("Field", f.name, "Validations", validations)
		}
	}

	return nil
}

var emptyConstrainer = constraint.Constrainer{}

func constrainerFor(validationsString string, logicalOp string) (constraint.Constrainer, gomerr.Gomerr) {
	if built, ok := built[validationsString]; ok {
		return built, nil
	}

	// Examples:
	//  1. len(1,2)
	//  2. required,len(1,2)
	//  3. or(required,len(1,2))
	var constrainers []constraint.Constrainer
	for len(validationsString) > 0 {
		openParenIndex := strings.Index(validationsString, "(")
		commaIndex := strings.Index(validationsString, ",")

		if openParenIndex >= 0 && (commaIndex < 0 || commaIndex > openParenIndex) { // true for 1 & 3
			constraintName := strings.ToLower(validationsString[:openParenIndex])
			validations := validationsString[openParenIndex+1:] // '1,2)', 'required,len(1,2))

			constrainer, ge := parameterizedConstrainer(constraintName, &validations)
			if ge != nil {
				return emptyConstrainer, ge
			}

			constrainers = append(constrainers, constrainer)
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

			constrainer, ge := constrainerFor(constraintName, "")
			if ge != nil {
				return emptyConstrainer, ge
			}

			constrainers = append(constrainers, constrainer)
		}
	}

	switch len(constrainers) {
	case 0:
		return emptyConstrainer, gomerr.Configuration("No constraints found")
	case 1:
		if logicalOp == "not" {
			return constraint.Not(constrainers[0]), nil
		} // ignore other logicalOps since they
		return constrainers[0], nil
	default:
		switch logicalOp {
		case "or":
			return constraint.Or(constrainers...), nil
		case "not":
			return constraint.Not(constraint.And(constrainers...)), nil
		default:
			return constraint.And(constrainers...), nil
		}
	}
}

// Called w/ first open paren "consumed", e.g. '1,2)', 'required,len(1,2))
func parameterizedConstrainer(constraintName string, parenthetical *string) (constraint.Constrainer, gomerr.Gomerr) {
	remainder := *parenthetical
	var accumulator int
	for parenCounter := 1; parenCounter != 0; {
		if closeIndex := strings.Index(remainder, ")"); closeIndex < 0 {
			return emptyConstrainer, gomerr.Configuration("Unable to find a balanced expression: (" + *parenthetical)
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
		return constrainerFor(parametersString, constraintName)
	default:
		return buildConstrainer(constraintName, parametersString)
	}
}

func buildConstrainer(constraintName, parameterString string) (constraint.Constrainer, gomerr.Gomerr) {
	constrainerFunction, ok := available[constraintName]
	if !ok {
		return emptyConstrainer, gomerr.Configuration("Unknown validation type: " + constraintName)
	}

	cfv := reflect.ValueOf(constrainerFunction)
	cft := cfv.Type()
	numIn := cft.NumIn()

	// find any escaped commas and replace w/ spaces
	parameterString = strings.ReplaceAll(parameterString, "\\,", " ")
	parameters := strings.Split(parameterString, ",")
	if len(parameters) != numIn {
		return emptyConstrainer, gomerr.Configuration(fmt.Sprintf("Expecting %d parameters, but found %d in %v", numIn, len(parameters), parameters))
	}

	in := make([]reflect.Value, numIn)
	for i, parameter := range parameters {
		parameter := strings.ReplaceAll(parameter, " ", ",")
		in[i] = reflect.New(cft.In(i)).Elem()
		if ge := flect.SetValue(in[i], parameter); ge != nil {
			return emptyConstrainer, gomerr.Configuration(fmt.Sprintf("Unable to set input parameter #%d for %s with: %s", i, cft.String(), parameter))
		}
	}

	// The set of "available" values all result in a single response element which is a constraint.Constrainer.
	// If something goes sideways, this will panic (and indicates bug in Gomer).
	results := cfv.Call(in)
	constrainer := results[0].Interface().(constraint.Constrainer)
	built[constraintName+"("+parameterString+")"] = constrainer

	return constrainer, nil
}
