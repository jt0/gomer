package constraint

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

var built = map[string]Constraint{
	"base64":   Base64,
	"empty":    Empty,
	"nil":      Nil,
	"notnil":   NotNil,
	"required": Required,
}

var available = map[string]interface{}{
	"and":          And,
	"endswith":     EndsWith,
	"equals":       Equals,
	"eq":           Equals,
	"float":        FloatCompare,
	"floatbetween": FloatBetween,
	"int":          IntCompare,
	"intbetween":   IntBetween,
	"len":          Length,
	"maxlen":       MaxLength,
	"minlen":       MinLength,
	"not":          Not,
	"notequals":    NotEquals,
	"neq":          NotEquals,
	"oneof":        OneOf,
	"or":           Or,
	"regexp":       Regexp,
	"startswith":   StartsWith,
	"typeof":       TypeOf,
	"uint":         UintCompare,
	"uintbetween":  UintBetween,
}

func RegisterConstraints(constraints map[string]Constraint) {
	for validationName, c := range constraints {
		if validationName[0] != '$' || len(validationName) < 2 || len(validationName) > 64 {
			panic("Custom validation names must start with a '$' symbol and between 2 and 64 characters long")
		}

		built[strings.ToLower(validationName)] = c
	}
}

var FieldValidationTool = fields.ScopingWrapper{FieldTool: fieldValidationTool{}}

type fieldValidationTool struct {
	constraint Constraint
}

func (t fieldValidationTool) Name() string {
	return "constraint.FieldValidationTool"
}

func (t fieldValidationTool) New(_ reflect.Type, _ reflect.StructField, input interface{}) (fields.FieldTool, gomerr.Gomerr) {
	if input == nil {
		return nil, nil
	}

	c, ge := constraintFor(input.(string), "")
	if ge != nil {
		return nil, ge.AddAttribute("Validations", input)
	}

	return fieldValidationTool{c}, nil
}

func (t fieldValidationTool) Apply(_ reflect.Value, fieldValue reflect.Value, _ fields.ToolContext) gomerr.Gomerr {
	if fieldValue.Kind() == reflect.Ptr && !fieldValue.IsZero() {
		fieldValue = fieldValue.Elem()
	}

	return t.constraint.Validate(fieldValue.Interface())
}

var emptyConstraint = NewType(nil)

func constraintFor(validationsString string, logicalOp string) (Constraint, gomerr.Gomerr) {
	var constraints []Constraint
	var ovs string

	if built, ok := built[validationsString]; ok {
		if built.Details()[LookupName] == logicalOp {
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
			not := Not(constraints[0])
			built[ovs] = not
			return not, nil
		}
		// ignore "and" and "or" since they simplify to the constraint itself
		return constraints[0], nil
	default:
		switch logicalOp {
		case "or":
			or := Or(constraints...)
			built[ovs] = or
			return or, nil
		case "not":
			not := Not(And(constraints...))
			built[ovs] = not
			return not, nil
		default:
			and := And(constraints...)
			built[ovs] = and
			return and, nil
		}
	}
}

// Called w/ first open paren "consumed", e.g. '1,2)', 'required,len(1,2)'
func parameterizedConstraint(constraintName string, parenthetical *string) (Constraint, gomerr.Gomerr) {
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

func buildConstraint(constraintName, parameterString string) (Constraint, gomerr.Gomerr) {
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

	// The set of "available" values all result in a single response element which is a Constraint.
	// If something goes sideways, this will panic (and indicates a bug in Gomer).
	results := cfv.Call(in)
	constrainer := results[0].Interface().(Constraint)
	built[constraintName+"("+parameterString+")"] = constrainer

	return constrainer, nil
}
