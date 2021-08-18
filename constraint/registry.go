package constraint

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

var built = map[string]Constraint{
	"base64":   Base64,
	"empty":    Empty,
	"isregexp": IsRegexp,
	"nil":      Nil,
	"nonempty": NonEmpty,
	"notnil":   NotNil,
	"required": Required,
}

var builders = map[string]interface{}{
	"and":          And,
	"array":        Elements,
	"elements":     Elements,
	"endswith":     EndsWith,
	"entries":      Entries,
	"equals":       Equals,
	"eq":           Equals,
	"float":        FloatCompare,
	"floatbetween": FloatBetween,
	"int":          IntCompare,
	"intbetween":   IntBetween,
	"len":          Length,
	"map":          Map,
	"mapkeys":      MapKeys,
	"mapvalues":    MapValues,
	"maxlen":       MaxLength,
	"minlen":       MinLength,
	"not":          Not,
	"notequals":    NotEquals,
	"neq":          NotEquals,
	"oneof":        OneOf,
	"or":           Or,
	"regexp":       Regexp,
	"slice":        Elements,
	"startswith":   StartsWith,
	"time":         TimeCompare,
	"timebetween":  TimeBetween,
	"typeof":       TypeOf,
	"uint":         UintCompare,
	"uintbetween":  UintBetween,
}

func RegisterEach(constraintsAndBuilders map[string]interface{}) gomerr.Gomerr {
	var errors []gomerr.Gomerr
	for name, cob := range constraintsAndBuilders {
		if ge := Register(name, cob); ge != nil {
			errors = append(errors, ge)
		}
	}
	return gomerr.Batcher(errors)
}

func Register(name string, constraintOrBuilder interface{}) gomerr.Gomerr {
	if name[0] != '$' || len(name) < 2 || len(name) > 64 {
		return gomerr.Configuration("Registered constraint names must start with a '$' symbol and between 2 and 64 characters long")
	}

	if c, isConstraint := constraintOrBuilder.(Constraint); isConstraint {
		built[strings.ToLower(name)] = c
	} else {
		// Check that it looks like a builder
		bv := reflect.ValueOf(constraintOrBuilder)
		if !bv.IsValid() || bv.Kind() != reflect.Func {
			return gomerr.Configuration("Can only register a constraint.Constraint or a constraint.Builder")
		}

		bvt := bv.Type()
		if bvt.NumOut() != 1 || !bvt.Out(0).AssignableTo(constraintType) {
			return gomerr.Configuration("Builder functions must return a single constraint.Constraint value")
		}

		builders[strings.ToLower(name)] = constraintOrBuilder
	}

	return nil
}

func constraintFor(validationsString string, op logicOp /* passing field to support e.g. gte(0) */, field reflect.StructField) (Constraint, gomerr.Gomerr) {
	var c Constraint
	var ok bool
	var constraints []Constraint

	if c, ok = built[validationsString]; ok {
		if c.Type() == op {
			return c, nil
		}
		constraints = append(constraints, c)
		validationsString = ""
	}

	// Examples:
	//  1. len(1,2)
	//  2. required
	//  3. or(required,len(1,2))
	//  4. map(len(3),struct)
	for len(validationsString) > 0 {
		openParenIndex := strings.Index(validationsString, "(")
		commaIndex := strings.Index(validationsString, ",")
		var ge gomerr.Gomerr

		if openParenIndex >= 0 && (commaIndex < 0 || commaIndex >= openParenIndex) { // true for 1 & 3
			constraintName := strings.ToLower(validationsString[:openParenIndex])
			validations := validationsString[openParenIndex+1:] // '1,2)', 'required,len(1,2))

			c, ge = parameterizedConstraint(constraintName, &validations, field)
			if ge != nil {
				return nil, ge
			}

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
				if constraintName == validationsString { // Peek-ahead to avoid recursively searching for an unrecognized value
					if _, ok = built[constraintName]; !ok {
						return nil, gomerr.Configuration("Unrecognized constraint: " + constraintName)
					}
				}
				validationsString = ""
			}

			c, ge = constraintFor(constraintName, none, field)
			if ge != nil {
				return nil, ge
			}
		}

		constraints = append(constraints, c)
	}

	if len(constraints) == 0 {
		return nil, gomerr.Configuration("No constraints found")
	}

	switch op {
	case orOp:
		return Or(constraints...), nil
	case notOp:
		return Not(And(constraints...)), nil
	default:
		return And(constraints...), nil
	}
}

// Called w/ first open paren "consumed", e.g. '1,2)', 'required,len(1,2)'
func parameterizedConstraint(constraintName string, parenthetical *string, field reflect.StructField) (Constraint, gomerr.Gomerr) {
	remainder := *parenthetical
	var accumulator int
	for parenCounter := 1; parenCounter != 0; {
		if closeIndex := strings.Index(remainder, ")"); closeIndex < 0 {
			return nil, gomerr.Configuration("Unable to find a balanced expression: (" + *parenthetical)
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

	switch strings.ToLower(constraintName) {
	case lcAndOp:
		return constraintFor(parametersString, andOp, field)
	case lcOrOp:
		return constraintFor(parametersString, orOp, field)
	case lcNotOp:
		return constraintFor(parametersString, notOp, field)
	default:
		return buildConstraint(constraintName, parametersString, field)
	}
}

func buildConstraint(constraintName, parametersString string, field reflect.StructField) (Constraint, gomerr.Gomerr) {
	cf, ok := builders[constraintName]
	if !ok {
		return nil, gomerr.Configuration("Unknown validation type: " + constraintName)
	}

	cfv := reflect.ValueOf(cf)
	cft := cfv.Type()
	numIn := cft.NumIn()
	isVariadic := cft.IsVariadic()
	if isVariadic {
		numIn -= 1 // we'll handle the last one as a special case
	}

	// find any escaped commas and replace w/ spaces
	parametersString = strings.ReplaceAll(parametersString, "\\,", " ")
	parameters := strings.Split(parametersString, ",")
	parametersLen := len(parameters)
	if isVariadic {
		if parametersLen < numIn {
			return nil, gomerr.Configuration(fmt.Sprintf("Expecting at least %d parameters, but found %d: %v", numIn, parametersLen, parameters))
		}
	} else if parametersLen != numIn {
		return nil, gomerr.Configuration(fmt.Sprintf("Expecting %d parameters, but found %d: %v", numIn, parametersLen, parameters))
	}

	in := make([]reflect.Value, parametersLen)
	dynamicValues := make(map[string]reflect.Value)

	var pIndex int
	for pIndex = 0; pIndex < numIn; pIndex++ {
		pValue, ge := parameterValue(cft.In(pIndex), strings.ReplaceAll(parameters[pIndex], " ", ","), dynamicValues, field)
		if ge != nil {
			return nil, gomerr.Configuration(fmt.Sprintf("Unable to set input parameter %d for '%s' constraint", pIndex, constraintName)).Wrap(ge)
		}
		in[pIndex] = pValue
	}

	if isVariadic {
		pType := cft.In(pIndex).Elem()
		for ; pIndex < parametersLen; pIndex++ {
			pValue, ge := parameterValue(pType, strings.ReplaceAll(parameters[pIndex], " ", ","), dynamicValues, field)
			if ge != nil {
				return nil, gomerr.Configuration(fmt.Sprintf("Unable to set variadic parameter %d for '%s' constraint", pIndex, constraintName)).Wrap(ge)
			}
			in[pIndex] = pValue
		}
	}

	// Builders return a single Constraint value. Registered builders at validated in Register().
	results := cfv.Call(in)
	c := results[0].Interface().(Constraint)
	if len(dynamicValues) > 0 {
		c = &dynamicConstraint{c, dynamicValues}
	}
	return c, nil
}

var (
	constraintType     = reflect.TypeOf((*Constraint)(nil)).Elem()
	nilConstraintValue = reflect.New(constraintType).Elem()
)

func parameterValue(pType reflect.Type, pString string, dynamicValues map[string]reflect.Value, field reflect.StructField) (reflect.Value, gomerr.Gomerr) {
	// Constraint parameter
	if pType == constraintType {
		if pString == "" {
			return nilConstraintValue, nil
		}
		pc, ge := constraintFor(strings.ReplaceAll(pString, ",", "\\,"), none, field) // re-escape commas since will be re-split
		if ge != nil {
			return reflect.Value{}, ge
		}
		return reflect.ValueOf(pc), nil
	}

	// Dynamic parameter
	// TODO: generalize to add support for functions (e.g. $now)
	if strings.HasPrefix(pString, "$.") {
		if pType.Kind() != reflect.Ptr {
			return reflect.Value{}, gomerr.Configuration(fmt.Sprintf("Dynamic value '%s' requires a pointer (or pointer-safe interface{}) input parameter type, found '%s'", pString, pType))
		}

		pv := reflect.New(pType).Elem()
		pv.Set(reflect.New(pType.Elem()))
		dynamicValues[pString] = pv
		return pv, nil
	}

	// Static parameter
	pv := reflect.New(pType).Elem()
	if ge := flect.SetValue(pv, pString); ge != nil {
		return reflect.Value{}, ge
	}
	return pv, nil
}
