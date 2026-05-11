package constraint

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

// Constraint Registry
//
// The registry provides the mapping between constraint names used in struct tags and their
// implementations. There are two types of entries:
//
//   - Built-in constraints (the "built" map): pre-constructed Constraint values that take no
//     parameters. Used for simple checks like "required", "nil", "notzero", etc.
//
//   - Builders (the "builders" map): functions that accept parameters and return a Constraint.
//     The function signature determines how parameters from the struct tag are parsed and passed.
//
// # Struct Tag Syntax
//
// Constraints are specified in struct tags using the "validate" key:
//
//	Field Type `validate:"constraint1,constraint2(param1,param2)"`
//
// Multiple constraints separated by commas at the top level are AND'd together.
// Logical operators (and, or, not) can compose constraints:
//
//	`validate:"or(nil,len(1,100))"`       // nil OR length between 1 and 100
//	`validate:"not(zero)"`                // not zero
//	`validate:"len(1,10),elements(len(1,64))"` // length 1-10 AND each element length 1-64
//
// # Builder Functions
//
// A builder is any function that returns a Constraint. Its parameter types determine how the
// tag string parameters are interpreted:
//
//   - Static parameters: parsed from the tag string and converted to the parameter's type
//     using flect.SetValue (supports string, int, float, bool, time.Time, etc.)
//
//   - Constraint parameters: if a parameter's type is Constraint, the corresponding tag string
//     is recursively parsed as a nested constraint expression.
//     Example: Elements(Constraint) registered as "elements" allows `elements(len(1,16))`
//
//   - Dynamic parameters (field references): if a parameter's type is a pointer (e.g. *int64,
//     *any) and the tag value starts with "$.", it creates a dynamic reference to another
//     struct field. The pointer is populated at validation time with the referenced field's
//     current value. Example: `int(gte,$.MinLength)` compares against the MinLength field.
//
//   - Variadic parameters: the last parameter can be variadic (e.g. ...any), allowing a
//     variable number of arguments. Example: OneOf(values ...any) allows `oneof(a,b,c)`.
//
// # Dynamic Parameters ($.FieldName)
//
// Dynamic parameters enable cross-field validation. When a parameter string starts with "$.",
// it references another field in the same struct. The builder's parameter type must be a
// pointer so the value can be injected at validation time.
//
// How it works:
//  1. At parse time: a pointer is allocated and stored in a dynamicValues map.
//  2. The builder function receives this pointer (initially zero-valued).
//  3. At validation time: before the constraint runs, the referenced field's value is read
//     from the struct and written into the pointer via flect.SetValue.
//  4. The constraint's test function dereferences the pointer to get the live value.
//
// Examples:
//
//	// IntCompare signature: func(comparisonType string, compareTo *int64) Constraint
//	Max int `validate:"int(gte,$.Min)"`  // Max must be >= Min
//
//	// Nil signature: func(value *any) Constraint
//	End *time.Time `validate:"or(nil($.Start),time(gte,$.Start))"`
//
//	// FieldTest signature: func(field *any, comparisonType string, compareTo *any) Constraint
//	Mode *string `validate:"or(field($.AccessMode,neq,read_write),required)"`
//
// A *any parameter can accept both dynamic references AND static values. When a static string
// is provided for a *any parameter, it is stored as-is and type-coerced at comparison time.

var built = map[string]Constraint{
	"empty":    Empty,
	"nonempty": NonEmpty,
	"isregexp": IsRegexp,
	"nil":      IsNil,
	"notnil":   IsNotNil,
	"required": IsRequired,
	"zero":     IsZero,
	"notzero":  IsNotZero,
	"true":     IsTrue,
	"false":    IsFalse,
}

var builders = map[string]any{
	"and":          And,
	"array":        Elements,
	"elements":     Elements,
	"endswith":     EndsWith, // static or dynamic values: endswith($.Suffix)
	"entries":      Entries,
	"equals":       Equals,       // static or dynamic values: equals($.Other)
	"eq":           Equals,       // static or dynamic values: eq($.Other)
	"false":        False,        // static or dynamic values: false($.Flag)
	"f":            False,        // static or dynamic values: f($.Flag)
	"field":        Field,        // static or dynamic values: field($.A,op,$.B) or field($.A,op,value)
	"float":        FloatCompare, // static or dynamic values: float(gte,$.Limit)
	"floatbetween": FloatBetween, // static or dynamic values: floatbetween($.Min,$.Max)
	"int":          IntCompare,   // static or dynamic values: int(gte,$.Limit)
	"intbetween":   IntBetween,   // static or dynamic values: intbetween($.Min,$.Max)
	"len":          Length,       // static or dynamic values: len($.Min,$.Max)
	"map":          Map,
	"mapkeys":      MapKeys,
	"mapvalues":    MapValues,
	"maxlen":       MaxLength, // static or dynamic values: maxlen($.Limit)
	"minlen":       MinLength, // static or dynamic values: minlen($.Limit)
	"nil":          Nil,       // static or dynamic values: nil($.Other)
	"not":          Not,
	"notequals":    NotEquals, // static or dynamic values: notequals($.Other)
	"notnil":       NotNil,    // static or dynamic values: notnil($.Other)
	"notzero":      NotZero,   // static or dynamic values: notzero($.Other)
	"neq":          NotEquals, // static or dynamic values: neq($.Other)
	"oneof":        OneOf,
	"or":           Or,
	"regexp":       Regexp,
	"required":     Required, // static or dynamic values: required($.Other)
	"slice":        Elements,
	"startswith":   StartsWith,  // static or dynamic values: startswith($.Prefix)
	"time":         TimeCompare, // static or dynamic values: time(gte,$.Limit)
	"timebetween":  TimeBetween, // static or dynamic values: timebetween($.Start,$.End)
	"true":         True,        // static or dynamic values: true($.Flag)
	"t":            True,        // static or dynamic values: t($.Flag)
	"typeof":       TypeOf,
	"uint":         UintCompare, // static or dynamic values: uint(gte,$.Limit)
	"uintbetween":  UintBetween, // static or dynamic values: uintbetween($.Min,$.Max)
	"zero":         Zero,        // static or dynamic values: zero($.Other)
}

func RegisterEach(constraintsAndBuilders map[string]any) gomerr.Gomerr {
	var errors []gomerr.Gomerr
	for name, cob := range constraintsAndBuilders {
		if ge := Register(name, cob); ge != nil {
			errors = append(errors, ge)
		}
	}
	return gomerr.Batcher(errors)
}

// Register adds a custom constraint or builder to the registry. The name must start with '$'
// and be between 2 and 64 characters. If constraintOrBuilder is a Constraint, it is added to
// the built map (no parameters). If it is a function returning a Constraint, it is added to
// the builders map (parameterized).
func Register(name string, constraintOrBuilder any) gomerr.Gomerr {
	if name[0] != '$' || len(name) < 2 || len(name) > 64 {
		return gomerr.Configuration("registered constraint names must start with a '$' symbol and between 2 and 64 characters long")
	}

	if c, isConstraint := constraintOrBuilder.(Constraint); isConstraint {
		built[strings.ToLower(name)] = c
	} else {
		// Check that it looks like a builder
		bv := reflect.ValueOf(constraintOrBuilder)
		if !bv.IsValid() || bv.Kind() != reflect.Func {
			return gomerr.Configuration("can only register a constraint.Constraint or a constraint.Builder")
		} else if safeCheckBuilderOutput(bv) {
			return gomerr.Configuration("builder functions must return a single constraint.Constraint value")
		}

		builders[strings.ToLower(name)] = constraintOrBuilder
	}

	return nil
}

func safeCheckBuilderOutput(bv reflect.Value) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = bv.IsValid() && !bv.IsNil() // so long as the value is not nil, assume it's okay
		}
	}()

	bvt := bv.Type()
	return bvt.NumOut() != 1 || !bvt.Out(0).AssignableTo(constraintType)
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
						return nil, gomerr.Configuration("unrecognized constraint: " + constraintName)
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
		return nil, gomerr.Configuration("no constraints found")
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
			return nil, gomerr.Configuration("unable to find a balanced expression: (" + *parenthetical)
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
	case andOp:
		return constraintFor(parametersString, andOp, field)
	case orOp:
		return constraintFor(parametersString, orOp, field)
	case notOp:
		return constraintFor(parametersString, notOp, field)
	default:
		return buildConstraint(constraintName, parametersString, field)
	}
}

// buildConstraint looks up the builder function by name, parses the parametersString into
// typed arguments matching the builder's signature, and calls the builder to produce a
// Constraint. If any parameter is a dynamic reference ($.Field), the resulting constraint
// is wrapped in a dynamicConstraint for runtime field resolution.
func buildConstraint(constraintName, parametersString string, field reflect.StructField) (Constraint, gomerr.Gomerr) {
	cf, ok := builders[constraintName]
	if !ok {
		return nil, gomerr.Configuration("unknown validation type: " + constraintName)
	}

	cfv := reflect.ValueOf(cf)
	cft := cfv.Type()
	numIn := cft.NumIn()
	isVariadic := cft.IsVariadic()
	if isVariadic {
		numIn -= 1 // we'll handle the last one as a special case
	}

	// find any escaped commas and replace w/ spaces
	parameters := splitTopLevel(parametersString)
	parametersLen := len(parameters)
	if isVariadic {
		if parametersLen < numIn {
			return nil, gomerr.Configuration(fmt.Sprintf("expecting at least %d parameters, but found %d: %v", numIn, parametersLen, parameters))
		}
	} else if parametersLen != numIn {
		return nil, gomerr.Configuration(fmt.Sprintf("expecting %d parameters, but found %d: %v", numIn, parametersLen, parameters))
	}

	in := make([]reflect.Value, parametersLen)
	dynamicValues := make(map[string]reflect.Value)

	var pIndex int
	for pIndex = 0; pIndex < numIn; pIndex++ {
		pValue, ge := parameterValue(cft.In(pIndex), parameters[pIndex], dynamicValues, field)
		if ge != nil {
			return nil, gomerr.Configuration(fmt.Sprintf("unable to set input parameter %d for '%s' constraint", pIndex, constraintName)).Wrap(ge)
		}
		in[pIndex] = pValue
	}

	if isVariadic {
		pType := cft.In(pIndex).Elem()
		for ; pIndex < parametersLen; pIndex++ {
			pValue, ge := parameterValue(pType, strings.ReplaceAll(parameters[pIndex], " ", ","), dynamicValues, field)
			if ge != nil {
				return nil, gomerr.Configuration(fmt.Sprintf("unable to set variadic parameter %d for '%s' constraint", pIndex, constraintName)).Wrap(ge)
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

// splitTopLevel splits s on commas that are not nested inside parentheses or curly braces.
func splitTopLevel(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(', '{':
			depth++
		case ')', '}':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}

var (
	constraintType     = reflect.TypeOf((*Constraint)(nil)).Elem()
	nilConstraintValue = reflect.New(constraintType).Elem()
)

// parameterValue converts a string from the struct tag into a reflect.Value matching the
// expected parameter type. It handles three cases:
//   - Constraint type: recursively parses pString as a constraint expression.
//   - Dynamic reference ($.Field): allocates a pointer that will be populated at validation
//     time with the referenced field's value.
//   - Static value: converts the string to the target type using flect.SetValue.
func parameterValue(pType reflect.Type, pString string, dynamicValues map[string]reflect.Value, field reflect.StructField) (reflect.Value, gomerr.Gomerr) {
	// Constraint parameter
	if pType == constraintType {
		if pString == "" {
			return nilConstraintValue, nil
		}
		pc, ge := constraintFor(pString, none, field)
		if ge != nil {
			return reflect.Value{}, ge
		}
		return reflect.ValueOf(pc), nil
	}

	// Dynamic parameter
	// TODO: generalize to add support for functions (e.g. $now)
	if strings.HasPrefix(pString, "$.") {
		if pType.Kind() != reflect.Ptr {
			return reflect.Value{}, gomerr.Configuration(fmt.Sprintf("dynamic value '%s' requires a pointer (or pointer-safe any) input parameter type, found '%s'", pString, pType))
		}

		pv := reflect.New(pType).Elem()
		pv.Set(reflect.New(pType.Elem()))
		dynamicValues[pString] = pv
		return pv, nil
	}

	// Static parameter
	pv := reflect.New(pType).Elem()
	if pType.Kind() == reflect.Ptr && pType.Elem().Kind() == reflect.Interface {
		// *any parameter with a static value: wrap the string in a *any
		var v any = pString
		pv.Set(reflect.ValueOf(&v))
	} else if ge := flect.SetValue(pv, pString); ge != nil {
		return reflect.Value{}, ge
	}
	return pv, nil
}
