package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

type bindOutFieldToolConfiguration struct {
	DefaultContentType             string
	PerContentTypeMarshalFunctions map[string]Marshal
	DefaultMarshalFunction         Marshal
	BindDirectiveConfiguration
}

func NewBindOutFieldToolConfiguration() bindOutFieldToolConfiguration {
	return bindOutFieldToolConfiguration{
		DefaultContentType:             DefaultContentType,
		PerContentTypeMarshalFunctions: make(map[string]Marshal),
		DefaultMarshalFunction:         json.Marshal,
		BindDirectiveConfiguration:     NewBindDirectiveConfiguration(),
	}
}

// Marshal provides a function to convert the toMarshal to bytes suitable for returning in a response body.
type Marshal func(toMarshal interface{}) ([]byte, error)

var bindOutInstance bindOutFieldTool

func BindOutFieldTool(configuration bindOutFieldToolConfiguration) fields.FieldTool {
	bindOutInstance = bindOutFieldTool{&configuration}
	return fields.ScopingWrapper{bindOutInstance}
}

type bindOutFieldTool struct {
	*bindOutFieldToolConfiguration
}

const bindOutToolName = "http.BindOutFieldTool"

func (b bindOutFieldTool) Name() string {
	return bindOutToolName
}

var hasOutBodyBinding = make(map[string]bool)

// Applier
//
// +                   -> Use field name as value's key. Required if EmptyDirectiveHandling == SkipField
// <name>              -> Use 'name' as value's key (if PayloadBindingPrefix != "", form is similar to header)
// header.<name>       -> Use 'name' as the header for value (if HeaderBindingPrefix == 'header')
// =<static>           -> Static output value
// $<function>         -> Function-derived output value
// ?<directive>        -> Applied iff field.IsZero(). Supports chaining (e.g. "?$foo?=last")
// <directive>&<right> -> Will apply the left directive followed by the right (e.g. "=OutValue&header.X-My-Header)
// -                   -> Explicitly not included in the output
//
// Except for '-', each of the above can be combined with an ",omitempty" or ",includempty" qualifier that acts like
// '-' or '+' respectively if the field's value is its zero Value.
func (b bindOutFieldTool) Applier(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.Applier, gomerr.Gomerr) {
	directive, ok := input.(string)
	if !ok && input != nil {
		return nil, gomerr.Configuration("Expected a string directive").AddAttribute("Input", input)
	}

	if directive == b.SkipFieldDirective || (directive == "" && b.EmptyDirectiveHandling == SkipField) {
		return nil, nil
	}

	//goland:noinspection GoBoolExpressions
	omitEmpty := b.EmptyValueHandlingDefault == OmitEmpty
	if cIndex := strings.IndexByte(directive, ','); cIndex != -1 {
		switch flag := directive[cIndex+1:]; flag {
		case b.OmitEmptyDirective:
			omitEmpty = true
		case b.IncludeEmptyDirective:
			omitEmpty = false
		default:
			return nil, gomerr.Configuration("Unrecognized directive flag: " + flag)
		}

		directive = directive[:cIndex]
	}

	if tIndex := strings.LastIndexAny(directive, "?&"); tIndex != -1 {
		leftDirective := directive[:tIndex]
		if leftDirective == b.PayloadBindingPrefix+b.BindToFieldNameDirective {
			leftDirective = b.PayloadBindingPrefix + structField.Name
		}

		left, ge := b.Applier(structType, structField, leftDirective)
		if ge != nil {
			return nil, gomerr.Configuration("Unable to process 'left' directive: " + leftDirective).Wrap(ge)
		}
		right, ge := b.Applier(structType, structField, directive[tIndex+1:])
		if ge != nil {
			return nil, gomerr.Configuration("Unable to process 'right' directive: " + directive[tIndex+1:]).Wrap(ge)
		}

		var testFn func(reflect.Value) bool
		if directive[tIndex] == '?' {
			testFn = reflect.Value.IsZero
		} else { // '&'
			testFn = func(reflect.Value) bool { return false }
		}

		return fields.ApplyAndTestApplier{structField.Name, left, testFn, right}, nil
	}

	if directive == b.PayloadBindingPrefix+b.BindToFieldNameDirective {
		return bindToMapApplier{structField.Name, omitEmpty}, nil
	} else if firstChar := directive[0]; firstChar == '=' {
		return fields.ValueApplier{structField.Name, directive[1:]}, nil // don't include the '='
	} else if firstChar == '$' {
		if directive[1] == '.' {
			return fields.MethodApplier{structField.Name, directive[2:]}, nil
		} else {
			fn := fields.GetFieldFunction(directive) // include the '$'
			if fn == nil {
				return nil, gomerr.Configuration("Field function not found: " + directive)
			}
			return fields.FunctionApplier{structField.Name, fn}, nil
		}
	} else if strings.HasPrefix(directive, b.HeaderBindingPrefix) {
		headerName := directive[len(b.HeaderBindingPrefix):]
		if headerName == b.BindToFieldNameDirective {
			headerName = structField.Name
		}
		return bindResponseHeaderApplier{headerName}, nil
	} else if directive == b.BodyBindingDirective {
		if structField.Type != byteSliceType {
			return nil, gomerr.Configuration("Body field must be of type []byte, not: " + structField.Type.String())
		}
		hasOutBodyBinding[structType.String()] = true
		return bodyOutApplier{}, nil
	} else if directive != "" {
		return bindToMapApplier{directive, omitEmpty}, nil
	} else {
		return nil, gomerr.Configuration("Do not know how to handle empty string as binding directive")
	}
}

type bindToMapApplier struct {
	name      string
	omitempty bool
}

func (b bindToMapApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	if fieldValue.IsZero() && b.omitempty {
		return nil
	}

	outMap := toolContext[outMapKey].(map[string]interface{})

	switch fieldValue.Kind() {
	case reflect.Struct:
		fs, ge := fields.Get(fieldValue.Type())
		if ge != nil {
			return ge
		}

		structMap := make(map[string]interface{})
		toolContext[outMapKey] = structMap

		if ge = fs.ApplyTools(fieldValue, fields.Application{bindOutToolName, toolContext}); ge != nil {
			return ge
		}

		if len(structMap) > 0 || !b.omitempty {
			outMap[b.name] = structMap
		}

		toolContext[outMapKey] = outMap
	case reflect.Slice:
		fvLen := fieldValue.Len()
		sliceOutput := make([]interface{}, 0, fvLen)
		sliceMap := make(map[string]interface{}, 1)

		for i := 0; i < fvLen; i++ {
			toolContext[outMapKey] = sliceMap
			if ge := b.Apply(structValue, fieldValue.Index(i), toolContext); ge != nil {
				return ge.AddAttribute("Index", i)
			}
			if v, ok := sliceMap[b.name]; ok && v != nil {
				sliceOutput = append(sliceOutput, v)
			}
		}

		if len(sliceOutput) > 0 || !b.omitempty {
			outMap[b.name] = sliceOutput
		}

		toolContext[outMapKey] = outMap
	case reflect.Map:
		if fieldValue.Type().Key().Kind() != reflect.String {
			return gomerr.Configuration("Unable to produce a map without string ")
		}

		mapOutput := make(map[string]interface{}, fieldValue.Len())
		mapMap := make(map[string]interface{})

		iter := fieldValue.MapRange()
		for iter.Next() {
			toolContext[outMapKey] = mapMap
			if ge := b.Apply(structValue, iter.Value(), toolContext); ge != nil {
				return ge.AddAttribute("Key", iter.Key().Interface())
			}
			if v, ok := mapMap[b.name]; ok && v != nil {
				mapOutput[iter.Key().Interface().(string)] = v
			}
		}

		if len(mapOutput) > 0 || !b.omitempty {
			outMap[b.name] = mapOutput
		}

		toolContext[outMapKey] = outMap
	case reflect.Ptr, reflect.Interface:
		if !fieldValue.IsNil() {
			return b.Apply(structValue, fieldValue.Elem(), toolContext)
		} else if b.omitempty {
			return nil
		}
		fallthrough
	default:
		outMap[b.name] = fieldValue.Interface()
	}

	return nil
}

type bindResponseHeaderApplier struct {
	name string
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

func (b bindResponseHeaderApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	if fieldValue.IsZero() {
		return nil // Cannot apply an empty value to a header so returning nil
	}

	fvt := fieldValue.Type()
	if fvt.Kind() == reflect.Ptr {
		fieldValue = fieldValue.Elem()
		fvt = fieldValue.Type()
	}

	var headerVal string
	switch val := fieldValue.Interface().(type) {
	case string:
		headerVal = val
	case int:
		headerVal = strconv.FormatInt(int64(val), 10)
	case int8:
		headerVal = strconv.FormatInt(int64(val), 10)
	case int16:
		headerVal = strconv.FormatInt(int64(val), 10)
	case int32:
		headerVal = strconv.FormatInt(int64(val), 10)
	case int64:
		headerVal = strconv.FormatInt(val, 10)
	case uint:
		headerVal = strconv.FormatUint(uint64(val), 10)
	case uint8:
		headerVal = strconv.FormatUint(uint64(val), 10)
	case uint16:
		headerVal = strconv.FormatUint(uint64(val), 10)
	case uint32:
		headerVal = strconv.FormatUint(uint64(val), 10)
	case uint64:
		headerVal = strconv.FormatUint(val, 10)
	case float32:
		headerVal = strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		headerVal = strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		headerVal = strconv.FormatBool(val)
	default:
		// TODO:p2 handle other builtin types like ints, floats, bools, etc
		if marshaler, ok := val.(Marshaler); ok {
			marshaled, err := marshaler.Marshal()
			if err != nil {
				return gomerr.Marshal("FieldValue", fieldValue).Wrap(err)
			}
			headerVal = string(marshaled)
		} else if stringer, ok := val.(fmt.Stringer); ok {
			headerVal = stringer.String()
		} else {

		}
	}

	toolContext[headersKey].(http.Header).Add(b.name, headerVal)

	return nil
}

type bodyOutApplier struct{}

func (bodyOutApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	toolContext[bodyBytesKey] = fieldValue.Interface()
	return nil
}

var directiveFunctions = map[string]func(reflect.Value) bool{
	"?":    reflect.Value.IsZero,
	"then": func(reflect.Value) bool { return false },
}
