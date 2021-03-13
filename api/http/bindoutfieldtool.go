package http

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
)

func BindOutFieldTool() fields.FieldTool {
	if bindOutInstance == nil {
		bindOutInstance = fields.ScopingWrapper{bindOutFieldTool{}}
	}
	return bindOutInstance
}

var (
	DefaultOmitEmpty = true

	bindOutInstance fields.FieldTool
)

type bindOutFieldTool struct{}

const bindOutToolName = "http.BindOutFieldTool"

func (b bindOutFieldTool) Name() string {
	return bindOutToolName
}

// <name>        -> Output attribute name for value. If name == "" then name = StructField.Name
// header.<name> -> Output header for value
// =<static>     -> Static output value
// $<function>   -> Dynamic output value
// ?<directive>  -> Applied iff field.IsZero(). Supports chaining (e.g. "?$foo?=last")
// -             -> Explicitly not included in the output
//
// Except for '-', each of the above can be combined with an ",omitempty" qualifier that acts like '-' if the field's
// value is its zero Value.
//
func (b bindOutFieldTool) Applier(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.Applier, gomerr.Gomerr) {
	directive, ok := input.(string)
	if !ok && input != nil {
		return nil, gomerr.Configuration("Expected a string directive").AddAttribute("Input", input)
	}

	//goland:noinspection GoBoolExpressions
	if directive == SkipFieldDirective || directive == "" && EmptyDirectiveHandling == SkipField {
		return nil, nil
	}

	//goland:noinspection GoBoolExpressions
	omitEmpty := DefaultOmitEmpty
	if cIndex := strings.IndexByte(directive, ','); cIndex != -1 {
		switch flag := directive[cIndex+1:]; flag {
		case "omitempty":
			omitEmpty = true
		case "includeempty":
			omitEmpty = false
		default:
			return nil, gomerr.Configuration("Unrecognized directive flag: " + flag)
		}

		directive = directive[:cIndex]
	}

	if qIndex := strings.LastIndexByte(directive, '?'); qIndex != -1 {
		leftDirective := directive[:qIndex]
		if leftDirective == PayloadBindingPrefix+BindToFieldNameDirective {
			leftDirective = PayloadBindingPrefix + structField.Name
		}

		applier, _ := b.Applier(structType, structField, leftDirective)
		ifNotValid, _ := b.Applier(structType, structField, directive[qIndex+1:])

		// TODO:p2 fix isValid() function
		return fields.ApplyAndTestApplier{applier, func(reflect.Value) bool { return false }, ifNotValid}, nil
	}

	if directive == PayloadBindingPrefix+BindToFieldNameDirective {
		return bindToMapApplier{structField.Name, omitEmpty, structField.Type.Kind() == reflect.Ptr}, nil
	} else if firstChar := directive[:1]; firstChar == "=" {
		return fields.ValueApplier{directive[1:]}, nil // don't include the '='
	} else if firstChar == "$" {
		fn := fields.GetFieldFunction(directive) // include the '$'
		if fn == nil {
			return nil, gomerr.Configuration("Field function not found: " + directive)
		}
		return fields.FunctionApplier{fn}, nil
	} else if strings.HasPrefix(directive, HeaderBindingPrefix) {
		headerName := directive[len(HeaderBindingPrefix):]
		if headerName == BindToFieldNameDirective {
			headerName = structField.Name
		}
		return bindResponseHeaderApplier{headerName}, nil
	} else if directive == BodyBindingDirective {
		if structField.Type != byteSliceType {
			return nil, gomerr.Configuration("Body field must be of type []byte, not: " + structField.Type.String())
		}
		hasOutBodyBinding[structType.String()] = true
		return bodyOutApplier{}, nil
	} else if directive != "" {
		return bindToMapApplier{directive, omitEmpty, structField.Type.Kind() == reflect.Ptr}, nil
	} else {
		return nil, gomerr.Configuration("Do not know how to handle empty string as binding directive")
	}
}

type bindToMapApplier struct {
	name      string
	omitempty bool
	isPtr     bool
}

func (b bindToMapApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	if fieldValue.IsZero() && b.omitempty {
		return nil
	}

	switch fieldValue.Kind() {
	case reflect.Struct:
	case reflect.Slice:
		unmarshaledMap := toolContext[unmarshaledMapKey].(map[string]interface{})
		fLen := fieldValue.Len()
		mSlice := make([]interface{}, fLen)
		for i := 0; i < fLen; i++ {
			toolContext[unmarshaledMapKey] = make(map[string]interface{})
			if r, ok := fieldValue.Index(i).Interface().(resource.Resource); ok {
				if ge := r.ApplyTools(fields.Application{bindOutToolName, toolContext}); ge != nil {
					return ge.AddAttributes("Field", b.name, "Index", i)
				}
			} else if seFs, ge := fields.Get(fieldValue.Type().Elem()); ge == nil {
				if ge = seFs.ApplyTools(fieldValue.Index(i), fields.Application{bindOutToolName, toolContext}); ge != nil {
					return ge.AddAttributes("Field", b.name, "Index", i)
				}
			} else {
				return ge.AddAttribute("Field", b.name)
			}

			mSlice[i] = toolContext[unmarshaledMapKey]
		}
		unmarshaledMap[b.name] = mSlice
		toolContext[unmarshaledMapKey] = unmarshaledMap
	case reflect.Map:
	default:
		toolContext[unmarshaledMapKey].(map[string]interface{})[b.name] = fieldValue.Interface()
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
