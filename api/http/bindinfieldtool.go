package http

import (
	"encoding/json"
	"net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

type bindInFieldToolConfiguration struct {
	DefaultContentType               string
	PerContentTypeUnmarshalFunctions map[string]Unmarshal
	DefaultUnmarshalFunction         Unmarshal
	BindDirectiveConfiguration
}

func NewBindInFieldToolConfiguration() bindInFieldToolConfiguration {
	return bindInFieldToolConfiguration{
		DefaultContentType:               DefaultContentType,
		PerContentTypeUnmarshalFunctions: make(map[string]Unmarshal),
		DefaultUnmarshalFunction:         json.Unmarshal,
		BindDirectiveConfiguration:       NewBindDirectiveConfiguration(),
	}
}

// Unmarshal defines afunction that processes the input and stores the result in the value pointed to by ptrToTarget.
// If ptrToTarget is nil, not a pointer, or otherwise unprocessable, Unmarshal returns a gomerr.Gomerr.
type Unmarshal func(toUnmarshal []byte, ptrToTarget interface{}) error

var bindInInstance bindInFieldTool

func BindInFieldTool(configuration bindInFieldToolConfiguration) fields.FieldTool {
	bindInInstance = bindInFieldTool{&configuration}
	return fields.ScopingWrapper{bindInInstance}
}

type bindInFieldTool struct {
	*bindInFieldToolConfiguration
}

const bindInToolName = "http.BindInFieldTool"

func (b bindInFieldTool) Name() string {
	return bindInToolName
}

var hasInBodyBinding = make(map[string]bool)

// Applier
//
// <name>        -> Payload input value matching <name>. If name == "" then name = StructField.Name
// path.<n>      -> <n>th path part from the request's URL
// query.<name>  -> Query parameter with name <name>
// header.<name> -> Header with name <name>
// =<static>     -> Application-defined static value
// $<function>   -> Application-defined dynamic value
// ?<directive>  -> Applied iff field.IsZero(). Supports chaining (e.g. "query.aName?header.A-Name?=aDefault")
// -             -> Explicitly not bound from any input
func (b bindInFieldTool) Applier(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.Applier, gomerr.Gomerr) {
	directive, ok := input.(string)
	if !ok && input != nil {
		return nil, gomerr.Configuration("Expected input to be a string directive").AddAttribute("Input", input)
	}

	//goland:noinspection GoBoolExpressions
	if directive == b.SkipFieldDirective || directive == "" && b.EmptyDirectiveHandling == SkipField {
		return nil, nil
	}

	if qIndex := strings.LastIndexByte(directive, '?'); qIndex != -1 {
		leftDirective := directive[:qIndex]
		if leftDirective == b.PayloadBindingPrefix+b.BindToFieldNameDirective {
			leftDirective = b.PayloadBindingPrefix + structField.Name
		}

		applier, _ := b.Applier(structType, structField, leftDirective)
		ifZero, _ := b.Applier(structType, structField, directive[qIndex+1:])

		return fields.ApplyAndTestApplier{structField.Name, applier, fields.NonZero, ifZero}, nil
	}

	if directive == b.PayloadBindingPrefix+b.BindToFieldNameDirective {
		return bindUnmarshaledApplier{structField.Name}, nil
	} else if firstChar := directive[0]; firstChar == '=' {
		return fields.ValueApplier{structField.Name, directive[1:]}, nil // don't include the '='
	} else if firstChar == '$' {
		fn := fields.GetFieldFunction(directive) // include the '$'
		if fn == nil {
			return nil, gomerr.Configuration("Field function not found: " + directive)
		}
		return fields.FunctionApplier{structField.Name, fn}, nil
	} else if strings.HasPrefix(directive, b.PathBindingPrefix) {
		index, err := strconv.Atoi(directive[len(b.PathBindingPrefix):])
		if err != nil {
			return nil, gomerr.Configuration("Expected numeric index value for path binding, received: " + directive)
		}
		return bindPathApplier{index}, nil
	} else if strings.HasPrefix(directive, b.QueryParamBindingPrefix) {
		queryParamName := directive[len(b.QueryParamBindingPrefix):]
		if queryParamName == b.BindToFieldNameDirective {
			queryParamName = structField.Name
		}
		return bindQueryParamApplier{queryParamName}, nil
	} else if strings.HasPrefix(directive, b.HeaderBindingPrefix) {
		headerName := directive[len(b.HeaderBindingPrefix):]
		if headerName == b.BindToFieldNameDirective {
			headerName = structField.Name
		}
		return bindRequestHeaderApplier{headerName}, nil
	} else if directive == b.BodyBindingDirective {
		if structField.Type != byteSliceType {
			return nil, gomerr.Configuration("Body field must be of type []byte, not: " + structField.Type.String())
		}
		hasInBodyBinding[structType.String()] = true
		return bodyInApplier{}, nil
	} else if directive != "" {
		return bindUnmarshaledApplier{directive}, nil
	} else {
		return nil, gomerr.Configuration("Do not know how to handle empty string as binding directive")
	}
}

type bindUnmarshaledApplier struct {
	name string
}

func (b bindUnmarshaledApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	inMap := toolContext[inMapKey].(map[string]interface{})
	value, ok := inMap[b.name]
	if !ok || value == nil {
		return nil
	}

	switch fieldValue.Kind() {
	case reflect.Struct:
		fs, ge := fields.Get(fieldValue.Type())
		if ge != nil {
			return ge
		}

		toolContext[inMapKey] = value
		if ge = fs.ApplyTools(fieldValue, fields.Application{bindInToolName, toolContext}); ge != nil {
			return ge
		}
		toolContext[inMapKey] = inMap
	case reflect.Slice:
		sliceData := value.([]interface{})
		sliceLen := len(sliceData)
		fieldValue.Set(reflect.MakeSlice(reflect.SliceOf(fieldValue.Type().Elem()), sliceLen, sliceLen))

		// Putting each element of the slice into a map so the b.Apply() call can fetch the data back out. Allows us
		// to easily support complex slice elem types.
		dummyMap := make(map[string]interface{}, 1)
		toolContext[inMapKey] = dummyMap
		for i := 0; i < sliceLen; i++ {
			dummyMap[b.name] = sliceData[i]
			if ge := b.Apply(structValue, fieldValue.Index(i), toolContext); ge != nil {
				return ge.AddAttribute("Index", i)
			}
		}
		toolContext[inMapKey] = inMap
	case reflect.Map:
		mapData := value.(map[string]interface{})
		fieldValue.Set(reflect.MakeMap(fieldValue.Type()))
		mapElem := reflect.New(fieldValue.Type().Elem())

		for dataKey, dataValue := range mapData {
			toolContext[inMapKey] = dataValue
			if ge := b.Apply(structValue, mapElem, toolContext); ge != nil {
				return ge.AddAttribute("Key", dataKey)
			}
			fieldValue.SetMapIndex(reflect.ValueOf(dataKey), mapElem)
		}
		toolContext[inMapKey] = inMap
	case reflect.Interface:
		return gomerr.Internal("Don't know what interface type to instantiate")
	case reflect.Ptr:
		elemKind := fieldValue.Type().Elem().Kind()
		if elemKind == reflect.Struct || elemKind == reflect.Slice || elemKind == reflect.Map || elemKind == reflect.Ptr {
			// No need to update toolContext
			return b.Apply(structValue, fieldValue.Elem(), toolContext)
		}

		fallthrough
	default:
		if ge := flect.SetValue(fieldValue, value); ge != nil {
			return ge.AddAttributes(inMapKey, b.name)
		}
	}

	return nil
}

type bindPathApplier struct {
	index int
}

func (b bindPathApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	pathParts := toolContext[pathPartsKey].([]string)
	if b.index >= len(pathParts) {
		return nil
	}

	if ge := flect.SetValue(fieldValue, pathParts[b.index]); ge != nil {
		return ge.AddAttributes("PathIndex", b.index)
	}

	return nil
}

type bindQueryParamApplier struct {
	name string
}

func (b bindQueryParamApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	// TODO:p3 support case-insensitive (or canonical) param names
	values, hasValues := toolContext[queryParamsKey].(url.Values)[b.name]
	if !hasValues {
		return nil
	}

	if ge := flect.SetValue(fieldValue, values[0]); ge != nil {
		return ge.AddAttributes("Parameter", b.name)
	}

	return nil
}

type bindRequestHeaderApplier struct {
	name string
}

func (b bindRequestHeaderApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	values, hasValues := toolContext[headersKey].(http.Header)[textproto.CanonicalMIMEHeaderKey(b.name)]
	if !hasValues {
		return nil
	}

	if ge := flect.SetValue(fieldValue, values[0]); ge != nil {
		return ge.AddAttributes("Header", b.name)
	}

	return nil
}

type bodyInApplier struct{}

func (bodyInApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	fieldValue.Set(reflect.ValueOf(toolContext[bodyBytesKey]))
	return nil
}

var byteSliceType = reflect.TypeOf([]byte{})
