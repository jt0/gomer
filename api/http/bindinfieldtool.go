package http

import (
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

func BindInFieldTool() fields.FieldTool {
	if bindInInstance == nil {
		bindInInstance = fields.ScopingWrapper{bindInFieldTool{}}
	}
	return bindInInstance
}

var bindInInstance fields.FieldTool

type bindInFieldTool struct{}

const bindInToolName = "http.BindInFieldTool"

func (b bindInFieldTool) Name() string {
	return bindInToolName
}

// <name>        -> Payload input value matching <name>. If name == "" then name = StructField.Name
// path.<n>      -> <n>th path part from the request's URL
// query.<name>  -> Query parameter with name <name>
// header.<name> -> Header with name <name>
// =<static>     -> Application-defined static value
// $<function>   -> Application-defined dynamic value
// ?<directive>  -> Applied iff field.IsZero(). Supports chaining (e.g. "query.aName?header.A-Name?=aDefault")
// -             -> Explicitly not bound from any input
//
func (b bindInFieldTool) Applier(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.Applier, gomerr.Gomerr) {
	directive, ok := input.(string)
	if !ok && input != nil {
		return nil, gomerr.Configuration("Expected input to be a string directive").AddAttribute("Input", input)
	}

	//goland:noinspection GoBoolExpressions
	if directive == SkipFieldDirective || directive == "" && EmptyDirectiveHandling == SkipField {
		return nil, nil
	}

	if qIndex := strings.LastIndexByte(directive, '?'); qIndex != -1 {
		leftDirective := directive[:qIndex]
		if leftDirective == PayloadBindingPrefix+BindToFieldNameDirective {
			leftDirective = PayloadBindingPrefix + structField.Name
		}

		applier, _ := b.Applier(structType, structField, leftDirective)
		ifZero, _ := b.Applier(structType, structField, directive[qIndex+1:])

		return fields.ApplyAndTestApplier{applier, fields.NonZero, ifZero}, nil
	}

	if directive == PayloadBindingPrefix+BindToFieldNameDirective {
		return bindUnmarshaledApplier{structField.Name}, nil
	} else if firstChar := directive[0]; firstChar == '=' {
		return fields.ValueApplier{directive[1:]}, nil // don't include the '='
	} else if firstChar == '$' {
		fn := fields.GetFieldFunction(directive) // include the '$'
		if fn == nil {
			return nil, gomerr.Configuration("Field function not found: " + directive)
		}
		return fields.FunctionApplier{fn}, nil
	} else if strings.HasPrefix(directive, PathBindingPrefix) {
		index, err := strconv.Atoi(directive[len(PathBindingPrefix):])
		if err != nil {
			return nil, gomerr.Configuration("Expected numeric index value for path binding, received: " + directive)
		}
		return bindPathApplier{index}, nil
	} else if strings.HasPrefix(directive, QueryParamBindingPrefix) {
		queryParamName := directive[len(QueryParamBindingPrefix):]
		if queryParamName == BindToFieldNameDirective {
			queryParamName = structField.Name
		}
		return bindQueryParamApplier{queryParamName}, nil
	} else if strings.HasPrefix(directive, HeaderBindingPrefix) {
		headerName := directive[len(HeaderBindingPrefix):]
		if headerName == BindToFieldNameDirective {
			headerName = structField.Name
		}
		return bindRequestHeaderApplier{headerName}, nil
	} else if directive == BodyBindingDirective {
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

func (b bindUnmarshaledApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	unmarshaledMap := toolContext[unmarshaledMapKey].(map[string]interface{})
	value, ok := unmarshaledMap[b.name]
	if !ok || value == nil {
		return nil
	}

	switch fieldValue.Kind() {
	case reflect.Struct:
	case reflect.Slice:
		vSlice := value.([]interface{})
		vSliceLen := len(vSlice)

		fte := fieldValue.Type().Elem()
		seFs, ge := fields.Get(fte)
		if ge != nil {
			return ge.AddAttribute("Field", b.name)
		}
		fSlice := reflect.MakeSlice(reflect.SliceOf(fte), vSliceLen, vSliceLen)

		for i := 0; i < vSliceLen; i++ {
			toolContext[unmarshaledMapKey] = vSlice[i]
			if ge := seFs.ApplyTools(fSlice.Index(i), fields.Application{bindInToolName, toolContext}); ge != nil {
				return ge.AddAttributes("Field", b.name, "Index", i)
			}
		}
		fieldValue.Set(fSlice)
		toolContext[unmarshaledMapKey] = unmarshaledMap
	case reflect.Map:
	default:
		if ge := flect.SetValue(fieldValue, value); ge != nil {
			return ge.AddAttributes(unmarshaledMapKey, b.name)
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
