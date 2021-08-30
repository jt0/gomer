package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
	"github.com/jt0/gomer/structs/bind"
)

// BindToResponseConfiguration
// TODO: add config option mechanism...
type BindToResponseConfiguration struct {
	BindConfiguration bind.Configuration
	BindDirectiveConfiguration

	defaultContentType             string
	perContentTypeMarshalFunctions map[string]Marshal
	defaultMarshalFunction         Marshal
}

// Marshal provides a function to convert the toMarshal to bytes suitable for returning in a response body.
type Marshal func(toMarshal interface{}) ([]byte, error)

func NewBindToResponseConfiguration() BindToResponseConfiguration {
	return BindToResponseConfiguration{
		BindConfiguration:              bind.NewConfiguration(),
		BindDirectiveConfiguration:     NewBindDirectiveConfiguration(),
		defaultContentType:             DefaultContentType,
		perContentTypeMarshalFunctions: make(map[string]Marshal),
		defaultMarshalFunction:         json.Marshal,
	}
}

var (
	responseTool   *structs.Tool
	responseConfig BindToResponseConfiguration
)

func init() {
	rc := NewBindToResponseConfiguration()
	responseTool = SetBindToResponseConfiguration(rc)
}

func SetBindToResponseConfiguration(responseConfiguration BindToResponseConfiguration) *structs.Tool {
	if responseTool == nil || !reflect.DeepEqual(requestConfig, responseConfiguration) {
		responseConfig = responseConfiguration
		responseConfig.BindConfiguration = bind.CopyConfigurationWithOptions(responseConfig.BindConfiguration, bind.ExtendsWith(bindToResponseExtension{}))
		responseTool = bind.NewOutTool(responseConfig.BindConfiguration, structs.StructTagDirectiveProvider{"out"})
	}
	return responseTool
}

// BindToResponse
// TODO: add support for data format type
func BindToResponse(result reflect.Value, header http.Header, scope string, acceptLanguage string) (output []byte, ge gomerr.Gomerr) {
	tc := structs.ToolContextWithScope(scope).Put(headersKey, header).Put(AcceptLanguageKey, acceptLanguage)

	outBodyBinding := hasOutBodyBinding[result.Type().String()]
	if !outBodyBinding {
		tc.Put(bind.OutKey, make(map[string]interface{}))
	}

	if ge = structs.ApplyTools(result, tc, responseTool); ge != nil {
		return nil, ge
	}

	if outBodyBinding {
		return tc.Get(bodyBytesKey).([]byte), nil
	} else {
		// based on content type, and the absence of any "body" attributes use the proper marshaler to put the
		// data into the response bytes
		// TODO:p3 Allow applications to provide alternative means to choose a marshaler
		contentType := header.Get(AcceptsHeader) // TODO:p4 support multi-options
		marshal, ok := responseConfig.perContentTypeMarshalFunctions[contentType]
		if !ok {
			if responseConfig.defaultMarshalFunction == nil {
				return nil, gomerr.Marshal("Unsupported Accepts content type", contentType)
			}
			marshal = responseConfig.defaultMarshalFunction
			contentType = DefaultContentType
		}

		outMap := tc.Get(bind.OutKey).(map[string]interface{})
		if len(outMap) == 0 && responseConfig.EmptyValueHandlingDefault == OmitEmpty {
			return nil, ge
		}

		bytes, err := marshal(outMap)
		if err != nil {
			return nil, gomerr.Marshal("Unable to marshal data", outMap).AddAttribute("ContentType", contentType).Wrap(err)
		}
		header.Set(ContentTypeHeader, contentType)

		return bytes, nil
	}
}

// bindToResponseExtension
//
// header.<name> -> Header with name <name>
// body          -> Body for the response
type bindToResponseExtension struct{}

func (bindToResponseExtension) Applier(structType reflect.Type, structField reflect.StructField, directive string) (structs.Applier, gomerr.Gomerr) {
	if strings.HasPrefix(directive, responseConfig.HeaderBindingPrefix) {
		headerName := directive[len(responseConfig.HeaderBindingPrefix):]
		if headerName == responseConfig.IncludeField {
			headerName = structField.Name
		}
		return bindResponseHeaderApplier{headerName}, nil
	} else if directive == responseConfig.BindBody {
		if structField.Type != byteSliceType {
			return nil, gomerr.Configuration("Body field must be of type []byte, not: " + structField.Type.String())
		}
		hasOutBodyBinding[structType.String()] = true
		return bodyOutApplier{}, nil
	}

	return nil, nil
}

const bindToResponseToolType = "http.BindToResponseTool"

func (bindToResponseExtension) Type() string {
	return bindToResponseToolType
}

var hasOutBodyBinding = make(map[string]bool)

type bindResponseHeaderApplier struct {
	name string
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

func (b bindResponseHeaderApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext structs.ToolContext) gomerr.Gomerr {
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

	toolContext.Get(headersKey).(http.Header).Add(b.name, headerVal)

	return nil
}

type bodyOutApplier struct{}

func (bodyOutApplier) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext structs.ToolContext) gomerr.Gomerr {
	toolContext.Put(bodyBytesKey, fieldValue.Interface())
	return nil
}

var directiveFunctions = map[string]func(reflect.Value) bool{
	"?":    reflect.Value.IsZero,
	"then": func(reflect.Value) bool { return false },
}
