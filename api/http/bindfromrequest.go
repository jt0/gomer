package http

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/jt0/gomer/auth"
	bind2 "github.com/jt0/gomer/bind"
	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
	"github.com/jt0/gomer/structs"
)

// BindFromRequestConfiguration
// TODO: add config option mechanism...
type BindFromRequestConfiguration struct {
	BindConfiguration bind2.Configuration
	BindDirectiveConfiguration

	defaultContentType               string
	perContentTypeUnmarshalFunctions map[string]Unmarshal
	defaultUnmarshalFunction         Unmarshal
}

// Unmarshal defines a function that processes the input and stores the result in the value pointed to by ptrToTarget.
// If ptrToTarget is nil, not a pointer, or otherwise unprocessable, Unmarshal returns a gomerr.Gomerr.
type Unmarshal func(toUnmarshal []byte, ptrToTarget interface{}) error

func NewBindFromRequestConfiguration() BindFromRequestConfiguration {
	return BindFromRequestConfiguration{
		BindConfiguration:                bind2.NewConfiguration(),
		BindDirectiveConfiguration:       NewBindDirectiveConfiguration(),
		defaultContentType:               DefaultContentType,
		perContentTypeUnmarshalFunctions: make(map[string]Unmarshal),
		defaultUnmarshalFunction:         json.Unmarshal,
	}
}

var (
	DefaultBindFromRequestTool *structs.Tool
	requestConfig              BindFromRequestConfiguration
)

func init() {
	rc := NewBindFromRequestConfiguration()
	DefaultBindFromRequestTool = SetBindFromRequestConfiguration(rc)
}

func SetBindFromRequestConfiguration(requestConfiguration BindFromRequestConfiguration) *structs.Tool {
	if DefaultBindFromRequestTool == nil || !reflect.DeepEqual(requestConfig, requestConfiguration) {
		requestConfig = requestConfiguration
		requestConfig.BindConfiguration = bind2.CopyConfigurationWithOptions(requestConfig.BindConfiguration, bind2.ExtendsWith(requestExtension{}))
		DefaultBindFromRequestTool = bind2.NewInTool(requestConfig.BindConfiguration, structs.StructTagDirectiveProvider{"in"})
	}
	return DefaultBindFromRequestTool
}

func BindFromRequest(request *http.Request, resourceType reflect.Type, subject auth.Subject, scope string) (resource.Resource, gomerr.Gomerr) {
	r, ge := resource.New(resourceType, subject)
	if ge != nil {
		return nil, ge
	}

	tc := structs.ToolContextWithScope(scope).
		Put(pathPartsKey, strings.Split(strings.Trim(request.URL.Path, "/"), "/")). // remove any leading or trailing slashes
		Put(queryParamsKey, request.URL.Query()).
		Put(headersKey, request.Header)

	bodyBytes, err := ioutil.ReadAll(request.Body) // TODO:p3 Support streaming rather than using []byte
	if err != nil {
		return nil, gomerr.Internal("Failed to read request body content").Wrap(err)
	}

	if hasInBodyBinding[resourceType.Elem().String()] {
		tc.Put(bodyBytesKey, bodyBytes)
	} else {
		unmarshaled := make(map[string]interface{})

		if len(bodyBytes) > 0 {
			// based on content type, and the absence of any "body" attributes use the proper unmarshaler to put the
			// data into the new resource
			// TODO:p3 Allow applications to provide alternative means to choose an unmarshaler
			contentType := request.Header.Get(ContentTypeHeader)
			unmarshal, ok := requestConfig.perContentTypeUnmarshalFunctions[contentType]
			if !ok {
				if requestConfig.defaultUnmarshalFunction == nil {
					return nil, gomerr.Unmarshal("Unsupported content-type", contentType, nil)
				}
				unmarshal = requestConfig.defaultUnmarshalFunction
			}

			if err = unmarshal(bodyBytes, &unmarshaled); err != nil {
				return nil, gomerr.Unmarshal("Unable to unmarshal data", bodyBytes, unmarshaled).AddAttribute("ContentType", contentType).Wrap(err)
			}
		}

		tc.Put(bind2.InKey, unmarshaled)
	}

	return r, structs.ApplyTools(r, tc, DefaultBindFromRequestTool, constraint.DefaultValidationTool)
}

// requestExtension
//
// path.<n>      -> <n>th path part from the request's URL
// query.<name>  -> Query parameter with name <name>
// header.<name> -> Header with name <name>
// body          -> Body of the request
type requestExtension struct{}

func (requestExtension) Applier(structType reflect.Type, structField reflect.StructField, directive string, _ string) (structs.Applier, gomerr.Gomerr) {
	if strings.HasPrefix(directive, requestConfig.PathBindingPrefix) {
		index, err := strconv.Atoi(directive[len(requestConfig.PathBindingPrefix):])
		if err != nil {
			return nil, gomerr.Configuration("Expected numeric index value for path binding, received: " + directive)
		}
		return bindPathApplier{index}, nil
	} else if strings.HasPrefix(directive, requestConfig.QueryParamBindingPrefix) {
		queryParamName := directive[len(requestConfig.QueryParamBindingPrefix):]
		if queryParamName == requestConfig.IncludeField {
			queryParamName = structField.Name
		}
		return bindQueryParamApplier{queryParamName}, nil
	} else if strings.HasPrefix(directive, requestConfig.HeaderBindingPrefix) {
		headerName := directive[len(requestConfig.HeaderBindingPrefix):]
		if headerName == requestConfig.IncludeField {
			headerName = structField.Name
		}
		return bindRequestHeaderApplier{headerName}, nil
	} else if directive == requestConfig.BindBody {
		if structField.Type != byteSliceType {
			return nil, gomerr.Configuration("Body field must be of type []byte, not: " + structField.Type.String())
		}
		hasInBodyBinding[structType.String()] = true
		return bodyInApplier{}, nil
	}

	return nil, nil
}

const bindFromRequestToolType = "http.BindFromRequestTool"

func (requestExtension) Type() string {
	return bindToResponseToolType
}

var hasInBodyBinding = make(map[string]bool)

type bindPathApplier struct {
	index int
}

func (b bindPathApplier) Apply(_ reflect.Value, fv reflect.Value, tc *structs.ToolContext) gomerr.Gomerr {
	pathParts := tc.Get(pathPartsKey).([]string)
	if b.index >= len(pathParts) {
		return nil
	}

	if ge := flect.SetValue(fv, pathParts[b.index]); ge != nil {
		return ge.AddAttributes("PathIndex", b.index)
	}

	return nil
}

type bindQueryParamApplier struct {
	name string
}

func (b bindQueryParamApplier) Apply(_ reflect.Value, fv reflect.Value, tc *structs.ToolContext) gomerr.Gomerr {
	// TODO:p3 support case-insensitive (or canonical) param names
	values, hasValues := tc.Get(queryParamsKey).(url.Values)[b.name]
	if !hasValues {
		return nil
	}

	if ge := flect.SetValue(fv, values[0]); ge != nil {
		return ge.AddAttributes("Parameter", b.name)
	}

	return nil
}

type bindRequestHeaderApplier struct {
	name string
}

func (b bindRequestHeaderApplier) Apply(_ reflect.Value, fv reflect.Value, tc *structs.ToolContext) gomerr.Gomerr {
	values, hasValues := tc.Get(headersKey).(http.Header)[textproto.CanonicalMIMEHeaderKey(b.name)]
	if !hasValues {
		return nil
	}

	if ge := flect.SetValue(fv, values[0]); ge != nil {
		return ge.AddAttributes("Header", b.name)
	}

	return nil
}

type bodyInApplier struct{}

func (bodyInApplier) Apply(_ reflect.Value, fv reflect.Value, tc *structs.ToolContext) gomerr.Gomerr {
	fv.Set(reflect.ValueOf(tc.Get(bodyBytesKey)))
	return nil
}

var byteSliceType = reflect.TypeOf([]byte{})
