package http

import (
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
)

type BindDirectiveConfiguration struct {
	// Default prefixes for qualified directives
	PathBindingPrefix       string
	HeaderBindingPrefix     string
	QueryParamBindingPrefix string
	PayloadBindingPrefix    string

	// Default values for unqualified directives
	SkipFieldDirective       string
	BindToFieldNameDirective string
	BodyBindingDirective     string

	// Defines how a field's binding be handled if no directive is specified. Default is to skip.
	EmptyDirectiveHandling EmptyDirectiveHandlingType

	OmitEmptyDirective    string
	IncludeEmptyDirective string

	// Defines how an empty value is marshaled unless overridden by OmitEmptyDirective or IncludeEmptyDirective. Default
	// is to omit.
	EmptyValueHandlingDefault EmptyValueHandlingType
}

func NewBindDirectiveConfiguration() BindDirectiveConfiguration {
	return BindDirectiveConfiguration{
		PathBindingPrefix:         DefaultPathBindingPrefix,
		HeaderBindingPrefix:       DefaultHeaderBindingPrefix,
		QueryParamBindingPrefix:   DefaultQueryParamBindingPrefix,
		PayloadBindingPrefix:      DefaultPayloadBindingPrefix,
		SkipFieldDirective:        DefaultSkipFieldDirective,
		BindToFieldNameDirective:  DefaultBindToFieldNameDirective,
		BodyBindingDirective:      DefaultBodyBindingDirective,
		EmptyDirectiveHandling:    DefaultEmptyDirectiveHandling,
		OmitEmptyDirective:        DefaultOmitEmptyDirective,
		IncludeEmptyDirective:     DefaultIncludeEmptyDirective,
		EmptyValueHandlingDefault: DefaultEmptyValueHandlingDefault,
	}
}

type EmptyDirectiveHandlingType int

const (
	SkipField EmptyDirectiveHandlingType = iota
	BindToFieldName
)

type EmptyValueHandlingType int

const (
	OmitEmpty EmptyValueHandlingType = iota
	IncludeEmpty
)

const (
	DefaultContentType               = "application/json"
	DefaultPathBindingPrefix         = "path."
	DefaultHeaderBindingPrefix       = "header."
	DefaultQueryParamBindingPrefix   = "query."
	DefaultPayloadBindingPrefix      = ""
	DefaultSkipFieldDirective        = "-"
	DefaultBindToFieldNameDirective  = "+"
	DefaultBodyBindingDirective      = "body"
	DefaultEmptyDirectiveHandling    = SkipField
	DefaultOmitEmptyDirective        = "omitempty"
	DefaultIncludeEmptyDirective     = "includeempty"
	DefaultEmptyValueHandlingDefault = OmitEmpty

	ContentTypeHeader = "Content-Type"
	AcceptsHeader     = "Accepts"

	AcceptLanguageKey = "$_accept_language"

	pathPartsKey   = "$_path_parts"
	queryParamsKey = "$_query_params"
	headersKey     = "$_headers"
	inMapKey       = "$_in_map"
	outMapKey      = "$_out_map"
	bodyBytesKey   = "$_body_bytes"

	toolsWithContextKey = "$_tools_with_context"
)

var BindFromRequestTools = []fields.FieldTool{
	BindInFieldTool(NewBindInFieldToolConfiguration()),
	constraint.ValidationFieldTool(),
}

func BindFromRequest(request *http.Request, resourceType reflect.Type, subject auth.Subject, scope string) (resource.Resource, gomerr.Gomerr) {
	r, ge := resource.New(resourceType, subject)
	if ge != nil {
		return nil, ge
	}

	tc := fields.AddScopeToContext(scope).
		Add(pathPartsKey, strings.Split(strings.Trim(request.URL.Path, "/"), "/")). // remove any leading or trailing slashes
		Add(queryParamsKey, request.URL.Query()).
		Add(headersKey, request.Header)

	bodyBytes, err := ioutil.ReadAll(request.Body) // TODO:p3 Support streaming rather than using []byte
	if err != nil {
		return nil, gomerr.Internal("Failed to read request body content").Wrap(err)
	}

	if hasInBodyBinding[resourceType.Elem().String()] {
		tc.Add(bodyBytesKey, bodyBytes)
	} else {
		unmarshaled := make(map[string]interface{})

		if len(bodyBytes) > 0 {
			// based on content type, and the absence of any "body" attributes use the proper unmarshaler to put the
			// data into the new resource
			// TODO:p3 Allow applications to provide alternative means to choose an unmarshaler
			contentType := request.Header.Get(ContentTypeHeader)
			unmarshal, ok := bindInInstance.PerContentTypeUnmarshalFunctions[contentType]
			if !ok {
				if bindInInstance.DefaultUnmarshalFunction == nil {
					return nil, gomerr.Unmarshal("Unsupported content-type", contentType, nil)
				}
				unmarshal = bindInInstance.DefaultUnmarshalFunction
			}

			if err = unmarshal(bodyBytes, &unmarshaled); err != nil {
				return nil, gomerr.Unmarshal("Unable to unmarshal data", bodyBytes, unmarshaled).AddAttribute("ContentType", contentType).Wrap(err)
			}
		}

		tc.Add(inMapKey, unmarshaled)
	}

	applications := toApplications(BindFromRequestTools, tc)
	tc.Add(toolsWithContextKey, applications)

	return r, r.ApplyTools(applications...)
}

var BindToResponseTools = []fields.FieldTool{
	BindOutFieldTool(NewBindOutFieldToolConfiguration()),
}

// BindToResponse
// TODO: add support for data format type
func BindToResponse(result reflect.Value, header http.Header, scope string, acceptLanguage string) (output []byte, ge gomerr.Gomerr) {
	tc := fields.AddScopeToContext(scope).Add(headersKey, header).Add(AcceptLanguageKey, acceptLanguage)

	outBodyBinding := hasOutBodyBinding[result.Type().String()]
	if !outBodyBinding {
		tc.Add(outMapKey, make(map[string]interface{}))
	}

	fs, ge := fields.Get(result.Type())
	if ge != nil {
		return nil, ge
	}

	if ge = fs.ApplyTools(result, toApplications(BindToResponseTools, tc)...); ge != nil {
		return nil, ge
	}

	if outBodyBinding {
		return tc[bodyBytesKey].([]byte), nil
	} else {
		// based on content type, and the absence of any "body" attributes use the proper marshaler to put the
		// data into the response bytes
		// TODO:p3 Allow applications to provide alternative means to choose a marshaler
		contentType := header.Get(AcceptsHeader) // TODO:p4 support multi-options
		marshal, ok := bindOutInstance.PerContentTypeMarshalFunctions[contentType]
		if !ok {
			if bindOutInstance.DefaultMarshalFunction == nil {
				return nil, gomerr.Marshal("Unsupported Accepts content type", contentType)
			}
			marshal = bindOutInstance.DefaultMarshalFunction
			contentType = DefaultContentType
		}

		outMap := tc[outMapKey].(map[string]interface{})
		if len(outMap) == 0 && bindOutInstance.EmptyValueHandlingDefault == OmitEmpty {
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

func toApplications(bindTools []fields.FieldTool, tc fields.ToolContext) []fields.Application {
	applications := make([]fields.Application, len(bindTools))
	for i, tool := range bindTools {
		applications[i] = fields.Application{tool.Name(), tc}
	}
	return applications
}
