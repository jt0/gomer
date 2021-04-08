package http

import (
	"encoding/json"
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

// Unmarshal defines afunction that processes the input and stores the result in the value pointed to by ptrToTarget.
// If ptrToTarget is nil, not a pointer, or otherwise unprocessable, Unmarshal returns a gomerr.Gomerr.
type Unmarshal func(toUnmarshal []byte, ptrToTarget interface{}) error

// Marshal provides a function to convert the toMarshal to bytes suitable for returning in a response body.
type Marshal func(toMarshal interface{}) ([]byte, error)

var (
	DefaultContentType = "application/json"

	// Unmarshal functions for specific content types
	PerContentTypeUnmarshalFunctions = make(map[string]Unmarshal)

	// Use this Unmarshal function if nothing else matches. Can be set to nil to only allow known content types.
	DefaultUnmarshalFunction = json.Unmarshal

	// Marshal functions for specific content types
	// TODO:p2 add to BindToResponse
	PerContentTypeMarshalFunctions = make(map[string]Marshal)

	// Use this Marshal function if nothing else matches. Can be set to nil to only allow known content types.
	DefaultMarshalFunction = json.Marshal

	// This slice can be added to (or replaced) as needed
	BindFromRequestTools = []fields.FieldTool{
		BindInFieldTool(),
		constraint.ValidationFieldTool(),
	}

	// This slice can be added to (or replaced) as needed
	BindToResponseTools = []fields.FieldTool{
		BindOutFieldTool(),
	}

	// Default prefixes for qualified directives
	PathBindingPrefix       = "path."
	HeaderBindingPrefix     = "header."
	QueryParamBindingPrefix = "query."
	PayloadBindingPrefix    = ""

	// Default values for unqualified directives
	SkipFieldDirective         = "-"
	BindToFieldNameDirective   = "+"
	BodyBindingDirective       = "body"
	StatusCodeBindingDirective = "statuscode"

	// Defines how a field's binding be handled if no directive is specified. Default is to skip.
	EmptyDirectiveHandling = SkipField

	OmitEmptyDirective    = "omitempty"
	IncludeEmptyDirective = "includeempty"

	// Defines how an empty value is marshaled unless overridden by OmitEmptyDirective or IncludeEmptyDirective. Default
	// is to omit.
	EmptyValueHandlingDefault = OmitEmpty

	// NB: If one scope defines a body binding, no other scope can try to access marshaled/unmarshaled data
	hasInBodyBinding  = make(map[string]bool)
	hasOutBodyBinding = make(map[string]bool)
)

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
	ContentTypeHeader = "Content-Type"
	AcceptsHeader     = "Accepts"

	pathPartsKey   = "$_path_parts"
	queryParamsKey = "$_query_params"
	headersKey     = "$_headers"
	inMapKey       = "$_in_map"
	outMapKey      = "$_out_map"
	bodyBytesKey   = "$_body_bytes"

	toolsWithContextKey = "$_tools_with_context"
)

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
			unmarshal, ok := PerContentTypeUnmarshalFunctions[contentType]
			if !ok {
				if DefaultUnmarshalFunction == nil {
					return nil, gomerr.Unmarshal("Unsupported content-type", contentType, nil)
				}
				unmarshal = DefaultUnmarshalFunction
			}

			if err := unmarshal(bodyBytes, &unmarshaled); err != nil {
				return nil, gomerr.Unmarshal("Unable to unmarshal data", bodyBytes, unmarshaled).AddAttribute("ContentType", contentType).Wrap(err)
			}
		}

		tc.Add(inMapKey, unmarshaled)
	}

	applications := toApplications(BindFromRequestTools, tc)
	tc.Add(toolsWithContextKey, applications)

	return r, r.ApplyTools(applications...)
}

// TODO: add support for data format type

func BindToResponse(r resource.Resource, header http.Header, scope string) (output []byte, ge gomerr.Gomerr) {
	tc := fields.AddScopeToContext(scope).Add(headersKey, header)

	outBodyBinding := hasOutBodyBinding[reflect.TypeOf(r).Elem().String()]
	if !outBodyBinding {
		tc.Add(outMapKey, make(map[string]interface{}))
	}

	if ge = r.ApplyTools(toApplications(BindToResponseTools, tc)...); ge != nil {
		return nil, ge
	}

	if outBodyBinding {
		return tc[bodyBytesKey].([]byte), nil
	} else {
		// based on content type, and the absence of any "body" attributes use the proper marshaler to put the
		// data into the response bytes
		// TODO:p3 Allow applications to provide alternative means to choose a marshaler
		contentType := header.Get(AcceptsHeader) // TODO:p4 support multi-options
		marshal, ok := PerContentTypeMarshalFunctions[contentType]
		if !ok {
			if DefaultUnmarshalFunction == nil {
				return nil, gomerr.Unmarshal("Unsupported Accepts content type", contentType, nil)
			}
			marshal = DefaultMarshalFunction
			contentType = DefaultContentType
		}

		outMap := tc[outMapKey].(map[string]interface{})

		//goland:noinspection GoBoolExpressions
		if len(outMap) == 0 && EmptyValueHandlingDefault == OmitEmpty {
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
