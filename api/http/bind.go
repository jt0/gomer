package http

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"

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
		BindInTool,
		constraint.FieldValidationTool,
	}

	// This slice can be added to (or replaced) as needed
	BindToResponseTools = []fields.FieldTool{
		BindOutTool,
	}

	// Default prefixes for qualified directives
	PathBindingPrefix       = "path."
	HeaderBindingPrefix     = "header."
	QueryParamBindingPrefix = "query."
	PayloadBindingPrefix    = ""

	// Default values for unqualified directives
	SkipFieldDirective       = "-"
	BindToFieldNameDirective = "+"
	BodyBindingDirective     = "body"

	// Defines how a field's binding be handled if no directive is specified. Default is to skip.
	EmptyDirectiveHandling = SkipField

	// NB: If one scope defines a body binding, no other scope can try to access marshaled/unmarshaled data
	hasInBodyBinding  = make(map[string]bool)
	hasOutBodyBinding = make(map[string]bool)
)

type EmptyDirectiveHandlingType int

const (
	SkipField EmptyDirectiveHandlingType = iota
	BindToFieldName
)

const (
	pathPartsKey      = "$_path_parts"
	queryParamsKey    = "$_query_params"
	headersKey        = "$_headers"
	unmarshaledMapKey = "$_unmarshaled_map"
	bodyBytesKey      = "$_body_bytes"

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
			// based on content type, and the absence of any "body" attributes use the proper marshaller to put the data into
			// the new resource
			// TODO:p3 Allow applications to provide alternative means to choose an unmarshaler
			const contentTypeHeader = "Content-Type"
			contentType := request.Header.Get(contentTypeHeader)
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

		tc.Add(unmarshaledMapKey, unmarshaled)
	}

	applications := toApplications(BindFromRequestTools, tc)
	tc.Add(toolsWithContextKey, applications)

	return r, r.ApplyTools(applications...)
}

func toApplications(bindTools []fields.FieldTool, tc fields.ToolContext) []fields.Application {
	applications := make([]fields.Application, len(bindTools))
	for i, tool := range bindTools {
		applications[i] = fields.Application{tool.Name(), tc}
	}
	return applications
}

func BindToResponse(r resource.Resource, successStatus int, c *gin.Context, scope string) (ge gomerr.Gomerr) {
	if successStatus == http.StatusNoContent {
		return nil
	}

	tc := fields.AddScopeToContext(scope).Add(headersKey, c.Writer.Header())

	if hasOutBodyBinding[reflect.TypeOf(r).Elem().String()] {
		defer func() {
			if ge == nil {
				c.Data(successStatus, "", tc[bodyBytesKey].([]byte))
			}
		}()
	} else {
		output := make(map[string]interface{})
		defer func() {
			if ge == nil {
				// TODO:p3 alternative output content types
				c.Render(successStatus, render.IndentedJSON{Data: output})
			}
		}()

		tc.Add(unmarshaledMapKey, output)
	}

	if ge = r.ApplyTools(toApplications(BindToResponseTools, tc)...); ge != nil {
		return ge
	}

	return nil
}
