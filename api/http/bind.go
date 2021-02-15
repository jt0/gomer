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
	"github.com/jt0/gomer/data/dataerr"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
)

const (
	contentTypeHeader = "Content-Type"

	headersTypeKey      = "$_headers_type"
	headersFromRequest  = "headersFromRequest"
	headersFromResponse = "headersFromResponse"
)

func init() {
	// These are the default tag keys for these tools, but an application can set different key values if they'd like or
	// add new entries to the map so long as they do it before invoking BindFromRequest()/BindToResponse().
	fields.TagToFieldToolAssociations(map[string]fields.FieldTool{
		"bind.path":   BindPathTool,
		"bind.query":  BindQueryParamTool,
		"bind.header": BindHeaderTool,
		"bind.body":   BindBodyTool,
	})
}

func BindFromRequest(request *http.Request, resourceType reflect.Type, subject auth.Subject) (resource.Resource, gomerr.Gomerr) {
	r, ge := resource.New(resourceType, subject)
	if ge != nil {
		return nil, ge
	}

	tc := fields.EnsureContext().
		Add(PathPartsKey, strings.Split(strings.Trim(request.URL.Path, "/"), "/")). // remove any leading or trailing slashes
		Add(QueryParamsKey, request.URL.Query()).
		Add(HeadersKey, request.Header).
		Add(headersTypeKey, headersFromRequest)

	// based on content type, and the absence of any "body" attributes use the proper marshaller to put the data into
	// the new resource
	fs, _ := fields.Get(resourceType)
	if len(fs.GetFieldNamesUsingTool(BindBodyTool)) == 0 && request.Body != nil {
		bodyBytes, _ := ioutil.ReadAll(request.Body)

		// TODO:p3 rather than use a switch, probably need a heuristic to determine the right type. Should also
		//         allow applications to specify the expected content type(s) (incl heuristics) and unmarshaler(s)
		switch request.Header.Get(contentTypeHeader) {
		default:
			if ge := JsonUnmarshal(bodyBytes, &r); ge != nil {
				return nil, ge
			}
		}
	} else {
		tc.Add(BodyKey, request.Body)
	}

	return r, r.ApplyTools(
		fields.ToolWithContext{BindPathTool.Name(), tc},
		fields.ToolWithContext{BindQueryParamTool.Name(), tc},
		fields.ToolWithContext{BindHeaderTool.Name(), tc},
		fields.ToolWithContext{BindBodyTool.Name(), tc},
	)
}

// Unmarshal parses the data and stores the result in the value pointed to by ptrToTarget. If ptrToTarget is nil or
// not a pointer, Unmarshal returns a gomerr.Gomerr.
type Unmarshal func(data []byte, ptrToTarget interface{}) gomerr.Gomerr

func JsonUnmarshal(data []byte, target interface{}) gomerr.Gomerr {
	if err := json.Unmarshal(data, target); err != nil {
		return gomerr.Unmarshal("Problem with data", data, target).Wrap(err)
	}
	return nil
}

func BindToResponse(r resource.Resource, successStatus int, c *gin.Context) gomerr.Gomerr {
	var ge gomerr.Gomerr
	if coll, ok := r.(resource.Collection); ok {
		ge = readableCollectionData(coll)
	} else if inst, ok := r.(resource.Instance); ok {
		ge = readableInstanceData(inst)
	} else {
		return gomerr.Unprocessable("Resource is neither a Collection nor Instance", reflect.TypeOf(r))
	}
	if ge != nil {
		return ge
	}

	// TODO:p3 alternative output content types
	c.Render(successStatus, render.IndentedJSON{Data: r})

	return nil
}

func readableCollectionData(c resource.Collection) gomerr.Gomerr {
	twc := fields.ToolWithContext{auth.FieldAccessTool.Name(), auth.AddClearIfDeniedToContext(c.Subject(), auth.ReadPermission)}

	for _, item := range c.Items() {
		if r, ok := item.(resource.Instance); ok {
			ge := r.ApplyTools(twc) // remove if not cleared count == 0?
			if ge != nil {
				return ge
			}
		}
	}

	ge := c.ApplyTools(twc)
	if ge != nil {
		return ge
	}

	return nil
}

func readableInstanceData(i resource.Instance) gomerr.Gomerr {
	tool := fields.ToolWithContext{auth.FieldAccessTool.Name(), auth.AddClearIfDeniedToContext(i.Subject(), auth.ReadPermission)}
	ge := i.ApplyTools(tool)
	if ge != nil {
		return ge
	}

	if tool.Context[auth.NotClearedCount] == 0 {
		return dataerr.PersistableNotFound(i.TypeName(), i.Id()).Wrap(ge)
	}

	return nil
}
