package http

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

const (
	PathPartsKey   = "$_path"
	QueryParamsKey = "$_queryparams"
	HeadersKey     = "$_headers"
	BodyKey        = "$_body"
)

var BindPathTool = bindPathTool{}

type bindPathTool struct {
	index int
}

func (b bindPathTool) Name() string {
	return "http.BindPathTool"
}

func (b bindPathTool) New(_ reflect.Type, _ reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {
	if input == nil {
		return nil, nil
	}

	index, err := strconv.Atoi(input.(string))
	if err != nil {
		return nil, gomerr.Configuration("Expected numeric index value for 'bind.path', received: " + input.(string))
	}
	return bindPathTool{index}, nil
}

func (b bindPathTool) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	pathParts := toolContext[PathPartsKey].([]string)
	if b.index >= len(pathParts) {
		return nil
	}

	if ge := flect.SetValue(fieldValue, pathParts[b.index]); ge != nil {
		return ge.AddAttributes("PathIndex", b.index)
	}

	return nil
}

var BindQueryParamTool = bindQueryParamTool{}

type bindQueryParamTool struct {
	paramName string
	multi     bool // TODO:p3 support multivalue queryParams
}

func (b bindQueryParamTool) Name() string {
	return "http.BindQueryParamTool"
}

func (b bindQueryParamTool) New(_ reflect.Type, structField reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {
	if input == nil {
		return nil, nil
	}

	var bindingName string
	if input.(string) == "" {
		bindingName = structField.Name
	} else {
		bindingName = input.(string)
	}

	return bindQueryParamTool{bindingName, false}, nil // Hard-coding 'multi' to false
}

func (b bindQueryParamTool) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	tcValue, ok := toolContext[QueryParamsKey]
	if !ok {
		return nil
	}
	queryParams := tcValue.(url.Values)

	if b.multi {
		panic("Not yet supported")
	} else {
		qpVal := queryParams.Get(b.paramName)
		if qpVal == "" {
			// Get() will return an empty string if the value is empty or not present. We want to disambiguate, so check
			// directly if the value is ambiguous
			if _, ok := queryParams[b.paramName]; !ok {
				return nil
			}
		}

		if ge := flect.SetValue(fieldValue, qpVal); ge != nil {
			return ge.AddAttributes("QueryParameter", b.paramName)
		}
	}

	return nil
}

// TODO:p4 This bindHeaderTool and bindQueryParmaTool are nearly identical. Consider whether they should remain separate
//  or merged into a single implementation.

var BindHeaderTool = bindHeaderTool{}

type bindHeaderTool struct {
	inName  string
	outName string
	multi   bool // TODO:p3 support multivalue queryParams
}

func (b bindHeaderTool) Name() string {
	return "http.BindHeaderTool"
}

//
// Cases:
//   a      in = a,  out = ""
//   a,     in = a,  out = a
//   a,b    in = a,  out = b
//   ,b     in = "", out = b
//   ""     in = structField.Name, out == ""
//   ,      in = structField.Name, out = structField.Name
func (b bindHeaderTool) New(_ reflect.Type, structField reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {
	if input == nil {
		return nil, nil
	}

	applier := bindHeaderTool{multi: false}
	if input.(string) == "" {
		applier.inName = structField.Name
	} else {
		parts := strings.Split(input.(string), ",")
		applier.inName = parts[0]
		if len(parts) > 1 {
			if parts[1] == "" {
				if parts[0] == "" {
					applier.inName = structField.Name
				}
				applier.outName = applier.inName
			} else {
				applier.outName = parts[1]
			}
		}
	}

	return applier, nil
}

func (b bindHeaderTool) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	tcValue, ok := toolContext[HeadersKey]
	if !ok {
		return nil
	}
	headers := tcValue.(http.Header)

	if b.multi {
		panic("Not yet supported")
	}

	if toolContext[headersTypeKey].(string) == headersFromRequest && b.inName != "" {
		hdrVal := headers.Get(b.inName)
		if hdrVal == "" {
			// Get() will return an empty string if the value is empty or not present. We want to disambiguate, so check
			// directly if the value is ambiguous
			if _, ok := headers[b.inName]; !ok {
				return nil
			}
		}

		if ge := flect.SetValue(fieldValue, hdrVal); ge != nil {
			return ge.AddAttributes("HeaderName", b.inName)
		}
	} else if toolContext[headersTypeKey].(string) == headersFromResponse && b.outName != "" {
		// TBD
	}

	return nil
}

var BindBodyTool = bindBodyTool{}

type bindBodyTool struct{}

func (b bindBodyTool) Name() string {
	return "http.BindBodyTool"
}

var (
	inBodyFieldForStruct = make(map[reflect.Type]reflect.StructField)
	byteSliceType        = reflect.TypeOf([]byte{})
)

func (b bindBodyTool) New(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {
	if input == nil {
		return nil, nil
	}

	if structField.Type == byteSliceType {
		if bf, ok := inBodyFieldForStruct[structType]; ok {
			return nil, gomerr.Configuration("Only one bind.in:\"body\" field allowed. Can't use '" +
				structField.Name + "'because it conflicts with '" + bf.Name + "'")
		}
		inBodyFieldForStruct[structType] = structField
		return bindBodyTool{}, nil
	}

	// TODO:p1 handle alternative attribute names. If attribute name is 'Foo' and binding name is 'bar', should
	//  be handled accordingly. Maybe we need to marshall it into a map type then assign them one-by-one in here...
	//  Can also see if there's a way to provide alt names to the unmarshaler, or set structField values....?
	//  Should also consider whatever we do in the context of replacing the SDK's ddb mapper
	//  --
	//  // If not a []byte, then ase supports "body" and "body,foo" (when attribute is not a []byte) and when location
	//  // is not specified like ",foo", which we presume are body-specific attribute renames

	return nil, nil
}

func (b bindBodyTool) Apply(_ reflect.Value, fieldValue reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	// TODO:p3 Would be nice to support streaming in/out rather than requiring to fully read the body
	bytes, _ := ioutil.ReadAll(toolContext[HeadersKey].(io.ReadCloser))
	fieldValue.Set(reflect.ValueOf(bytes))
	return nil
}

// TODO:p1 Need to have 'omitempty' support. Should have mechanism to set it as the default (omit or include) then
//         specify when one wants the opposite behavior
// var BindOutTool = fields.RegexpWrapper{
// 	// TODO:p3 regexp could support additional url-safe characters
// 	Regexp:       regexp.MustCompile("^(?:(header|body)(?:\\[(.*)])?)?(?:,([\\w]{0,23}))?$"),
// 	RegexpGroups: []string{"", location, locationDetails, name},
// 	FieldTool:    bindOutTool{},
// }

type bindOutTool struct{}

func (bo bindOutTool) Name() string {
	return "http.BindOutTool"
}

func (bo bindOutTool) New(_ reflect.Type, structField reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {

	return nil, nil
}

// scope = req, resp (nothing special, can be any values one wants)
// locations: path,query,header,body,headers(map),queryparams(map) - how compare/contrast w/ json: or xml: or other marshaler?
//            body has a special meaning when type == []byte
//            w/o
// bind:"req:,path;out:Id"
//
// maybe include an "action"-like mechanism based on scope to do something interesting?
//   example: req
//
// omitempty on output?
//

// TODO:p3
//   LinkTool? link:"<curi>(Id,NextToken),<verb?>,<form data>"
//   when setting up could define format (e.g. HAL, etc)
//
// TODO:p3
//   gomer/smithy-gen

// func (f *field) externalNameTag(nameTag string) {
// 	if nameTag == "" {
// 		f.externalName = f.name
// 		return
// 	}
//
// 	nameTagParts := strings.Split(nameTag, ",")
// 	name := strings.TrimSpace(nameTagParts[0])
//
// 	if name == "" {
// 		f.externalName = f.name
// 		return
// 	}
//
// 	f.externalName = name
// }
//
// func (fs *Fields) ExternalNameToFieldName(externalName string) (string, bool) {
// 	if field, ok := fs.fieldMap[externalName]; ok {
// 		return field.name, ok
// 	} else {
// 		return externalName, ok
// 	}
// }

// rationale for a separate struct tag is to make it easier to test if there is a bind.body tag or not against fields
// rather than walking through the fields somehow (no mechanism (currently) to support this)
//
//
// TODO: should be
//       bind.body:"in:<stuff>;out:<stuff>" -> nice due to scopes plus single tag
//       bind.in.body:"<stuff>"             -> semi-consistent
//       bind.in:"body[<stuff>]"            -> fits w/in model but no way (yet) to
//
// Fitting in w/ current model does allow for:
//   1. bind.in:"body,-"                    -> ignore or just have bind.in:"" or no bind.in at all
//   2. bind.in:"-"                         -> should be ignore....
//   3. bind.in:"body[base64]"              -> perform conversion on input
//   4. bind.in:"body,alt_name"             -> like `json:"alt_name"`
//   5. bind.in:"body[omitempty],alt_name"  -> like `json:"alt_name,omitempty"`
