package bind

import (
	"reflect"
	"strings"
	"time"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

var DefaultOutTool = NewOutTool(NewConfiguration(), structs.StructTagDirectiveProvider{"out"})

func Out(v interface{}, outTool *structs.Tool, optional ...*structs.ToolContext) (map[string]interface{}, gomerr.Gomerr) {
	tc := structs.EnsureContext(optional...).Put(OutKey, make(map[string]interface{}))
	if ge := structs.ApplyTools(v, tc, outTool); ge != nil {
		return nil, ge
	}
	return tc.Get(OutKey).(map[string]interface{}), nil
}

// NewOutTool
//
// +                   -> Use field name as value's key. Required if EmptyDirectiveHandling == skipField
// <name>              -> Use 'name' as value's key (if PayloadBindingPrefix != "", form is similar to header)
// =<static>           -> Static output value
// $<function>         -> Function-derived output value
// ?<directive>        -> Applied iff field.IsZero(). Supports chaining (e.g. "?$foo?=last")
// <directive>&<right> -> Will apply the left directive followed by the right (e.g. "=OutValue&header.X-My-Header)
// -                   -> Explicitly not included in the output
//
// Except for '-', each of the above can be combined with an ",omitempty" or ",includempty" qualifier that acts like
// '-' or '+' respectively if the field's value is its zero Value.
func NewOutTool(bindConfig Configuration, dp structs.DirectiveProvider) *structs.Tool {
	var toolName = "bind.OutTool"
	if bindConfig.extension != nil {
		toolName = bindConfig.extension.Type()
	}

	var tool structs.Tool
	tool = *structs.NewTool(toolName, outApplierProvider{bindConfig, &tool}, dp)
	return &tool
}

type outApplierProvider struct {
	Configuration
	tool *structs.Tool
}

func (ap outApplierProvider) Applier(st reflect.Type, sf reflect.StructField, directive string, scope string) (structs.Applier, gomerr.Gomerr) {
	if directive == skipField || (directive == "" && ap.emptyDirective == skipField) {
		return nil, nil
	}

	//goland:noinspection GoBoolExpressions
	omitIfEmpty := ap.emptyValue == omitEmpty
	if cIndex := strings.IndexByte(directive, ','); cIndex != -1 {
		switch flag := directive[cIndex+1:]; flag {
		case omitEmpty:
			omitIfEmpty = true
		case includeEmpty:
			omitIfEmpty = false
		default:
			return nil, gomerr.Configuration("Unrecognized directive flag: " + flag)
		}

		directive = directive[:cIndex]
	}

	if applier, ge := structs.Composite(directive, ap.tool, st, sf); applier != nil || ge != nil {
		return applier, ge
	}

	if directive == includeField || directive == "" { // b.emptyDirectiveHandling must be 'includeField' otherwise would have returned above
		return outApplier{(*ap.toCase)(sf.Name), omitIfEmpty, ap.tool}, nil
	} else if firstChar := directive[0]; firstChar == '=' {
		return structs.ValueApplier{directive[1:]}, nil // don't include the '='
	} else if firstChar == '$' {
		if directive[1] == '.' {
			return structs.StructApplier{directive}, nil
		} else {
			tf := structs.GetToolFunction(directive) // include the '$'
			if tf == nil {
				return nil, gomerr.Configuration("Function not found: " + directive)
			}
			return tf, nil
		}
	}

	if ap.extension != nil {
		if applier, ge := ap.extension.Applier(st, sf, directive, scope); applier != nil || ge != nil {
			return applier, ge
		}
	}

	return outApplier{(*ap.toCase)(directive), omitIfEmpty, ap.tool}, nil
}

type outApplier struct {
	toName    string
	omitempty bool
	tool      *structs.Tool
}

func (a outApplier) Apply(_ reflect.Value, fv reflect.Value, tc *structs.ToolContext) gomerr.Gomerr {
	if fv.IsZero() && a.omitempty {
		return nil
	}

	outData := tc.Get(OutKey).(map[string]interface{})

	switch fv.Kind() {
	case reflect.Struct:
		// Time structs are a special case
		if t, ok := fv.Interface().(time.Time); ok {
			outData[a.toName] = t.Format(time.RFC3339Nano)
			return nil
		}

		structMap := make(map[string]interface{})
		tc.Put(OutKey, structMap)

		if ge := structs.ApplyTools(fv, tc, a.tool); ge != nil {
			return ge
		}

		if len(structMap) > 0 || !a.omitempty {
			outData[a.toName] = structMap
		}

		tc.Put(OutKey, outData)
	case reflect.Slice:
		// []byte types are a special case
		// TODO: should treat other primitive types this way?
		if fv.Type() == byteSliceType {
			outData[a.toName] = fv.Interface() // TODO:p0 This won't work when (later) json marshaled since []byte will be (double) b64 encoded
			return nil
		}

		fvLen := fv.Len()
		sliceOutput := make([]interface{}, 0, fvLen)

		for i := 0; i < fvLen; i++ {
			sliceMap := make(map[string]interface{}, 1)
			tc.Put(OutKey, sliceMap)
			if ge := a.Apply(reflect.Value{}, fv.Index(i), tc); ge != nil {
				return ge.AddAttribute("Index", i)
			}
			if v, ok := sliceMap[a.toName]; ok && v != nil {
				sliceOutput = append(sliceOutput, v)
			}
		}

		if len(sliceOutput) > 0 || !a.omitempty {
			outData[a.toName] = sliceOutput
		}

		tc.Put(OutKey, outData)
	case reflect.Map:
		if fv.Type().Key().Kind() != reflect.String {
			return gomerr.Configuration("Unable to produce a map without string ")
		}
		mapOutput := make(map[string]interface{}, fv.Len())

		iter := fv.MapRange()
		for iter.Next() {
			dummyMap := make(map[string]interface{})
			tc.Put(OutKey, dummyMap)
			if ge := a.Apply(reflect.Value{}, iter.Value(), tc); ge != nil {
				return ge.AddAttribute("Key", iter.Key().Interface())
			}
			if v, ok := dummyMap[a.toName]; ok && v != nil {
				mapOutput[iter.Key().Interface().(string)] = v
			}
		}

		if len(mapOutput) > 0 || !a.omitempty {
			if a.toName == "^" {
				for k, v := range mapOutput {
					outData[k] = v
				}
			} else {
				outData[a.toName] = mapOutput
			}
		}

		tc.Put(OutKey, outData)
	case reflect.Ptr, reflect.Interface:
		if !fv.IsNil() {
			elemApplier := outApplier{
				toName:    a.toName,
				omitempty: false, // the ptr is not empty and we don't want to potentially omit the underlying value
				tool:      a.tool,
			}
			return elemApplier.Apply(reflect.Value{}, fv.Elem(), tc)
		} else if a.omitempty {
			return nil
		}
		fallthrough
	default:
		outData[a.toName] = fv.Interface()
	}

	return nil
}
