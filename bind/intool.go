package bind

import (
	"reflect"
	"strconv"
	"time"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

var DefaultInTool = NewInTool(NewConfiguration(), structs.StructTagDirectiveProvider{"in"})

func In(data map[string]interface{}, v interface{}, inTool *structs.Tool, optional ...*structs.ToolContext) gomerr.Gomerr {
	return structs.ApplyTools(v, structs.EnsureContext(optional...).Put(InKey, data), inTool)
}

// NewInTool
//
// <name>              -> Default input value matching <name>. If name == "" then name = b.DataCase(StructField.Name)
// =<static>           -> Application-defined static value
// $<function>         -> Application-defined dynamic value
// ?<directive>        -> Applied iff field.IsZero(). Supports chaining (e.g. "query.aName?header.A-Source?=aDefault")
// <directive>&<right> -> Applies the left directive followed by the right (e.g. "input&$transform)
// <directive>!<right> -> Applies the left directive and, if it succeeds, the right
// -                   -> Explicitly not bound from any input
func NewInTool(bindConfig Configuration, dp structs.DirectiveProvider) *structs.Tool {
	var toolName = "bind.InTool"
	if bindConfig.extension != nil {
		toolName = bindConfig.extension.Type()
	}

	var tool structs.Tool
	tool = *structs.NewTool(toolName, inApplierProvider{bindConfig, &tool}, dp)
	return &tool
}

type inApplierProvider struct {
	Configuration
	tool *structs.Tool
}

func (ap inApplierProvider) Applier(st reflect.Type, sf reflect.StructField, directive string) (structs.Applier, gomerr.Gomerr) {
	if directive == skipField || (directive == "" && ap.emptyDirective == skipField) {
		return nil, nil
	}

	if applier, ge := structs.Composite(directive, ap.tool, st, sf); applier != nil || ge != nil {
		return applier, ge
	}

	if directive == includeField || directive == "" { // b.emptyDirective must be 'includeField' otherwise would have returned above
		return inApplier{(*ap.toCase)(sf.Name), ap.tool}, nil
	} else if firstChar := directive[0]; firstChar == '=' {
		return structs.ValueApplier{directive[1:]}, nil // don't include the '='
	} else if firstChar == '$' {
		return structs.ExpressionApplierProvider(st, sf, directive)
	}

	if ap.extension != nil {
		if applier, ge := ap.extension.Applier(st, sf, directive); applier != nil || ge != nil {
			return applier, ge
		}
	}

	return inApplier{directive, ap.tool}, nil
}

type inApplier struct {
	source string
	tool   *structs.Tool
}

var (
	timeType      = reflect.TypeOf((*time.Time)(nil)).Elem()
	byteSliceType = reflect.TypeOf((*[]uint8)(nil)).Elem()
	// uint8SliceType = reflect.TypeOf((*[]uint8)(nil)).Elem()
)

func (a inApplier) Apply(sv reflect.Value, fv reflect.Value, tc *structs.ToolContext) gomerr.Gomerr {
	inData := tc.Get(InKey)
	if inData == nil {
		return nil
	}

	imv := reflect.ValueOf(inData)
	if imv.Kind() != reflect.Map {
		return gomerr.Unprocessable("Expected data map", inData).AddAttribute("Source", a.source)
	}

	mv := imv.MapIndex(reflect.ValueOf(a.source))
	if !mv.IsValid() || mv.IsNil() {
		return nil
	}
	value := mv.Interface()

	switch fvt := fv.Type(); fv.Kind() {
	case reflect.Struct:
		vt := reflect.TypeOf(value)

		// Time structs are a special case
		if stringValue, ok := value.(string); ok && fvt == timeType {
			t, err := time.Parse(time.RFC3339Nano, stringValue)
			if err != nil {
				return gomerr.BadValue(gomerr.GenericBadValueType, a.source, stringValue).AddAttribute("Expected", "RFC3339-formatted string")
			}
			fv.Set(reflect.ValueOf(t)) // TODO: use flect.SetValue instead?
			return nil
		} else if fvt == vt {
			return flect.SetValue(fv, value)
		}

		if vt.Kind() != reflect.Map {
			return gomerr.Unprocessable("Expected data map", value).AddAttribute("Source", a.source)
		}

		tc.Put(InKey, value)
		defer tc.Put(InKey, inData)
		if ge := structs.ApplyTools(fv, tc, a.tool); ge != nil {
			return ge.AddAttribute("Source", a.source)
		}
	case reflect.Slice:
		// []byte types are a special case
		// TODO: should treat other primitive types this way?
		if fvt == byteSliceType {
			if _, ok := value.(string); ok {
				if ge := flect.SetValue(fv, value); ge != nil {
					return ge.AddAttributes("Source", a.source)
				}
				return nil
			} // TODO:p2 treat the rest as raw input data - but may have already been exploded depending on how the rest of the data has been handled
		}

		sliceData, ok := value.([]interface{})
		if !ok {

		}

		sliceLen := len(sliceData)
		fv.Set(reflect.MakeSlice(reflect.SliceOf(fvt.Elem()), sliceLen, sliceLen))

		// Putting each element of the slice into a map so the a.Apply() call can fetch the data back out. Allows us
		// to easily support complex slice elem types.
		defer tc.Put(InKey, inData)
		sliceSource := a.source
		for i := 0; i < sliceLen; i++ {
			a.source = sliceSource + "." + strconv.Itoa(i)
			tc.Put(InKey, map[string]interface{}{a.source: sliceData[i]})
			if ge := a.Apply(sv, fv.Index(i), tc); ge != nil {
				return ge.AddAttribute("Key", strconv.Itoa(i))
			}
		}
	case reflect.Map:
		fv.Set(reflect.MakeMap(fvt))

		iter := reflect.ValueOf(value).MapRange() // Unsure why this needs to be reflected again...
		defer tc.Put(InKey, inData)
		mapSource := a.source
		for iter.Next() {
			a.source = mapSource + "." + iter.Key().Interface().(string)
			tc.Put(InKey, map[string]interface{}{a.source: iter.Value().Interface()})
			mapElem := reflect.New(fvt.Elem()).Elem()
			if ge := a.Apply(sv, mapElem, tc); ge != nil {
				return ge.AddAttribute("Key", iter.Key().String())
			}
			fv.SetMapIndex(iter.Key(), mapElem)
		}
		tc.Put(InKey, inData)
	case reflect.Ptr:
		elemKind := fvt.Elem().Kind()
		if elemKind == reflect.Struct || elemKind == reflect.Slice || elemKind == reflect.Map || elemKind == reflect.Ptr {
			if fv.IsNil() {
				fv.Set(reflect.New(fvt.Elem()))
			}
			// No need to update toolContext
			return a.Apply(sv, fv.Elem(), tc)
		}
		fallthrough
	default:
		if ge := flect.SetValue(fv, value); ge != nil {
			return ge.AddAttributes("Source", a.source)
		}
	}

	return nil
}
