package structs

import (
	"reflect"
	"time"
	"unicode"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/id"
)

// TODO: Build a mechanism to generate structs from Smithy models and JSON schema definitions

func ApplyTools(v interface{}, tc *ToolContext, tools ...*Tool) gomerr.Gomerr {
	vv, ge := flect.IndirectValue(v, false)
	if ge != nil {
		return gomerr.Unprocessable("Unable to apply tools to invalid value", v).Wrap(ge)
	}

	vt := vv.Type()
	vts := vt.String()
	if vt.Kind() != reflect.Struct {
		return gomerr.Configuration("Can only apply tools to struct (or pointer to struct) types").AddAttribute("Type", vts)
	}

	ps, ok := preparedStructs[vts]
	if !ok {
		ps = &preparedStruct{
			typeName: vts,
			fields:   make([]*field, 0, vt.NumField()),
			applied:  make(map[string]bool, len(tools)),
		}
		preparedStructs[vts] = ps
	}

	return ps.applyTools(vv, tc, tools...)
}

func Preprocess(v interface{}, tools ...*Tool) gomerr.Gomerr {
	vt := flect.IndirectType(v)
	ps, errors := process(vt, tools...)
	if ps == nil {
		return gomerr.Configuration("Invalid type: must be a struct or pointer to struct").AddAttribute("Type", vt.String())
	}
	return gomerr.Batcher(errors)
}

func NewTool(toolType string, ap ApplierProvider, dp DirectiveProvider) *Tool {
	return &Tool{toolType + "_" + idGen.Generate(), toolType, ap, dp}
}

// Tool contains references to some behavior that can be applied to structs present in an application.
type Tool struct {
	id                string
	toolType          string
	applierProvider   ApplierProvider
	directiveProvider DirectiveProvider
	// around            func(Apply) gomerr.Gomerr
}

func (t *Tool) Id() string {
	return t.id
}

func (t *Tool) Type() string {
	return t.toolType
}

func (t *Tool) applierFor(st reflect.Type, sf reflect.StructField) (Applier, gomerr.Gomerr) {
	return applyScopes(t.applierProvider, st, sf, t.directiveProvider.Get(st, sf))
}

type ApplierProvider interface {
	Applier(structType reflect.Type, structField reflect.StructField, directive string) (Applier, gomerr.Gomerr)
}

type DirectiveProvider interface {
	Get(structType reflect.Type, structField reflect.StructField) string
}

type StructTagDirectiveProvider struct {
	TagKey string
}

func (s StructTagDirectiveProvider) Get(_ reflect.Type, structField reflect.StructField) string {
	return structField.Tag.Get(s.TagKey)
}

var (
	idGen           = id.NewBase36IdGenerator(4, id.Chars)
	preparedStructs = map[string]*preparedStruct{}
	timeType        = reflect.TypeOf((*time.Time)(nil)).Elem()
)

func process(st reflect.Type, tools ...*Tool) (*preparedStruct, []gomerr.Gomerr) {
	for k := st.Kind(); k != reflect.Struct; k = st.Kind() {
		switch st.Kind() {
		case reflect.Array, reflect.Map, reflect.Ptr, reflect.Slice:
			st = st.Elem()
		default:
			return nil, nil
		}
	}

	// Time structs are a special case, ignore.
	if st == timeType {
		return nil, nil
	}

	var toolsForStruct []*Tool
	typeName := st.String()
	ps, ok := preparedStructs[typeName]
	if ok {
		for _, tool := range tools {
			if !ps.applied[tool.Id()] {
				toolsForStruct = append(toolsForStruct, tool)
			}
		}
		if len(toolsForStruct) == 0 {
			// No work to do, return
			return ps, nil
		}
	} else {
		toolsForStruct = tools
		ps = &preparedStruct{
			typeName: typeName,
			fields:   make([]*field, 0, st.NumField()),
			applied:  make(map[string]bool, len(toolsForStruct)),
		}
	}

	errors := make([]gomerr.Gomerr, 0)
	for i := 0; i < st.NumField(); i++ {
		sf := st.Field(i)

		if sf.Tag.Get("structs") == "ignore" {
			continue
		}

		sft := sf.Type
		switch sft.Kind() {
		case reflect.Struct:
			if subStruct, subErrors := process(sf.Type, toolsForStruct...); len(subErrors) > 0 {
				errors = append(errors, subErrors...)
			} else if sf.Anonymous {
				for _, f := range subStruct.fields {
					ps.addAppliers(f.name, f.appliers)
				}
			}
		case reflect.Array, reflect.Map, reflect.Ptr, reflect.Slice:
			if sft.String() == "*resource.metadata" {
				println("Found", sft.String())
			}
			_, subErrors := process(sft.Elem(), tools...)
			errors = append(errors, subErrors...)
		}

		// TODO: Is there a case where we want to interpret a directive on this attribute?
		if unicode.IsLower([]rune(sf.Name)[0]) {
			continue
		}

		appliers := map[string]Applier{}
		for _, tool := range toolsForStruct {
			if applier, ge := tool.applierFor(st, sf); ge != nil {
				errors = append(errors, ge)
			} else if applier != nil {
				appliers[tool.Id()] = applier
			}
		}
		ps.addAppliers(sf.Name, appliers)
	}

	if len(errors) == 0 {
		preparedStructs[ps.typeName] = ps
	}

	return ps, errors
}

type preparedStruct struct {
	typeName string
	fields   []*field
	applied  map[string]bool // tool id -> true (if applied)
}

type field struct {
	name     string
	appliers map[string]Applier
}

func (ps *preparedStruct) addAppliers(fieldName string, appliersToAdd map[string]Applier) {
	for _, f := range ps.fields {
		if f.name == fieldName {
			for toolId, toAdd := range appliersToAdd {
				if _, hasApplier := f.appliers[toolId]; !hasApplier {
					f.appliers[toolId] = toAdd
					ps.applied[toolId] = true
				}
			}
			return
		}
	}

	ps.fields = append(ps.fields, &field{fieldName, appliersToAdd})
	return
}

// ApplyTools will apply the tool associated with each tool type in the appliers slice, in order, to each value in sv.
func (ps *preparedStruct) applyTools(sv reflect.Value, tc *ToolContext, tools ...*Tool) gomerr.Gomerr {
	var errors []gomerr.Gomerr
	for _, tool := range tools {
		if !ps.applied[tool.Id()] {
			// TODO:p3 verify all tools applied....
			if _, pErrors := process(sv.Type(), tools...); len(pErrors) > 0 {
				return gomerr.Batcher(pErrors)
			}
		}

		for _, f := range ps.fields {
			applier, ok := f.appliers[tool.Id()]
			if !ok {
				continue
			}
			fv := sv.FieldByName(f.name) // fv should always be valid
			if ge := applier.Apply(sv, fv, tc); ge != nil {
				errors = append(errors, ge)
			}
		}
	}
	return gomerr.Batcher(errors)
}
