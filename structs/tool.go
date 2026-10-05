package structs

import (
	"reflect"
	"sync"
	"time"
	"unicode"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/id"
)

// TODO: Build a mechanism to generate structs from Smithy models and JSON schema definitions

func ApplyTools(v any, tc ToolContext, tools ...*Tool) gomerr.Gomerr {
	vv, ge := flect.IndirectValue(v, false)
	if ge != nil {
		return gomerr.Unprocessable("unable to apply tools to invalid value", v).Wrap(ge)
	}

	vt := vv.Type()
	if vt.Kind() != reflect.Struct {
		return gomerr.Configuration("can only apply tools to struct (or pointer to struct) types").AddAttribute("type", vt.String())
	}

	appliers, ge := prepare(vt, tools...)
	if ge != nil {
		return ge
	}

	return apply(vv, tc, appliers)
}

type fieldApplier struct {
	name    string
	applier Applier
}

// prepare prepares st for the given tools while holding mu, returning an immutable
// snapshot of the (field, applier) pairs to run. preparedStructs and each preparedStruct
// are read and mutated only under the lock. Process never runs an Applier, so a first
// concurrent use of a type through any tool (the bind in/out tools included) cannot race
// on the shared map or observe a half-built preparedStruct. Returning a snapshot lets
// the appliers run without the lock, so a tool whose Apply recurses into ApplyTools does
// not deadlock on it.
func prepare(st reflect.Type, tools ...*Tool) ([]fieldApplier, gomerr.Gomerr) {
	mu.Lock()
	defer mu.Unlock()

	var eb gomerr.ErrorBatch
	ps := process(st, &eb, tools...)
	if ge := eb.GomerrOrNil(); ge != nil {
		return nil, ge
	}
	if ps == nil {
		return nil, nil
	}

	var plan []fieldApplier
	for _, tool := range tools {
		for _, f := range ps.fields {
			if applier, ok := f.appliers[tool.Id()]; ok {
				plan = append(plan, fieldApplier{f.name, applier})
			}
		}
	}
	return plan, nil
}

func apply(sv reflect.Value, tc ToolContext, plan []fieldApplier) gomerr.Gomerr {
	var eb gomerr.ErrorBatch
	for _, fa := range plan {
		fv := sv.FieldByName(fa.name) // fv should always be valid
		ge := fa.applier.Apply(sv, fv, tc)
		if ge == nil {
			continue
		}

		var fieldName string
		if keyAttr, exists := ge.AttributeLookup("key"); !exists {
			fieldName = fa.name
		} else if key := keyAttr.(string); len(key) > 0 {
			fieldName = fa.name + "." + key
			_ = ge.DeleteAttribute("key")
		} else {
			fieldName = fa.name
		}

		if fieldAttr, exists := ge.AttributeLookup("field"); exists {
			_ = ge.ReplaceAttribute("field", fieldName+"."+fieldAttr.(string))
		} else {
			_ = ge.AddAttribute("field", fieldName)
		}

		eb.Capture(ge)
	}
	return eb.GomerrOrNil()
}

func Preprocess(v any, tools ...*Tool) gomerr.Gomerr {
	vt := flect.IndirectType(v)
	var eb gomerr.ErrorBatch
	mu.Lock()
	ps := process(vt, &eb, tools...)
	mu.Unlock()
	if ps == nil {
		return gomerr.Configuration("invalid type: must be a struct or pointer to struct").AddAttribute("type", vt.String())
	}
	return eb.GomerrOrNil()
}

func NewTool(toolType string, ap ApplierProvider, dp DirectiveProvider) *Tool {
	return &Tool{toolType + "_" + idGen.Generate(), toolType, ap, dp}
}

// Tool contains references to some behavior that can be applied to structs present in an
// application.
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

func (t *Tool) ApplierProvider() ApplierProvider {
	return t.applierProvider
}

func (t *Tool) applierFor(st reflect.Type, sf reflect.StructField) (Applier, gomerr.Gomerr) {
	directive, ok := t.directiveProvider.Get(sf)
	if !ok {
		if h, handles := t.applierProvider.(MissingDirectiveHandler); handles {
			directive = h.DefaultDirective()
		} else {
			return nil, nil
		}
	}
	return applyScopes(t.applierProvider, st, sf, directive)
}

type ApplierProvider interface {
	Applier(structType reflect.Type, structField reflect.StructField, directive string, scope string) (Applier, gomerr.Gomerr)
}

// MissingDirectiveHandler can be implemented by an ApplierProvider to indicate that
// fields without the struct tag should still be processed. The returned string is used
// as the default directive.
type MissingDirectiveHandler interface {
	DefaultDirective() string
}

type DirectiveProvider interface {
	Get(structField reflect.StructField) (string, bool)
}

type StructTagDirectiveProvider struct {
	TagKey string
}

func (s StructTagDirectiveProvider) Get(structField reflect.StructField) (string, bool) {
	return structField.Tag.Lookup(s.TagKey)
}

var (
	idGen           = id.NewBase36IdGenerator(4, id.Chars)
	mu              sync.Mutex
	preparedStructs = map[string]*preparedStruct{}
	timeType        = reflect.TypeFor[time.Time]()
)

func process(st reflect.Type, eb *gomerr.ErrorBatch, tools ...*Tool) *preparedStruct {
	for k := st.Kind(); k != reflect.Struct; k = st.Kind() {
		switch st.Kind() {
		case reflect.Array, reflect.Map, reflect.Pointer, reflect.Slice:
			st = st.Elem()
		default:
			return nil
		}
	}

	// Time structs are a special case, ignore.
	if st == timeType {
		return nil
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
			return ps
		}
	} else {
		toolsForStruct = tools
		ps = &preparedStruct{
			typeName: typeName,
			fields:   make([]*field, 0, st.NumField()),
			applied:  make(map[string]bool, len(toolsForStruct)),
		}
		preparedStructs[ps.typeName] = ps
	}

	// TODO: descend into non-exported if tag value provided?

	for sf := range st.Fields() {
		if sf.Tag.Get("structs") == "ignore" {
			continue
		}

		sft := sf.Type
		switch sft.Kind() {
		case reflect.Struct:
			if subStruct := process(sf.Type, eb, toolsForStruct...); subStruct != nil && sf.Anonymous {
				for _, f := range subStruct.fields {
					ps.addAppliers(f.name, f.appliers)
				}
			}
		case reflect.Array, reflect.Map, reflect.Pointer, reflect.Slice:
			process(sft.Elem(), eb, tools...)
		default:
			// nothing to do with other kinds
		}

		// TODO: Is there a case where we want to interpret a directive on this attribute?
		if unicode.IsLower([]rune(sf.Name)[0]) {
			continue
		}

		appliers := map[string]Applier{}
		for _, tool := range toolsForStruct {
			if applier, ge := tool.applierFor(st, sf); ge != nil {
				eb.Capture(ge)
			} else if applier != nil {
				appliers[tool.Id()] = applier
			}
			ps.applied[tool.Id()] = true
		}
		ps.addAppliers(sf.Name, appliers)
	}

	return ps
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
				}
			}
			return
		}
	}
	ps.fields = append(ps.fields, &field{fieldName, appliersToAdd})
	return
}
