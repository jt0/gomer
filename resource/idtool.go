package resource

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/id"
	"github.com/jt0/gomer/structs"
)

func RegisterIdGeneratorFieldFunction(idGenerator id.Generator) {
	fn := func(reflect.Value, reflect.Value, *structs.ToolContext) (interface{}, gomerr.Gomerr) {
		return idGenerator.Generate(), nil
	}
	_ = structs.RegisterToolFunction("$id", fn)
}

var DefaultIdFieldTool = NewIdTool(structs.StructTagDirectiveProvider{"id"})

// NewIdTool produces a structs.Applier that will take each of the defined id fields and propagate them to another
// struct value.
// Todo:p3 specify that should be ordered in decreasing specificity.
func NewIdTool(dp structs.DirectiveProvider) *structs.Tool {
	return structs.NewTool("resource.IdTool", idTool{}, dp)
}

type idTool struct{}

func (idTool) Applier(st reflect.Type, sf reflect.StructField, directive string, _ string) (structs.Applier, gomerr.Gomerr) {
	if directive == "" {
		return nil, nil
	}

	var typeName string
	if parts := strings.Split(directive, "/"); len(parts) < 2 {
		typeName = st.String()
	} else if len(parts) == 2 {
		directive = parts[0]
		typeName = parts[1]
	} else {
		return nil, gomerr.Configuration(fmt.Sprintf("Only one explicit type name may be specified, found %d in %s", len(parts)-1, directive))
	}

	applier := copyIdsApplier{hidden: make(map[string]bool)}
	for _, idField := range strings.Split(directive, ",") {
		idField = strings.TrimSpace(idField)
		if idField == "" {
			continue
		}
		if idField[0] == '~' {
			applier.hidden[idField] = true
			idField = idField[1:]
		} else if idField == "+" {
			idField = sf.Name
		}
		applier.idFields = append(applier.idFields, idField)
	}

	switch len(applier.idFields) {
	case 0:
		applier.idFields = []string{sf.Name}
	default:
		if applier.idFields[0] != sf.Name {
			applier.idFields = append([]string{sf.Name}, applier.idFields...)
		}
	}

	if sa, exists := structIdFields[typeName]; exists {
		if !reflect.DeepEqual(*sa, applier) {
			return nil, gomerr.Configuration("Already have an id attribute specified for this struct: " + sa.idFields[0])
		}
	} else {
		structIdFields[typeName] = &applier
	}

	return applier, nil
}

const SourceValue = "$_source_value"

var structIdFields = make(map[string]*copyIdsApplier)

type copyIdsApplier struct {
	idFields []string
	hidden   map[string]bool
}

func (a copyIdsApplier) Apply(sv reflect.Value, _ reflect.Value, tc *structs.ToolContext) gomerr.Gomerr {
	sourceValue, ok := tc.Lookup(SourceValue)
	if !ok {
		return gomerr.Configuration("Missing source for ids to copy")
	}

	source, ok := sourceValue.(reflect.Value)
	if !ok {
		source = reflect.ValueOf(sourceValue)
		if source.Kind() == reflect.Ptr {
			source = source.Elem()
		}
	}

	for _, idField := range a.idFields {
		svf := sv.FieldByName(idField)
		if !svf.IsValid() || !svf.CanSet() {
			return gomerr.Unprocessable("Field is invalid: ", idField)
		}
		svf.Set(source.FieldByName(idField))
	}

	return nil
}

func Id(sv reflect.Value) (string, gomerr.Gomerr) {
	idfa, ok := structIdFields[sv.Type().String()]
	if !ok {
		// TODO: dummy call to just prepare type is kinda...yeah. Maybe need a "Prepare" or something after all.
		_ = structs.ApplyTools(sv, nil, DefaultIdFieldTool)

		idfa, ok = structIdFields[sv.Type().String()]
		if !ok {
			return "", gomerr.Unprocessable("Unprocessed type or no field marked as an 'id'", sv.Type().String())
		}
	}

	fv := sv.FieldByName(idfa.idFields[0])
	if !fv.IsValid() {
		return "", gomerr.Unprocessable("Provided struct's 'id' field is not valid", idfa.idFields[0])
	}

	if idfa.hidden[idfa.idFields[0]] {
		return "**********", nil
	}

	switch t := fv.Interface().(type) {
	case string:
		return t, nil
	case fmt.Stringer:
		return t.String(), nil
	default:
		return "", gomerr.Unprocessable("Id value does not provide a string representation", t)
	}
}
