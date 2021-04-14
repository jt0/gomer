package id

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

func CopyIdsFieldTool() fields.FieldTool {
	if copyIdsInstance == nil {
		copyIdsInstance = copyIdsFieldTool{}
	}
	return copyIdsInstance
}

var copyIdsInstance fields.FieldTool

type copyIdsFieldTool struct{}

func (t copyIdsFieldTool) Name() string {
	return "id.CopyIdsFieldTool"
}

const SourceValue = "$_source_value"

var structIdFields = make(map[string]*idFieldApplier)

// Applier for the CopyIdsFieldTool produces a fields.Applier that will take each of the defined id fields and propagate
// them to another structValue. In addition, processing the
// Should be ordered in decreasing specificity.
func (t copyIdsFieldTool) Applier(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.Applier, gomerr.Gomerr) {
	idFields, ok := input.(string)
	if !ok {
		return nil, nil
	}

	var typeName string
	if parts := strings.Split(idFields, "/"); len(parts) < 2 {
		typeName = structType.String()
	} else if len(parts) == 2 {
		idFields = parts[0]
		typeName = parts[1]
	} else {
		return nil, gomerr.Configuration(fmt.Sprintf("Only one explicit type name may be specified, found %d in %s", len(parts)-1, idFields))
	}

	applier := idFieldApplier{hidden: make(map[string]bool)}
	for _, idField := range strings.Split(idFields, ",") {
		idField = strings.TrimSpace(idField)
		if idField == "" {
			continue
		}
		if idField[0] == '~' {
			applier.hidden[idField] = true
			idField = idField[1:]
		}
		applier.idFields = append(applier.idFields, idField)
	}

	switch len(applier.idFields) {
	case 0:
		applier.idFields = []string{structField.Name}
	default:
		if applier.idFields[0] != structField.Name {
			applier.idFields = append([]string{structField.Name}, applier.idFields...)
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

type idFieldApplier struct {
	idFields []string
	hidden   map[string]bool
}

func (a idFieldApplier) Apply(structValue reflect.Value, _ reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	sourceValue, ok := toolContext[SourceValue]
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
		svf := structValue.FieldByName(idField)
		if !svf.IsValid() || !svf.CanSet() {
			return gomerr.Unprocessable("Field is invalid: ", idField)
		}
		svf.Set(source.FieldByName(idField))
	}

	return nil
}

func Id(structValue reflect.Value) (string, gomerr.Gomerr) {
	idfa, ok := structIdFields[structValue.Type().String()]
	if !ok {
		return "", gomerr.Unprocessable("Unprocessed type or no field marked as an 'id'", structValue.Type())
	}

	fv := structValue.FieldByName(idfa.idFields[0])
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
