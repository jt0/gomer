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

//goland:noinspection GoNameStartsWithPackageName
type copyIdsFieldTool struct{}

func (t copyIdsFieldTool) Name() string {
	return "id.CopyIdsFieldTool"
}

const SourceValue = "$_source_value"

// Should be ordered in decreasing specificity.
func (t copyIdsFieldTool) Applier(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.Applier, gomerr.Gomerr) {
	idFields, ok := input.(string)
	if !ok {
		return nil, nil
	}

	if sa, exists := structIdFields[structType]; exists {
		return nil, gomerr.Configuration("Already have an id attribute specified for this struct: " + sa.idFields[0])
	}

	applier := idFieldApplier{hidden: make(map[string]bool)}
	structIdFields[structType] = &applier

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
		applier.idFields = append([]string{structField.Name}, applier.idFields...)
	default:
		if applier.idFields[0] != structField.Name {
			applier.idFields = append([]string{structField.Name}, applier.idFields...)
		}
	}

	return applier, nil
}

type idFieldApplier struct {
	idFields []string
	hidden   map[string]bool
}

func (a idFieldApplier) Apply(structValue reflect.Value, _ reflect.Value, toolContext fields.ToolContext) gomerr.Gomerr {
	source, ok := toolContext[SourceValue].(reflect.Value)
	if !ok {
		return nil
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

var structIdFields = make(map[reflect.Type]*idFieldApplier)

func Id(structValue reflect.Value) (string, gomerr.Gomerr) {
	idfa, ok := structIdFields[structValue.Type()]
	if !ok {
		return "", gomerr.Unprocessable(
			"Provided value is not a recognized type. Was NewFields() called on it and does it have a marked 'id' field?",
			structValue.Type())
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
