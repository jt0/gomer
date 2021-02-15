package fields

import (
	"reflect"
	"unicode"

	"github.com/jt0/gomer/gomerr"
)

var tagToFieldToolMap = map[string]FieldTool{}

// TagToFieldToolAssociations accepts a set of mappings for struct tag keys to map to a FieldTool to apply at various
// points of a struct's lifecycle. These are additive to others tag/tools that may have been added. To remove a mapping,
// provide a nil value for the corresponding key when calling this function.
func TagToFieldToolAssociations(associations map[string]FieldTool) {
	for k, v := range associations {
		if v == nil {
			delete(tagToFieldToolMap, k)
		} else {
			tagToFieldToolMap[k] = v
		}
	}
}

var typeToFields = map[reflect.Type]Fields{}

func Get(structType reflect.Type) (Fields, gomerr.Gomerr) {
	fields, ok := typeToFields[structType]
	if !ok {
		var ge gomerr.Gomerr
		if fields, ge = process(structType); ge != nil {
			return nil, ge
		}

		typeToFields[structType] = fields
	}

	return fields, nil
}

func process(structType reflect.Type) (Fields, gomerr.Gomerr) {
	if structType.Kind() != reflect.Struct {
		return nil, gomerr.Configuration("Input's kind is not a struct. Do you need to call Elem()?").AddAttribute("Kind", structType.Kind().String())
	}

	fields, errors := processStruct(structType, "", tagToFieldToolMap)
	if len(errors) > 0 {
		return nil, gomerr.Configuration("Failed to process Fields for " + structType.Name()).Wrap(gomerr.Batcher(errors))
	}

	return fields, nil
}

type Fields map[string][]field // key = toolName, value = list of fields that are applicable to the tool

type field struct {
	structFieldName        string
	fullyQualifiedLocation string
	appliersByName         map[string]FieldToolApplier
}

func processStruct(structType reflect.Type, path string, tagKeyToFieldTool map[string]FieldTool) (Fields, []gomerr.Gomerr) {
	fields := Fields{}
	errors := make([]gomerr.Gomerr, 0)

	for i := 0; i < structType.NumField(); i++ {
		structField := structType.Field(i)
		if structField.Tag.Get("fields") == "ignore" {
			continue
		}

		if structField.Type.Kind() == reflect.Struct {
			var subFields Fields
			var subErrors []gomerr.Gomerr
			if structField.Anonymous {
				subFields, subErrors = processStruct(structField.Type, path+structField.Name+"+", tagKeyToFieldTool)
			} else {
				subFields, subErrors = processStruct(structField.Type, path+structField.Name+".", tagKeyToFieldTool)
			}

			for tool, sub := range subFields {
				fields[tool] = append(fields[tool], sub...)
			}
			errors = append(errors, subErrors...)
		}

		if unicode.IsLower([]rune(structField.Name)[0]) {
			continue
		}

		appliersByName := make(map[string]FieldToolApplier)
		for tagKey, toolType := range tagKeyToFieldTool {
			var newInput interface{}
			if tagValue, ok := structField.Tag.Lookup(tagKey); ok {
				newInput = tagValue
			} else {
				newInput = nil
			}

			tool, ge := toolType.New(structType, structField, newInput)
			if ge != nil {
				errors = append(errors, ge)
			} else if tool != nil {
				appliersByName[toolType.Name()] = tool
			}
		}

		newField := field{
			structFieldName:        structField.Name,
			fullyQualifiedLocation: path + structField.Name,
			appliersByName:         appliersByName,
		}
		for toolName := range appliersByName {
			fields[toolName] = append(fields[toolName], newField)
		}
	}

	return fields, errors
}

type ToolWithContext struct {
	TypeName string
	Context  ToolContext
}

// ApplyTools will apply the tool associated with each tool type in the appliers slice in order to each field
// in the struct.
func (fs Fields) ApplyTools(sv reflect.Value, toolWithContexts ...ToolWithContext) gomerr.Gomerr {
	if sv.Kind() != reflect.Struct {
		return gomerr.Unprocessable("Not a struct type", sv.Interface())
	}

	var errors = make([]gomerr.Gomerr, 0)
	for _, toolWithContext := range toolWithContexts {
		for _, usesTool := range fs[toolWithContext.TypeName] {
			fv := sv.FieldByName(usesTool.structFieldName)               // fv should always be valid
			tool, _ := usesTool.appliersByName[toolWithContext.TypeName] // tool should always be found

			if ge := tool.Apply(sv, fv, toolWithContext.Context); ge != nil {
				errors = append(errors, ge.AddAttribute("Field", usesTool.structFieldName))
			}
		}
	}

	return gomerr.Batcher(errors)
}

func (fs Fields) GetFieldNamesUsingTool(tool FieldTool) (names []string) {
	for _, f := range fs[tool.Name()] {
		names = append(names, f.structFieldName)
	}
	return
}
