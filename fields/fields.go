package fields

import (
	"reflect"
	"unicode"

	"github.com/jt0/gomer/gomerr"
)

var tagToFieldToolMap = map[string]FieldTool{}

// StructTagToFieldTools accepts a set of mappings for struct tag keys to map to a FieldTool to apply at various
// points of a struct's lifecycle. These are additive to other entries that may have been provided. To remove a
// mapping, provide a nil Value for the corresponding key when calling this Function.
func StructTagToFieldTools(associations map[string]FieldTool) {
	for k, v := range associations {
		if v == nil {
			delete(tagToFieldToolMap, k)
		} else {
			tagToFieldToolMap[k] = v
		}
	}
}

var typeToFields = map[string]Fields{}

func Get(structType reflect.Type) (Fields, gomerr.Gomerr) {
	if fields, ok := typeToFields[structType.Name()]; ok {
		return fields, nil
	}

	return Process(structType)
}

func Process(structType reflect.Type) (Fields, gomerr.Gomerr) {
	if structType.Kind() != reflect.Struct {
		return nil, gomerr.Configuration("Input's kind is not a struct. Do you need to call Elem()?").AddAttribute("Kind", structType.Kind().String())
	}

	fields, errors := processStruct(structType, "", tagToFieldToolMap)
	if len(errors) > 0 {
		return nil, gomerr.Configuration("Failed to process Fields for " + structType.Name()).Wrap(gomerr.Batcher(errors))
	}

	typeToFields[structType.Name()] = fields

	return fields, nil
}

type Fields map[string][]field // key = toolName, value = list of fields that are applicable to the tool

type field struct {
	structFieldName        string
	fullyQualifiedLocation string
	appliersByName         map[string]Applier
}

func processStruct(structType reflect.Type, path string, tagKeyToFieldTool map[string]FieldTool) (Fields, []gomerr.Gomerr) {
	fields := Fields{}
	errors := make([]gomerr.Gomerr, 0)

	for i := 0; i < structType.NumField(); i++ {
		structField := structType.Field(i)
		if structField.Tag.Get("fields") == "ignore" {
			continue
		}

		if structField.Type.Kind() == reflect.Struct && structField.Anonymous {
			subFields, subErrors := processStruct(structField.Type, path+structField.Name+"+", tagKeyToFieldTool)
			for tool, sub := range subFields {
				fields[tool] = append(fields[tool], sub...)
			}
			errors = append(errors, subErrors...)
		}

		if unicode.IsLower([]rune(structField.Name)[0]) {
			continue
		}

		appliersByName := make(map[string]Applier)
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

type Application struct {
	ToolName string
	Context  ToolContext
}

// ApplyTools will apply the tool associated with each tool type in the appliers slice, in order, to each value in sv.
func (fs Fields) ApplyTools(sv reflect.Value, applications ...Application) gomerr.Gomerr {
	var errors = make([]gomerr.Gomerr, 0)
	for _, application := range applications {
		for _, usesTool := range fs[application.ToolName] {
			fv := sv.FieldByName(usesTool.structFieldName)           // fv should always be valid
			tool, _ := usesTool.appliersByName[application.ToolName] // tool should always be found

			if ge := tool.Apply(sv, fv, application.Context); ge != nil {
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
