package fields

import (
	"reflect"
	"unicode"

	"github.com/jt0/gomer/gomerr"
)

func NewFields(structType reflect.Type) (Fields, gomerr.Gomerr) {
	if structType.Kind() != reflect.Struct {
		return nil, gomerr.Configuration("Input's kind is not a struct. Do you need to call Elem()?").AddAttribute("Kind", structType.Kind().String())
	}

	fields, errors := processStruct(structType, "")
	if len(errors) > 0 {
		return nil, gomerr.Configuration("Failed to process Fields for " + structType.Name()).Wrap(gomerr.Batcher(errors))
	}

	return fields, nil
}

var fieldFunctions map[string]func(structValue reflect.Value) interface{}

func RegisterFieldFunctions(functions map[string]func(structValue reflect.Value) interface{}) {
	if fieldFunctions == nil {
		fieldFunctions = make(map[string]func(structValue reflect.Value) interface{})
	}

	for fnName, function := range functions {
		if len(fnName) < 2 || len(fnName) > 64 || fnName[0] != '$' {
			panic("Field function names must start with a '$' symbol and between 2 and 64 characters long")
		}

		if fnName[1:2] == "_" {
			panic("Function Name must not start with an underscore")
		}

		fieldFunctions[fnName] = function
	}
}

func GetFieldFunction(functionName string) func(structValue reflect.Value) interface{} {
	return fieldFunctions[functionName]
}

var _tagKeyToFieldToolMap map[string]FieldTool

func SetTagKeyToFieldToolMap(tagKeyToFieldToolMap map[string]FieldTool) {
	_tagKeyToFieldToolMap = make(map[string]FieldTool, len(tagKeyToFieldToolMap))
	for k, v := range tagKeyToFieldToolMap {
		_tagKeyToFieldToolMap[k] = v
	}
}

type Fields []field

type field struct {
	name        string
	location    string
	zeroVal     reflect.Value
	toolsByName map[string]FieldTool
}

func processStruct(structType reflect.Type, path string) (Fields, []gomerr.Gomerr) {
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
				subFields, subErrors = processStruct(structField.Type, path+structField.Name+"+")
			} else {
				subFields, subErrors = processStruct(structField.Type, path+structField.Name+".")
			}
			fields = append(fields, subFields...)
			errors = append(errors, subErrors...)
		}

		if unicode.IsLower([]rune(structField.Name)[0]) {
			continue
		}

		toolsByName := make(map[string]FieldTool)
		for tagKey, toolType := range _tagKeyToFieldToolMap {
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
				toolsByName[toolType.Name()] = tool
			}
		}

		fields = append(fields, field{
			name:        structField.Name,
			location:    path + structField.Name,
			zeroVal:     reflect.Zero(structField.Type),
			toolsByName: toolsByName,
		})
	}

	return fields, errors
}

type ToolWithContext struct {
	Type    FieldTool
	Context ToolContext
}

// ApplyTools will apply the tool associated with each tool type in the appliers slice in order to each field
// in the struct.
func (fs Fields) ApplyTools(sv reflect.Value, tools ...ToolWithContext) gomerr.Gomerr {
	if sv.Kind() != reflect.Struct {
		return gomerr.Unprocessable("Not a struct type", sv.Interface())
	}

	// TODO:p0 handle nested structs (embedded seems okay)
	var errors = make([]gomerr.Gomerr, 0)
	for _, field := range fs {
		if len(field.toolsByName) == 0 {
			continue
		}

		fv := sv.FieldByName(field.name)
		if !fv.IsValid() {
			continue
		}

		for _, tool := range tools {
			toolInstance := field.toolsByName[tool.Type.Name()]
			if toolInstance == nil {
				continue
			}
			if ge := toolInstance.Apply(sv, fv, tool.Context); ge != nil {
				errors = append(errors, ge.AddAttribute("Field", field.name))
			}
		}
	}

	return gomerr.Batcher(errors)
}
