package fields

import (
	"reflect"
	"unicode"

	"github.com/jt0/gomer/gomerr"
)

func NewFields(structType reflect.Type) (*Fields, gomerr.Gomerr) {
	if structType.Kind() != reflect.Struct {
		return nil, gomerr.Configuration("Input's kind is not a struct. Do you need to call Elem()?").AddAttribute("Kind", structType.Kind().String())
	}

	fields := &Fields{
		fieldMap: make(map[string]*field),
	}

	if errors := fields.processStruct(structType, "", make([]gomerr.Gomerr, 0)); len(errors) > 0 {
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

type Fields struct {
	fieldMap map[string]*field // TODO: should this be ordered instead of a map?

	// To consider...
	//   idField  *field
	//   internalToField map[string]*field
	//   keyFields []*field
}

type field struct {
	name        string
	location    string
	zeroVal     reflect.Value
	toolsByName map[string]FieldTool
}

func (fs *Fields) processStruct(structType reflect.Type, path string, errors []gomerr.Gomerr) []gomerr.Gomerr {
	for i := 0; i < structType.NumField(); i++ {
		structField := structType.Field(i)
		if structField.Type.Kind() == reflect.Struct {
			if structField.Anonymous {
				errors = fs.processStruct(structField.Type, path+structField.Name+"+", errors)
			} else {
				errors = fs.processStruct(structField.Type, path+structField.Name+".", errors)
			}
		}

		if unicode.IsLower([]rune(structField.Name)[0]) {
			continue
		}

		toolsByName := make(map[string]FieldTool)
		for tagKey, toolType := range _tagKeyToFieldToolMap {
			tagValue, ok := structField.Tag.Lookup(tagKey)
			if !ok {
				continue
			}

			tool, ge := toolType.New(structType, structField, tagValue)
			if ge != nil {
				errors = append(errors, ge)
			} else if tool != nil {
				toolsByName[toolType.Name()] = tool
			}
		}

		fs.fieldMap[structField.Name] = &field{
			name:        structField.Name,
			location:    path + structField.Name,
			zeroVal:     reflect.Zero(structField.Type),
			toolsByName: toolsByName,
		}
	}

	return errors
}

type ToolWithContext struct {
	Type    FieldTool
	Context ToolContext
}

// ApplyTools will apply the tool associated with each tool type in the appliers slice in order to each field
// in the struct.
func (fs *Fields) ApplyTools(sv reflect.Value, tools ...ToolWithContext) gomerr.Gomerr {
	if sv.Kind() != reflect.Struct {
		return gomerr.Unprocessable("Not a struct type", sv.Interface())
	}

	// TODO:p0 handle nested structs (embedded seems okay)
	var errors = make([]gomerr.Gomerr, 0)
	for _, field := range fs.fieldMap {
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
