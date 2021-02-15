package id

import (
	"fmt"
	"reflect"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

//goland:noinspection GoNameStartsWithPackageName
var IdFieldTool = fields.ScopingWrapper{FieldTool: idFieldTool{}}

type idFieldTool struct {
	idFunction func(structValue reflect.Value) interface{}
}

func (t idFieldTool) Name() string {
	return "id.IdFieldTool"
}

// TODO: need to consider what to use for the id of a singleton resource w/o an obvious value (e.g. a '/configuration'
//       resource that nonetheless supports GET and PUT operations and may need to redact info)
func (t idFieldTool) New(structType reflect.Type, structField reflect.StructField, input interface{}) (fields.FieldToolApplier, gomerr.Gomerr) {
	if input == nil {
		return nil, nil
	}

	if idf, ok := idFieldForStruct[structType]; ok && !reflect.DeepEqual(idf, structField) {
		return nil, gomerr.Configuration("Can't mark '" + structField.Name + "'as an id field because it conflicts with '" + idf.Name + "'")
	}
	idFieldForStruct[structType] = structField

	if fnName, ok := input.(string); !ok || fnName != "" && fnName[0] != '$' && fnName != "$" {
		return nil, gomerr.Configuration("Invalid Id function. Must start with a '$'.").AddAttribute("Field", structField.Name)
	} else if fnName == "" {
		// If there's no idFunction, the presumption is the application will handle setting the value itself. Treat as a no-op
		return noopIdFieldTool, nil
	}

	idFunction := fields.GetFieldFunction(input.(string))
	if idFunction == nil {
		return nil, gomerr.Configuration("Id function not found").AddAttributes("Field", structField.Name, "Function", idFunction)
	}

	return idFieldTool{idFunction}, nil
}

func (t idFieldTool) Apply(structValue reflect.Value, fieldValue reflect.Value, _ fields.ToolContext) gomerr.Gomerr {
	if t.idFunction == nil {
		return nil
	}

	if !fieldValue.IsValid() || !fieldValue.CanSet() {
		return gomerr.Unprocessable("Field is not assignable", fieldValue)
	}

	defaultValue := t.idFunction(structValue)
	if ge := flect.SetValue(fieldValue, defaultValue); ge != nil {
		return gomerr.Unprocessable("Unable to set field to default value", defaultValue).Wrap(ge)
	}

	return nil
}

var (
	idFieldForStruct = make(map[reflect.Type]reflect.StructField)
	noopIdFieldTool  = idFieldTool{}
)

// NB: see to do above to consider how singleton resources should present an 'id'
func Id(structValue reflect.Value) (string, gomerr.Gomerr) {
	sf, ok := idFieldForStruct[structValue.Type()]
	if !ok {
		return "", gomerr.Unprocessable("Provided value is not a recognized type. Was NewFields() called on it and"+
			" does it have a marked 'id' field?", structValue.Type())
	}

	fv := structValue.FieldByName(sf.Name)
	if !fv.IsValid() {
		return "", gomerr.Unprocessable("Provided struct's 'id' field is not valid", sf.Name)
	}

	id, ok := fv.Interface().(string)
	if !ok {
		stringer, ok := fv.Interface().(fmt.Stringer)
		if !ok {
			return "", gomerr.Unprocessable("Id value does not provide a string representation", fv.Interface())
		}

		id = stringer.String()
	}

	return id, nil
}
