package fields

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
)

type Fields struct {
	fieldMap map[string]*field
	idField  *field

	//internalToField map[string]*field
	//keyFields []*field
}

type field struct {
	name                 string
	externalName         string
	location             string
	accessBits           fieldAccessBits
	provided             bool
	defaultValue         string
	defaultValueFunction DefaultFunction
	bypassDefaultIfSet   bool
	zeroVal              reflect.Value
	constraints          map[string]constraint.Constrainer
}

var dollarNameConstraint = constraint.And(constraint.Length(2, 64), constraint.StartsWith("$"))

func NewFields(structType reflect.Type) (*Fields, gomerr.Gomerr) {
	fields := &Fields{
		fieldMap: make(map[string]*field),
	}

	if errors := fields.process(structType, "", make([]gomerr.Gomerr, 0)); len(errors) > 0 {
		return nil, gomerr.Configuration("Failed to process Fields for " + structType.Name()).Wrap(gomerr.Batcher(errors))
	}

	return fields, nil
}

func (fs *Fields) process(structType reflect.Type, path string, errors []gomerr.Gomerr) []gomerr.Gomerr {
	for i := 0; i < structType.NumField(); i++ {
		sField := structType.Field(i)
		sFieldName := sField.Name

		if sField.Type.Kind() == reflect.Struct {
			if sField.Anonymous {
				errors = fs.process(sField.Type, path+sFieldName+"+", errors)
			} else {
				errors = fs.process(sField.Type, path+sFieldName+".", errors)
			}
		} else {
			if unicode.IsLower([]rune(sFieldName)[0]) {
				continue
			}

			f := &field{
				name:     sFieldName,
				location: path + sFieldName,
				zeroVal:  reflect.Zero(sField.Type),
			}

			f.externalNameTag(sField.Tag.Get("json"))

			if ge := f.accessTag(sField.Tag.Get("access")); ge != nil {
				errors = append(errors, ge)
			}

			if tag, ok := sField.Tag.Lookup("id"); ok {
				if ge := f.idTag(tag); ge != nil {
					errors = append(errors, ge)
				}

				if fs.idField != nil {
					errors = append(errors, gomerr.Configuration(structType.Name()+" should only one field should have an `id` struct tag"))
				}

				fs.idField = f
			} else {
				f.defaultTag(sField.Tag.Get("default"))
			}

			if ge := f.validateTag(sField.Tag.Get("validate")); ge != nil {
				errors = append(errors, ge)
			}

			if current, exists := fs.fieldMap[f.externalName]; exists {
				if strings.Count(current.location, ".") == 0 && strings.Count(current.location, "+") == 0 {
					fmt.Printf("Info: skipping duplicate field found at '%s'\n", f.location)

					continue
				} else {
					fmt.Printf("Info: replacing duplicate field found at '%s' with '%s'\n", current.location, f.location)
				}
			}

			fs.fieldMap[f.externalName] = f // may override nested (up or down) value. That's okay.
		}
	}

	if path == "" && fs.idField == nil {
		errors = append(errors, gomerr.Configuration(structType.Name()+" does not have a field with an `id` struct tag"))
	}

	return errors
}

func (fs *Fields) ExternalNameToFieldName(externalName string) (string, bool) {
	if field, ok := fs.fieldMap[externalName]; ok {
		return field.name, ok
	} else {
		return externalName, ok
	}
}

func (fs *Fields) ApplyDefaults(v reflect.Value) gomerr.Gomerr {
	// TODO: handle nested/embedded structs
	for _, field := range fs.fieldMap {
		if field.defaultValueFunction == nil && field.defaultValue == "" {
			continue
		}

		fieldValue := v.FieldByName(field.name)
		if field.bypassDefaultIfSet && flect.IsSet(fieldValue) {
			continue
		}

		var defaultValue interface{}
		if field.defaultValueFunction != nil {
			defaultValue = field.defaultValueFunction(v)
		} else {
			defaultValue = field.defaultValue
		}

		if ge := flect.SetValue(fieldValue, defaultValue); ge != nil {
			return gomerr.Configuration("Cannot set field to default").AddAttributes("Field", field.location, "Default", defaultValue).Wrap(ge)
		}
	}

	return nil
}

func (fs *Fields) RemoveNonReadable(v reflect.Value, principal AccessPrincipal) map[string]interface{} { // TODO: clear Fields w/in instance
	readView := make(map[string]interface{})
	for _, field := range fs.fieldMap {
		if field.access(principal, ReadAccess) {
			fieldValue := v.FieldByName(field.name)
			if flect.IsSet(fieldValue) {
				if fieldValue.Kind() == reflect.Ptr {
					fieldValue = fieldValue.Elem()
				}
				readView[field.externalName] = fieldValue.Interface()
			}
		}
	}

	return readView
}

var canSet = constraint.NewConstrainer(func(value interface{}) bool { return value.(reflect.Value).CanSet() }, "CanSet", true)

func (fs *Fields) RemoveNonWritable(v reflect.Value, accessType fieldAccessBits, accessPrincipal AccessPrincipal) gomerr.Gomerr {
	for _, field := range fs.fieldMap {
		// TODO: handle nested/embedded structs
		if field.provided || strings.Contains(field.location, ".") || field.access(accessPrincipal, accessType) {
			continue
		}

		fv := v.FieldByName(field.name)
		if !fv.IsValid() || fv.IsZero() {
			continue
		}

		if ge := gomerr.Test("Field is settable", fv, canSet); ge != nil {
			return ge.AddAttribute("FieldName", field.name)
		}

		fv.Set(field.zeroVal)
	}

	return nil
}

func (fs *Fields) CopyProvided(from, to reflect.Value) gomerr.Gomerr {
	for _, field := range fs.fieldMap {
		// TODO: handle nested/embedded structs
		if !field.provided || strings.Contains(field.location, ".") {
			continue
		}

		ffv := from.FieldByName(field.name)
		if !ffv.IsValid() || ffv.IsZero() {
			continue
		}

		tfv := to.FieldByName(field.name)
		if ge := gomerr.Test("Field is settable", tfv, canSet); ge != nil {
			return ge.AddAttribute("FieldName", field.name)
		}

		tfv.Set(ffv)
	}

	return nil
}

const matchImplicitly = "*"

func (fs *Fields) Validate(v reflect.Value, context string) gomerr.Gomerr {
	var errors []gomerr.Gomerr

	for _, field := range fs.fieldMap {
		if field.constraints == nil || strings.Contains(field.location, ".") { // TODO: handle nested/embedded structs
			continue
		}

		c, ok := field.constraints[context]
		if !ok {
			if c, ok = field.constraints[matchImplicitly]; !ok {
				continue
			}
		}

		fv := v.FieldByName(field.name)
		if fv.Kind() == reflect.Ptr && !fv.IsNil() {
			fv = fv.Elem()
		}

		fvi := fv.Interface()
		if ge := gomerr.Test(field.name, fvi, c); ge != nil {
			errors = append(errors, ge)
		}
	}

	return gomerr.Batcher(errors)
}
