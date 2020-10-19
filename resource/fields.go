package resource

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

type fields struct {
	fieldMap map[string]*field
	idField  *field

	//internalToField map[string]*field
	//keyFields []*field
}

func newFields(structType reflect.Type) (*fields, gomerr.Gomerr) {
	fields := &fields{
		fieldMap: make(map[string]*field),
	}

	if errors := fields.process(structType, "", make([]gomerr.Gomerr, 0)); len(errors) > 0 {
		return nil, gomerr.Configuration("Failed to process fields for " + structType.Name()).Wrap(gomerr.Batch(errors))
	}

	return fields, nil
}

func (fs *fields) process(structType reflect.Type, path string, errors []gomerr.Gomerr) []gomerr.Gomerr {
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

func (fs *fields) idExternalName() string {
	return fs.idField.externalName
}

func (fs *fields) externalNameToFieldName(externalName string) (string, bool) {
	if field, ok := fs.fieldMap[externalName]; ok {
		return field.name, ok
	} else {
		return externalName, ok
	}
}

func (fs *fields) applyDefaults(i Instance) gomerr.Gomerr {
	resource := reflect.ValueOf(i).Elem() // Support non-pointer types?

	// TODO: handle nested/embedded structs
	for _, field := range fs.fieldMap {
		if field.defaultValueFunction == nil && field.defaultValue == "" {
			continue
		}

		fieldValue := resource.FieldByName(field.name)
		if field.bypassDefaultIfSet && flect.IsSet(fieldValue) {
			continue
		}

		var defaultValue interface{}
		if field.defaultValueFunction != nil {
			defaultValue = field.defaultValueFunction(i)
		} else {
			defaultValue = field.defaultValue
		}

		if ge := flect.SetValue(fieldValue, defaultValue); ge != nil {
			return gomerr.Configuration("Cannot set field to default").AddAttributes("Field", field.location, "Default", defaultValue).Wrap(ge)
		}
	}

	return nil
}

func (fs *fields) removeNonReadable(i Instance) map[string]interface{} { // TODO: clear fields w/in instance
	resource := reflect.ValueOf(i).Elem() // Support non-pointer types?
	readView := make(map[string]interface{})

	for _, field := range fs.fieldMap {
		if field.access(i.Subject().Principal(FieldAccess), readAccess) {
			fieldValue := resource.FieldByName(field.name)
			if flect.IsSet(fieldValue) {
				readView[field.externalName] = fieldValue.Interface()
			}
		}
	}

	return readView
}

func (fs *fields) removeNonWritable(i Instance, accessType fieldAccessBits) gomerr.Gomerr {
	iv := reflect.ValueOf(i).Elem()

	for _, field := range fs.fieldMap {
		// TODO: handle nested/embedded structs
		if field.provided || strings.Contains(field.location, ".") || field.access(i.Subject().Principal(FieldAccess), accessType) {
			continue
		}

		fv := iv.FieldByName(field.name)
		if !fv.IsValid() || fv.IsZero() {
			continue
		}

		if !fv.CanSet() {
			return gomerr.Configuration("Unable to zero field: " + i.metadata().instanceName + "." + field.name)
		}

		fv.Set(field.zeroVal)
	}

	return nil
}

func (fs *fields) copyProvided(from, to Instance) gomerr.Gomerr {
	fv := reflect.ValueOf(from).Elem()
	tv := reflect.ValueOf(to).Elem() // Support non-pointer types?

	for _, field := range fs.fieldMap {
		// TODO: handle nested/embedded structs
		if !field.provided || strings.Contains(field.location, ".") {
			continue
		}

		ffv := fv.FieldByName(field.name)
		if !ffv.IsValid() || ffv.IsZero() {
			continue
		}

		tfv := tv.FieldByName(field.name)
		if !tfv.CanSet() {
			return gomerr.Configuration("Unable to copy provided field value: " + to.metadata().instanceName + "." + field.name)
		}

		tfv.Set(ffv)
	}

	return nil
}
