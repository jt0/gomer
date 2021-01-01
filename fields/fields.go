package fields

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
)

type Fields struct {
	fieldMap map[string]*field
	idField  *field

	// internalToField map[string]*field
	// keyFields []*field
}

type field struct {
	name          string
	externalName  string
	location      string
	accessBits    fieldAccessBits
	provided      bool
	defaultValues map[string]defaultValue
	zeroVal       reflect.Value
	constraints   map[string]constraint.Constraint
}

type defaultValue struct {
	value       string
	function    DefaultFunction
	bypassIfSet bool
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
