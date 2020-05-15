package resource

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
)

type fields struct {
	fieldMap map[string]*field
	idField  *field

	//internalToField map[string]*field
	//keyFields []*field
}

type field struct {
	name                 string
	externalName         string
	location             string
	access               fieldAccessBits
	defaultValue         string
	defaultValueFunction FieldDefaultFunction
	bypassDefaultIfSet   bool
}

type fieldAccessBits uint16
type FieldDefaultFunction func() interface{}

func (f *fields) process(structType reflect.Type, path string) *fields {
	if f.fieldMap == nil {
		f.fieldMap = make(map[string]*field)
	}

	for i := 0; i < structType.NumField(); i++ {
		sField := structType.Field(i)
		sFieldName := sField.Name

		if sField.Type.Kind() == reflect.Struct {
			if sField.Anonymous {
				f.process(sField.Type, path+sFieldName+"+")
			} else {
				f.process(sField.Type, path+sFieldName+".")
			}
		} else {
			if unicode.IsLower([]rune(sFieldName)[0]) {
				continue
			}

			field := &field{
				name:         sFieldName,
				externalName: externalNameTag(sField.Tag.Get("json"), sFieldName),
				location:     path + sFieldName,
				access:       accessTag(sField.Tag.Get("access")),
			}

			// Format: (<defaultTag>)?(,<externalName>)?
			if idTag, ok := sField.Tag.Lookup("id"); ok {
				parts := strings.Split(idTag, ",")

				if len(parts) > 2 {
					panic("unexpected format for id tag: " + idTag)
				}

				defaultTag(parts[0], field)

				if len(parts) == 2 {
					field.externalName = strings.TrimSpace(parts[1])
				}

				if f.idField != nil {
					panic("multiple fields have `id` tag - only one allowed")
				}

				f.idField = field
			} else {
				defaultTag(sField.Tag.Get("default"), field)
			}

			if current, exists := f.fieldMap[field.externalName]; exists {
				if strings.Count(current.location, ".") == 0 || strings.Count(current.location, "+") == 0 {
					fmt.Printf("Info: skipping duplicate field name '%s' from '%s", field.externalName, field.location)

					continue
				}
			}

			f.fieldMap[field.externalName] = field // may override nested (up or down) value. That's okay.
		}
	}

	if path == "" && f.idField == nil {
		panic("no `id` field defined for resource " + structType.Name())
	}

	return f
}

func (f *fields) idExternalName() string {
	return f.idField.externalName
}

func (f *fields) externalNameToFieldName(externalName string) (string, bool) {
	if field, ok := f.fieldMap[externalName]; ok {
		return field.name, ok
	} else {
		return externalName, ok
	}
}

func (f *fields) applyDefaults(i Instance) *gomerr.ApplicationError {
	resource := reflect.ValueOf(i).Elem() // Support non-pointer types?

	// TODO: handle nested/embedded structs
	for _, field := range f.fieldMap {
		if field.defaultValueFunction == nil && field.defaultValue == "" {
			continue
		}

		fieldValue := resource.FieldByName(field.name)
		if field.bypassDefaultIfSet && isSet(fieldValue) {
			continue
		}

		var defaultValue interface{}
		if field.defaultValueFunction != nil {
			defaultValue = field.defaultValueFunction()
		} else {
			defaultValue = field.defaultValue
		}

		setDefaultValue(fieldValue, defaultValue)
	}

	return nil
}

func (f *fields) removeNonReadable(i Instance) map[string]interface{} { // TODO: clear fields w/in instance
	resource := reflect.ValueOf(i).Elem() // Support non-pointer types?
	readView := make(map[string]interface{})

	for _, field := range f.fieldMap {
		if field.readable(i.Subject().Principal(FieldAccess)) {
			fieldValue := resource.FieldByName(field.name)
			if isSet(fieldValue) {
				readView[field.externalName] = fieldValue.Interface()
			}
		}
	}

	return readView
}

var zeroStructVal = reflect.Value{}

func (f *fields) removeNonWritable(i Instance) *gomerr.ApplicationError {
	uv := reflect.ValueOf(i).Elem()

	for _, field := range f.fieldMap {
		if !field.writable(i.Subject().Principal(FieldAccess)) {
			if strings.Contains(field.location, ".") {
				continue // TODO: handle nested/embedded structs
			}

			fv := uv.FieldByName(field.name)
			if fv == zeroStructVal || fv.IsZero() {
				continue
			}

			if !fv.CanSet() {
				return gomerr.InternalServerError("Unable to set field " + field.name)
			}

			fv.Set(reflect.Zero(fv.Type()))
		}
	}

	return nil
}

func (f field) readable(fieldAccessPrincipal auth.Principal) bool {
	if fieldAccessPrincipal == nil || fieldAccessPrincipal == NoAccess {
		return false
	} else if fieldAccessPrincipal == ReadWriteAll || fieldAccessPrincipal == ReadAll {
		return true
	}

	bitsLocation, ok := bitsLocationForPrincipal[fieldAccessPrincipal]
	if !ok {
		return false
	}

	readBitLocationForRole := 2*bitsLocation + 1
	readBitForRole := fieldAccessBits(1 << readBitLocationForRole)

	return f.access&readBitForRole != 0
}

func (f field) writable(fieldAccessPrincipal auth.Principal) bool {
	if fieldAccessPrincipal == nil || fieldAccessPrincipal == NoAccess {
		return false
	} else if fieldAccessPrincipal == ReadWriteAll {
		return true
	}

	bitsLocation, ok := bitsLocationForPrincipal[fieldAccessPrincipal]
	if !ok {
		return false
	}

	writeBitLocationForRole := 2 * bitsLocation
	writeBitForRole := fieldAccessBits(1 << writeBitLocationForRole)

	return f.access&writeBitForRole != 0
}

func setDefaultValue(fieldValue reflect.Value, defaultValue interface{}) *gomerr.ApplicationError {
	defaultValueValue := reflect.ValueOf(defaultValue)

	if defaultValueValue == zeroStructVal || fieldValue == zeroStructVal {
		panic("foo")
	}

	// This handles non-string FieldDefaultFunction results and default strings
	if defaultValueValue.Type().AssignableTo(fieldValue.Type()) {
		fieldValue.Set(defaultValueValue)

		return nil
	}

	stringValue, ok := defaultValue.(string)
	if !ok {
		return gomerr.InternalServerError(fmt.Sprintf("Expected string, got '%d'", defaultValueValue.Kind()))
	}

	var typedDefaultValue interface{}
	var err error
	switch fieldValue.Kind() {
	case reflect.Bool:
		typedDefaultValue, err = strconv.ParseBool(stringValue)
	case reflect.Int:
		parsed, parseErr := strconv.ParseInt(stringValue, 0, 64)
		if parseErr != nil {
			err = parseErr
		} else {
			typedDefaultValue = int(parsed)
		}
	case reflect.Int8:
		parsed, parseErr := strconv.ParseInt(stringValue, 0, 8)
		if parseErr != nil {
			err = parseErr
		} else {
			typedDefaultValue = int8(parsed)
		}
	case reflect.Int16:
		parsed, parseErr := strconv.ParseInt(stringValue, 0, 16)
		if parseErr != nil {
			err = parseErr
		} else {
			typedDefaultValue = int16(parsed)
		}
	case reflect.Int32:
		parsed, parseErr := strconv.ParseInt(stringValue, 0, 32)
		if parseErr != nil {
			err = parseErr
		} else {
			typedDefaultValue = int32(parsed)
		}
	case reflect.Int64:
		typedDefaultValue, err = strconv.ParseInt(stringValue, 0, 64)
	case reflect.Uint:
		parsed, parseErr := strconv.ParseUint(stringValue, 0, 64)
		if parseErr != nil {
			err = parseErr
		} else {
			typedDefaultValue = uint(parsed)
		}
	case reflect.Uint8:
		parsed, parseErr := strconv.ParseUint(stringValue, 0, 8)
		if parseErr != nil {
			err = parseErr
		} else {
			typedDefaultValue = uint8(parsed)
		}
	case reflect.Uint16:
		parsed, parseErr := strconv.ParseUint(stringValue, 0, 16)
		if parseErr != nil {
			err = parseErr
		} else {
			typedDefaultValue = uint16(parsed)
		}
	case reflect.Uint32:
		parsed, parseErr := strconv.ParseUint(stringValue, 0, 32)
		if parseErr != nil {
			err = parseErr
		} else {
			typedDefaultValue = uint32(parsed)
		}
	case reflect.Uint64:
		typedDefaultValue, err = strconv.ParseUint(stringValue, 0, 64)
	case reflect.Uintptr:
		typedDefaultValue, err = strconv.ParseUint(stringValue, 0, 64)
	case reflect.Float32:
		parsed, parseErr := strconv.ParseFloat(stringValue, 32)
		if parseErr != nil {
			err = parseErr
		} else {
			typedDefaultValue = float32(parsed)
		}
	case reflect.Float64:
		typedDefaultValue, err = strconv.ParseFloat(stringValue, 64)
	default:
		return gomerr.InternalServerError(fmt.Sprintf("Unsupported default value type (%d)", fieldValue.Kind()))
	}

	if err != nil {
		return gomerr.InternalServerError(fmt.Sprintf("Invalid value for '%d' type: %v", defaultValueValue.Kind(), defaultValue))
	}

	fieldValue.Set(reflect.ValueOf(typedDefaultValue))

	return nil
}

func isSet(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() != 0
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() != 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() != 0
	case reflect.Float32, reflect.Float64:
		return v.Float() != 0
	case reflect.Interface, reflect.Ptr:
		return !v.IsNil()
	case reflect.Invalid:
		// TODO: log?
		return false
	}
	return true
}

func externalNameTag(nameTag string, fieldName string) string {
	if nameTag == "" {
		return fieldName
	}

	nameTagParts := strings.Split(nameTag, ",")
	name := strings.TrimSpace(nameTagParts[0])

	if name == "" {
		return fieldName
	}

	return name
}

func accessTag(accessTag string) fieldAccessBits {
	var accessBits fieldAccessBits

	if accessTag == "" {
		return accessBits // no access
	}

	if len(accessTag) > 16 {
		panic("'access' tag too long.  Can only support up to 8 access pairs (16 bits)")
	}

	if len(accessTag)%2 != 0 {
		panic("expected 'access' must have two values for each field access principal")
	}

	for i := 0; i < len(accessTag); i += 2 {
		accessBits <<= 2 // Prepare by pushing two bits up.  The first time this still results in an no bits set (as expected)

		if accessTag[i] == 'r' {
			accessBits |= 2
		} else if accessTag[i] != '-' {
			panic("Invalid access tag value: " + accessTag)
		}

		if accessTag[i+1] == 'w' {
			accessBits |= 1
		} else if accessTag[i+1] != '-' {
			panic("Invalid access tag value: " + accessTag)
		}
	}

	return accessBits
}

func defaultTag(defaultTag string, field *field) {
	if defaultTag == "" {
		return
	}

	if defaultTag[:1] == "?" {
		field.bypassDefaultIfSet = true

		defaultTag = defaultTag[1:]
	}

	if defaultTag[:1] == "$" {
		if fn, ok := fieldDefaultFunctions[defaultTag]; ok {
			field.defaultValueFunction = fn

			return
		}
	}

	field.defaultValue = defaultTag
}

type FieldAccessPrincipal string

const (
	FieldAccess auth.PrincipalType = "FieldAccess"

	ReadWriteAll FieldAccessPrincipal = "ReadWriteAll"
	ReadAll      FieldAccessPrincipal = "ReadAll"
	NoAccess     FieldAccessPrincipal = "NoAccess"
)

func (f FieldAccessPrincipal) Id() string {
	return string(f)
}

func (f FieldAccessPrincipal) Type() auth.PrincipalType {
	return FieldAccess
}

func (f FieldAccessPrincipal) Release() {
	// No-op
}

var bitsLocationForPrincipal = make(map[auth.Principal]uint)

func RegisterFieldAccessPrincipals(fieldAccessPrincipals ...FieldAccessPrincipal) {
	if len(fieldAccessPrincipals) > 8 {
		panic(fmt.Sprintf("Too many fieldAccessPrincipals (maximum = 8): %v", fieldAccessPrincipals))
	}

	for i, r := range fieldAccessPrincipals {
		bitsLocationForPrincipal[r] = uint(i)
	}
}

var fieldDefaultFunctions map[string]FieldDefaultFunction

func RegisterFieldDefaultFunctions(functions map[string]FieldDefaultFunction) {
	for k, _ := range functions {
		if k[:1] != "$" {
			panic("Default functions must start with a '$' symbol")
		}

		if k[1:2] == "_" {
			panic("Default function names must not begin with an underscore")
		}
	}

	fieldDefaultFunctions = functions
}
