package resource

import (
	"fmt"
	"reflect"
	"strconv"
	"unicode"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
)

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

func RegisterFieldAccessPrincipals(fieldAccessPrincipals ...FieldAccessPrincipal) {
	if len(fieldAccessPrincipals) > 8 {
		panic(fmt.Sprintf("Too many fieldAccessPrincipals (maximum = 8): %v", fieldAccessPrincipals))
	}

	for i, r := range fieldAccessPrincipals {
		bitsLocationForPrincipal[r] = uint(i)
	}
}

type FieldDefaultFunction func() interface{}
type FieldDefaultFunctions func(name string) func(subject auth.Subject) interface{}

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

var (
	bitsLocationForPrincipal      = make(map[auth.Principal]uint)
	fieldDefaultFunctions         map[string]FieldDefaultFunction
	internalFieldDefaultFunctions = map[string]FieldDefaultFunction{
		//"$_emptyStringToIntMap": func() interface{} { return make(map[string]int) },
	}
)

func fieldMetadata(structType reflect.Type) map[string]field {
	fields := make(map[string]field)

	for i := 0; i < structType.NumField(); i++ {
		structField := structType.Field(i)
		structFieldName := structField.Name

		if unicode.IsLower([]rune(structFieldName)[0]) {
			continue
		}

		if structField.Type.Kind() == reflect.Struct {
			nestedFields := fieldMetadata(structField.Type)

			for k, v := range nestedFields {
				fields[k] = v
			}
		} else {
			field := field{
				name:         structFieldName,
				externalName: handleExtnameTag(structField.Tag.Get("extname"), structFieldName),
				access:       handleAccessTag(structField.Tag.Get("access")),
			}

			handleDefaultTag(structField.Tag.Get("default"), &field)

			fields[structFieldName] = field
		}
	}

	return fields
}

func handleExtnameTag(extname, fieldName string) string {
	if extname != "" {
		return extname
	} else {
		return fieldName
	}
}

type fieldAccessBits uint16

func handleAccessTag(access string) fieldAccessBits {
	var accessBits fieldAccessBits

	if access == "" {
		return accessBits // no access
	}

	if len(access) > 16 {
		panic("'access' tag too long.  Can only support up to 8 access pairs (16 bits)")
	}

	if len(access)%2 != 0 {
		panic("expected 'access' must have two values for each field access principal")
	}

	for i := 0; i < len(access); i += 2 {
		accessBits <<= 2 // Prepare by pushing two bits up.  The first time this still results in an no bits set (as expected)

		if access[i] == 'r' {
			accessBits |= 2
		} else if access[i] != '-' {
			panic("Invalid access tag value: " + access)
		}

		if access[i+1] == 'w' {
			accessBits |= 1
		} else if access[i+1] != '-' {
			panic("Invalid access tag value: " + access)
		}
	}

	return accessBits
}

func handleDefaultTag(defaultTag string, field *field) {
	if defaultTag == "" {
		return
	}

	if defaultTag[:1] == "!" {
		field.forceDefaultValue = true

		defaultTag = defaultTag[1:]
	}

	if defaultTag[:1] == "$" {
		if fn, ok := fieldDefaultFunctions[defaultTag]; ok {
			field.defaultValueFunction = fn

			return
		} else if fn, ok := internalFieldDefaultFunctions[defaultTag]; ok {
			field.defaultValueFunction = fn

			return
		}
	}

	field.defaultValue = defaultTag
}

type field struct {
	name                 string
	externalName         string
	access               fieldAccessBits
	defaultValue         string
	defaultValueFunction FieldDefaultFunction
	forceDefaultValue    bool
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

func applyFieldDefaults(i Instance) *gomerr.ApplicationError {
	metadata := i.metadata()
	resource := reflect.ValueOf(i).Elem() // Support non-pointer types?

	// TODO: handle nested/embedded structs
	for name, field := range metadata.fields {
		if !field.forceDefaultValue && field.defaultValueFunction == nil && field.defaultValue == "" {
			continue
		}

		fieldValue := resource.FieldByName(name)
		if !field.forceDefaultValue && !isEmptyValue(fieldValue) {
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

func setDefaultValue(fieldValue reflect.Value, defaultValue interface{}) *gomerr.ApplicationError {
	defaultValueValue := reflect.ValueOf(defaultValue)

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

func scopedResult(i Instance) (interface{}, *gomerr.ApplicationError) {
	if result := extractFields(i); result == nil || len(result) == 0 {
		return nil, gomerr.ResourceNotFound(i)
	} else {
		return result, nil
	}
}

func extractFields(i Instance) map[string]interface{} {
	metadata := i.metadata()
	resource := reflect.ValueOf(i).Elem() // Support non-pointer types?
	resourceView := make(map[string]interface{})

	for name, field := range metadata.fields {
		if field.readable(i.Subject().Principal(FieldAccess)) {
			fieldValue := resource.FieldByName(name)
			if !isEmptyValue(fieldValue) {
				resourceView[field.externalName] = fieldValue.Interface()
			}
		}
	}

	return resourceView
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	case reflect.Invalid:
		// TODO: log?
		return true
	}
	return false
}
