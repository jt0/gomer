package resource

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
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
	accessBits           fieldAccessBits
	provided             bool
	defaultValue         string
	defaultValueFunction FieldDefaultFunction
	bypassDefaultIfSet   bool
	zeroVal              reflect.Value
}

type fieldAccessBits uint32
type FieldDefaultFunction func(Instance) interface{}

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
		if field.bypassDefaultIfSet && isSet(fieldValue) {
			continue
		}

		var defaultValue interface{}
		if field.defaultValueFunction != nil {
			defaultValue = field.defaultValueFunction(i)
		} else {
			defaultValue = field.defaultValue
		}

		if ge := setDefaultValue(fieldValue, defaultValue); ge != nil {
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
			if isSet(fieldValue) {
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

var parsableKindConstraint = constraint.OneOf(
	reflect.Bool,
	reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
	reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
	reflect.Uintptr,
	reflect.Float32, reflect.Float64,
)

func setDefaultValue(fieldValue reflect.Value, defaultValue interface{}) gomerr.Gomerr {
	defaultValueValue := reflect.ValueOf(defaultValue)

	// This handles non-string FieldDefaultFunction results and default strings
	if defaultValueValue.Type().AssignableTo(fieldValue.Type()) {
		fieldValue.Set(defaultValueValue)

		return nil
	}

	stringValue, ok := defaultValue.(string)
	if !ok {
		return gomerr.Unprocessable("defaultValue", defaultValue, constraint.Or(constraint.TypeOf(fieldValue.Type()), constraint.TypeOf(stringValue)))
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
		return gomerr.Unprocessable("fieldValue.Kind", fieldValue.Kind().String(), parsableKindConstraint)
	}

	if err != nil {
		return gomerr.Unmarshal("defaultValue", defaultValue, fieldValue.Interface()).Wrap(err)
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

func (f *field) idTag(idTag string) gomerr.Gomerr {
	parts := strings.Split(idTag, ",")

	if len(parts) > 2 {
		return gomerr.Configuration("Expected format '(<defaultTagValue>)?(,<externalName>)?', but got: " + idTag)
	}

	f.defaultTag(parts[0])

	if len(parts) == 2 {
		f.externalName = strings.TrimSpace(parts[1])
	}

	return nil
}

func (f *field) externalNameTag(nameTag string) {
	if nameTag == "" {
		f.externalName = f.name
		return
	}

	nameTagParts := strings.Split(nameTag, ",")
	name := strings.TrimSpace(nameTagParts[0])

	if name == "" {
		f.externalName = f.name
		return
	}

	f.externalName = name
}

const (
	readAccess = fieldAccessBits(1 << iota)
	createAccess
	updateAccess
	numAccessTypes = iota // iota is not reset, so this is the number of access types starting w/ readAccess

	readChar   = 'r'
	writeChar  = 'w'
	createChar = 'c'
	updateChar = 'u'
	dashChar   = '-'
)

func (f *field) accessTag(accessTag string) gomerr.Gomerr {
	if accessTag == "" {
		return nil // no access
	}

	parts := strings.Split(accessTag, ",")
	if len(parts) > 1 {
		f.provided = parts[1] == "p" || parts[1] == "provided"
	}

	access := strings.TrimSpace(parts[0])
	if len(access) == 0 {
		return nil
	} else if len(access) > 16 {
		return gomerr.Configuration("'access' tag can support up to 8 field access principals, but got: " + strconv.Itoa(len(access)/2))
	} else if !mod2(access) {
		return gomerr.Configuration("'access' tag must have two parts for each field access principals, but got: " + access)
	}

	var accessBits fieldAccessBits
	for i := 0; i < len(access); i += 2 {
		accessBits <<= numAccessTypes // Prepare to write the next access bits.  The first time this still results in no set bits

		switch access[i] {
		case readChar:
			accessBits |= readAccess
		case dashChar:
			// nothing to set
		default:
			return gomerr.Configuration("Expected one of read access values (r, -), but got: " + string(access[i]))
		}

		switch access[i+1] {
		case writeChar:
			accessBits |= createAccess | updateAccess
		case createChar:
			accessBits |= createAccess
		case updateChar:
			accessBits |= updateAccess
		case dashChar:
			// nothing to set
		default:
			return gomerr.Configuration("Expected one of write access values (w, c, u, -), but got: " + string(access[i]))
		}
	}

	f.accessBits = accessBits

	return nil
}

func (f *field) access(fieldAccessPrincipal auth.Principal, accessType fieldAccessBits) bool {
	switch fieldAccessPrincipal {
	case ReadWriteAll:
		return true
	case ReadAll:
		return accessType&readAccess == readAccess
	case NoAccess:
		fallthrough
	case nil:
		return false
	default:
		bitsIndex, ok := bitsLocationForPrincipal[fieldAccessPrincipal]
		if !ok {
			return false // no matching principal
		}

		accessTypeBits := accessType << (numAccessTypes * bitsIndex)
		return f.accessBits&accessTypeBits == accessTypeBits
	}
}

func mod2(i interface{}) bool {
	return len(i.(string))%2 == 0
}

func (f *field) defaultTag(defaultTag string) {
	if defaultTag == "" {
		return
	}

	if defaultTag[:1] == "?" {
		f.bypassDefaultIfSet = true
		defaultTag = defaultTag[1:]
	}

	if defaultTag[:1] == "$" {
		if fn, ok := fieldDefaultFunctions[defaultTag]; ok {
			f.defaultValueFunction = fn
			return
		}
	}

	f.defaultValue = defaultTag
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

func (f FieldAccessPrincipal) Release(_ bool) gomerr.Gomerr {
	return nil
}

var bitsLocationForPrincipal = make(map[auth.Principal]uint)
var notReservedPrincipalsConstraint = constraint.Not(constraint.OneOf(ReadWriteAll, ReadAll, NoAccess))

func RegisterFieldAccessPrincipals(fieldAccessPrincipals ...FieldAccessPrincipal) {
	if len(fieldAccessPrincipals) > 7 {
		panic(fmt.Sprintf("too many fieldAccessPrincipals (maximum = 7): %v", fieldAccessPrincipals))
	}

	for i, r := range fieldAccessPrincipals {
		if ge := gomerr.Test("FieldAccessPrincipal.Id()", r, notReservedPrincipalsConstraint); ge != nil {
			panic(ge)
		}

		bitsLocationForPrincipal[r] = uint(i)
	}
}

var fieldDefaultFunctions map[string]FieldDefaultFunction

func RegisterFieldDefaultFunctions(functions map[string]FieldDefaultFunction) {
	for k := range functions {
		if k[:1] != "$" {
			panic("Default functions must start with a '$' symbol")
		}

		if k[1:2] == "_" {
			panic("Default function names must not begin with an underscore")
		}
	}

	fieldDefaultFunctions = functions
}
