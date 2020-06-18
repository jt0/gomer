package resource

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/constraint"
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
type FieldDefaultFunction func(Instance) interface{}

func newFields(structType reflect.Type) (*fields, gomerr.Gomerr) {
	fields := &fields{
		fieldMap: make(map[string]*field),
	}

	if errors := fields.process(structType, "", make([]gomerr.Gomerr, 0)); len(errors) > 0 {
		return nil, gomerr.Batch(errors).AddCulprit(gomerr.Configuration)
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
					errors = append(errors, gomerr.BadValue(structType.Name()+"."+sFieldName, []*field{f, fs.idField}, constraint.ExactlyOnce()).AddNotes("multiple fields with `id`"))
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
		errors = append(errors, gomerr.BadValue(structType.Name()+".<idField>", nil, constraint.ExactlyOnce()))
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
			return ge.AddNotes(fmt.Sprintf("Unable to set %s's default value (%v)", field.location, defaultValue))
		}
	}

	return nil
}

func (fs *fields) removeNonReadable(i Instance) map[string]interface{} { // TODO: clear fields w/in instance
	resource := reflect.ValueOf(i).Elem() // Support non-pointer types?
	readView := make(map[string]interface{})

	for _, field := range fs.fieldMap {
		if field.readable(i.Subject().Principal(FieldAccess)) {
			fieldValue := resource.FieldByName(field.name)
			if isSet(fieldValue) {
				readView[field.externalName] = fieldValue.Interface()
			}
		}
	}

	return readView
}

func (fs *fields) removeNonWritable(i Instance) gomerr.Gomerr {
	uv := reflect.ValueOf(i).Elem()

	for _, field := range fs.fieldMap {
		if !field.writable(i.Subject().Principal(FieldAccess)) {
			if strings.Contains(field.location, ".") {
				continue // TODO: handle nested/embedded structs
			}

			fv := uv.FieldByName(field.name)
			if !fv.IsValid() || fv.IsZero() {
				continue
			}

			if !fv.CanSet() {
				return gomerr.BadValue(i.PersistableTypeName()+"."+field.name+".CanSet()", fv, constraint.Function(canSet)).AddNotes("Can't zero field").AddCulprit(gomerr.Configuration)
			}

			fv.Set(reflect.Zero(fv.Type()))
		}
	}

	return nil
}

func canSet(i interface{}) bool {
	return i.(reflect.Value).CanSet()
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

func setDefaultValue(fieldValue reflect.Value, defaultValue interface{}) gomerr.Gomerr {
	defaultValueValue := reflect.ValueOf(defaultValue)

	// This handles non-string FieldDefaultFunction results and default strings
	if defaultValueValue.Type().AssignableTo(fieldValue.Type()) {
		fieldValue.Set(defaultValueValue)

		return nil
	}

	stringValue, ok := defaultValue.(string)
	if !ok {
		return gomerr.BadValue("defaultValue type", defaultValue, constraint.TypeOf("")).AddNotes("Non-string representations should have already been handled").AddCulprit(gomerr.Configuration)
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
		return gomerr.BadValue("Unsupported defaultValue type", fieldValue.Kind().String(), constraint.Values(
			reflect.Bool.String(),
			reflect.Int.String(), reflect.Int8.String(), reflect.Int16.String(), reflect.Int32.String(), reflect.Int64.String(),
			reflect.Uint.String(), reflect.Uint8.String(), reflect.Uint16.String(), reflect.Uint32.String(), reflect.Uint64.String(), reflect.Uintptr.String(),
			reflect.Float32.String, reflect.Float64.String(),
		)).AddCulprit(gomerr.Configuration)
	}

	if err != nil {
		return gomerr.Unmarshal(err, defaultValue, fieldValue.Kind().String()).AddCulprit(gomerr.Configuration)
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
	// Format: (<defaultTagValue>)?(,<externalName>)?
	parts := strings.Split(idTag, ",")

	if len(parts) > 2 {
		return gomerr.BadValue("'id' tag", parts, constraint.Length(1, 2)).AddCulprit(gomerr.Configuration)
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

func (f *field) accessTag(accessTag string) gomerr.Gomerr {
	if accessTag == "" {
		return nil // no access
	}

	if len(accessTag) > 16 {
		return gomerr.BadValue("'access' tag", accessTag, constraint.Length(0, 16)).AddNotes("can only support up to 8 field access principals pairs").AddCulprit(gomerr.Configuration)
	}

	if !mod2(accessTag) {
		return gomerr.BadValue("'access' tag", accessTag, constraint.Function(mod2)).AddNotes("each field access principal needs a specified 'read' and 'write' value").AddCulprit(gomerr.Configuration)
	}

	var accessBits fieldAccessBits
	for i := 0; i < len(accessTag); i += 2 {
		accessBits <<= 2 // Prepare by pushing two bits up.  The first time this still results in an no bits set (as expected)

		if accessTag[i] == 'r' {
			accessBits |= 2
		} else if accessTag[i] != '-' {
			return gomerr.BadValue("'access' tag", accessTag[i], constraint.Values('r', '-')).AddCulprit(gomerr.Configuration)
		}

		if accessTag[i+1] == 'w' {
			accessBits |= 1
		} else if accessTag[i+1] != '-' {
			return gomerr.BadValue("'access' tag", accessTag[i], constraint.Values('r', '-')).AddCulprit(gomerr.Configuration)
		}
	}

	f.access = accessBits

	return nil
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
