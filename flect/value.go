package flect

import (
	"reflect"
	"strconv"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
)

var parsableKindConstraint = constraint.OneOf(
	reflect.Bool,
	reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
	reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
	reflect.Uintptr,
	reflect.Float32, reflect.Float64,
)

func SetValue(target reflect.Value, value interface{}) gomerr.Gomerr {
	valueValue := reflect.ValueOf(value)
	targetType := target.Type()

	if target.Kind() == reflect.Ptr && valueValue.Kind() != reflect.Ptr {
		p := reflect.New(valueValue.Type())
		p.Elem().Set(valueValue)
		valueValue = p
	}

	// This handles non-string FieldDefaultFunction results and default strings
	if valueValue.Type().AssignableTo(targetType) {
		target.Set(valueValue)

		return nil
	}

	stringValue, ok := value.(string)
	if !ok {
		return gomerr.Unprocessable("value", value, constraint.Or(constraint.TypeOf(targetType), constraint.TypeOf("")))
	}

	if targetType.Kind() == reflect.Ptr {
		targetType = targetType.Elem()
	}

	var typed interface{}
	var err error
	switch targetType.Kind() {
	case reflect.Bool:
		typed, err = strconv.ParseBool(stringValue)
	case reflect.Int:
		parsed, parseErr := strconv.ParseInt(stringValue, 0, 64)
		if parseErr != nil {
			err = parseErr
		} else {
			typed = int(parsed)
		}
	case reflect.Int8:
		parsed, parseErr := strconv.ParseInt(stringValue, 0, 8)
		if parseErr != nil {
			err = parseErr
		} else {
			typed = int8(parsed)
		}
	case reflect.Int16:
		parsed, parseErr := strconv.ParseInt(stringValue, 0, 16)
		if parseErr != nil {
			err = parseErr
		} else {
			typed = int16(parsed)
		}
	case reflect.Int32:
		parsed, parseErr := strconv.ParseInt(stringValue, 0, 32)
		if parseErr != nil {
			err = parseErr
		} else {
			typed = int32(parsed)
		}
	case reflect.Int64:
		typed, err = strconv.ParseInt(stringValue, 0, 64)
	case reflect.Uint:
		parsed, parseErr := strconv.ParseUint(stringValue, 0, 64)
		if parseErr != nil {
			err = parseErr
		} else {
			typed = uint(parsed)
		}
	case reflect.Uint8:
		parsed, parseErr := strconv.ParseUint(stringValue, 0, 8)
		if parseErr != nil {
			err = parseErr
		} else {
			typed = uint8(parsed)
		}
	case reflect.Uint16:
		parsed, parseErr := strconv.ParseUint(stringValue, 0, 16)
		if parseErr != nil {
			err = parseErr
		} else {
			typed = uint16(parsed)
		}
	case reflect.Uint32:
		parsed, parseErr := strconv.ParseUint(stringValue, 0, 32)
		if parseErr != nil {
			err = parseErr
		} else {
			typed = uint32(parsed)
		}
	case reflect.Uint64:
		typed, err = strconv.ParseUint(stringValue, 0, 64)
	case reflect.Uintptr:
		typed, err = strconv.ParseUint(stringValue, 0, 64)
	case reflect.Float32:
		parsed, parseErr := strconv.ParseFloat(stringValue, 32)
		if parseErr != nil {
			err = parseErr
		} else {
			typed = float32(parsed)
		}
	case reflect.Float64:
		typed, err = strconv.ParseFloat(stringValue, 64)
	default:
		return gomerr.Unprocessable("target.Type()", targetType.String(), parsableKindConstraint)
	}

	if err != nil {
		return gomerr.Unmarshal("value", value, target.Interface()).Wrap(err)
	}

	typedValue := reflect.ValueOf(typed)
	if target.Kind() == reflect.Ptr {
		p := reflect.New(typedValue.Type())
		p.Elem().Set(typedValue)
		typedValue = p
	}

	target.Set(typedValue)

	return nil
}

func IsSet(v reflect.Value) bool {
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
