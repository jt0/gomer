package flect

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jt0/gomer/gomerr"
)

type zeroVal struct{}

var ZeroVal = zeroVal{}

func SetValue(target reflect.Value, value any) gomerr.Gomerr {
	if value == nil {
		return nil
	} else if value == ZeroVal {
		target.Set(reflect.Zero(target.Type()))
		return nil
	}

	indirectTargetValueType := target.Type()
	var tvtPtr bool
	if indirectTargetValueType.Kind() == reflect.Ptr { // Doesn't handle pointer pointers
		indirectTargetValueType = indirectTargetValueType.Elem()
		tvtPtr = true
	}

	if stringValue, ok := value.(string); ok {
		if typedValue, ge := StringToType(stringValue, indirectTargetValueType); ge != nil {
			return ge
		} else if typedValue != nil {
			value = typedValue
		}
	} // Feature:p2 handle []byte input and perform conversion?

	valueValue, ok := value.(reflect.Value)
	if !ok {
		valueValue = reflect.ValueOf(value)
	}

	indirectValueValueType := valueValue.Type()
	var vvtPtr bool
	if indirectValueValueType.Kind() == reflect.Ptr {
		indirectValueValueType = indirectValueValueType.Elem()
		vvtPtr = true
	}

	indirectTargetIsAny := indirectTargetValueType.Kind() == reflect.Interface && indirectTargetValueType.Name() == ""
	indirectTypesMatch := indirectTargetIsAny || indirectValueValueType == indirectTargetValueType
	var vvConvertibleToTv bool
	if !indirectTypesMatch {
		vvConvertibleToTv = indirectValueValueType.ConvertibleTo(indirectTargetValueType)
		if !vvConvertibleToTv && !indirectValueValueType.AssignableTo(indirectTargetValueType) {
			return gomerr.Unprocessable("Unable to set value with type '"+valueValue.Type().String()+"' to '"+target.Type().String()+"'", value)
		}
	}

	type ptrType struct {
		tvtPtr bool
		vvtPtr bool
	}
	pt := ptrType{tvtPtr, vvtPtr}
	switch pt {
	case ptrType{true, true}:
		if target.Type() == valueValue.Type() {
			break
		}
		valueValue = valueValue.Elem()
		fallthrough
	case ptrType{true, false}:
		if vvConvertibleToTv {
			valueValue = valueValue.Convert(indirectTargetValueType)
			vvConvertibleToTv = false // already done
		}
		p := reflect.New(valueValue.Type())
		p.Elem().Set(valueValue)
		valueValue = p
	case ptrType{false, true}:
		if valueValue.IsNil() {
			target.Set(reflect.Zero(target.Type())) // won't this break length constraint that wants a nil value?
			return nil
		}
		valueValue = valueValue.Elem()
	case ptrType{false, false}:
		// nothing to do
	}

	if !indirectTypesMatch && vvConvertibleToTv {
		valueValue = valueValue.Convert(indirectTargetValueType)
	}

	target.Set(valueValue)

	return nil
}

// StringToType returns a value corresponding to the provided targetType. If the targetType isn't recognized, this
// returns nil rather than an error. An error occurs if the targetType is recognized, but it's not possible to convert
// the string into that type.
func StringToType(valueString string, targetType reflect.Type) (any, gomerr.Gomerr) {
	var value any
	var err error

	switch targetType.Kind() {
	case reflect.String:
		value = valueString
	case reflect.Bool:
		value, err = strconv.ParseBool(valueString)
	case reflect.Int:
		parsed, parseErr := strconv.ParseInt(valueString, 0, 64)
		if parseErr != nil {
			err = parseErr
		} else {
			value = int(parsed)
		}
	case reflect.Int8:
		parsed, parseErr := strconv.ParseInt(valueString, 0, 8)
		if parseErr != nil {
			err = parseErr
		} else {
			value = int8(parsed)
		}
	case reflect.Int16:
		parsed, parseErr := strconv.ParseInt(valueString, 0, 16)
		if parseErr != nil {
			err = parseErr
		} else {
			value = int16(parsed)
		}
	case reflect.Int32:
		parsed, parseErr := strconv.ParseInt(valueString, 0, 32)
		if parseErr != nil {
			err = parseErr
		} else {
			value = int32(parsed)
		}
	case reflect.Int64:
		value, err = strconv.ParseInt(valueString, 0, 64)
	case reflect.Uint:
		parsed, parseErr := strconv.ParseUint(valueString, 0, 64)
		if parseErr != nil {
			err = parseErr
		} else {
			value = uint(parsed)
		}
	case reflect.Uint8:
		parsed, parseErr := strconv.ParseUint(valueString, 0, 8)
		if parseErr != nil {
			err = parseErr
		} else {
			value = uint8(parsed)
		}
	case reflect.Uint16:
		parsed, parseErr := strconv.ParseUint(valueString, 0, 16)
		if parseErr != nil {
			err = parseErr
		} else {
			value = uint16(parsed)
		}
	case reflect.Uint32:
		parsed, parseErr := strconv.ParseUint(valueString, 0, 32)
		if parseErr != nil {
			err = parseErr
		} else {
			value = uint32(parsed)
		}
	case reflect.Uint64:
		value, err = strconv.ParseUint(valueString, 0, 64)
	case reflect.Uintptr:
		value, err = strconv.ParseUint(valueString, 0, 64)
	case reflect.Float32:
		parsed, parseErr := strconv.ParseFloat(valueString, 32)
		if parseErr != nil {
			err = parseErr
		} else {
			value = float32(parsed)
		}
	case reflect.Float64:
		value, err = strconv.ParseFloat(valueString, 64)
	case reflect.Struct:
		if targetType == timeType {
			if strings.Index(valueString, "T") == -1 {
				valueString = valueString + "T00:00:00Z"
			}
			value, err = time.Parse(time.RFC3339Nano, valueString)
		}
	case reflect.Slice:
		if targetType == byteSliceType {
			value = []byte(valueString) // NB: To decode the bytes, use (or define) a field function (e.g. $base64Decode)
		} // Feature:p2 splitting comma separated values
	}

	if err != nil {
		return nil, gomerr.Unmarshal("valueString", valueString, targetType.String()).Wrap(err)
	}

	return value, nil
}

var (
	timeType      = reflect.TypeOf((*time.Time)(nil)).Elem()
	byteSliceType = reflect.TypeOf((*[]uint8)(nil)).Elem()
)
