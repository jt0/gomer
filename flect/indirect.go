package flect

import (
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

// IndirectInterface dereferences a pointer value one level, returning the underlying value
// as an interface. Returns (nil, false) if v is nil or an invalid/nil pointer. For
// non-pointer values, returns (v, true) unchanged. Useful for unwrapping optional fields
// or interface values before comparison or serialization.
func IndirectInterface(v any) (indirect any, ok bool) {
	ttv := reflect.ValueOf(v)
	if !ttv.IsValid() {
		return nil, false
	}

	if ttv.Kind() == reflect.Ptr {
		if ttv.IsNil() {
			return nil, false
		}
		return ttv.Elem().Interface(), true
	}

	return v, true
}

// ReadableIndirectValue returns the fully dereferenced reflect.Value for v, suitable for
// reading its kind, calling type-specific accessors (.Int(), .String(), etc.), or passing
// to other reflection APIs. Accepts both raw values and reflect.Value inputs. Returns
// (invalid, false) if the value is nil or otherwise unreadable.
func ReadableIndirectValue(v any) (indirectValue reflect.Value, ok bool) {
	vv, ok := v.(reflect.Value)
	if !ok {
		vv = reflect.ValueOf(v)
	}

	vv = reflect.Indirect(vv)
	if !vv.IsValid() {
		return vv, false
	}

	if vv.Kind() == reflect.Ptr {
		if vv.IsNil() {
			return vv, false
		}
		return vv.Elem(), true
	}

	return vv, true
}

// IndirectType returns the element type if v is a pointer type, otherwise returns the type
// directly. Accepts either a reflect.Type or a value from which the type is derived.
func IndirectType(v any) reflect.Type {
	vt, ok := v.(reflect.Type)
	if !ok {
		vt = reflect.TypeOf(v)
	}

	if vt.Kind() != reflect.Ptr {
		return vt
	}
	return vt.Elem()
}

// IndirectValue dereferences v and optionally checks that the result is settable.
// Returns an error if the value is invalid or (when mustSet is true) not addressable.
func IndirectValue(v any, mustSet bool) (reflect.Value, gomerr.Gomerr) {
	vv, ok := v.(reflect.Value)
	if !ok {
		vv = reflect.ValueOf(v)
	}

	vv = reflect.Indirect(vv)
	if !vv.IsValid() {
		return vv, gomerr.Unprocessable("value is not valid", v)
	} else if mustSet && !vv.CanSet() {
		return vv, gomerr.Unprocessable("value is not settable", v)
	}
	return vv, nil
}
