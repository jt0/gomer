package flect

import (
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

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

func IndirectValue(v any, mustSet bool) (reflect.Value, gomerr.Gomerr) {
	vv, ok := v.(reflect.Value)
	if !ok {
		vv = reflect.ValueOf(v)
	}

	vv = reflect.Indirect(vv)
	if !vv.IsValid() {
		return vv, gomerr.Unprocessable("Value is not valid", v)
	} else if mustSet && !vv.CanSet() {
		return vv, gomerr.Unprocessable("Value is not settable", v)
	}
	return vv, nil
}
