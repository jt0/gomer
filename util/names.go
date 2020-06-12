package util

import (
	"reflect"
	"strings"
)

func UnqualifiedTypeName(i interface{}) string {
	if i == nil {
		return ""
	}

	t, ok := i.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(i)
	}

	s := t.String()
	dotIndex := strings.Index(s, ".")

	return s[dotIndex+1:]
}
