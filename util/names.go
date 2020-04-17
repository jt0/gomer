package util

import (
	"reflect"
	"strings"
)

func UnqualifiedTypeName(t reflect.Type) string {
	if t == nil {
		return ""
	}

	s := t.String()
	dotIndex := strings.Index(s, ".")

	return s[dotIndex+1:]
}
