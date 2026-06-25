package dataerr

import (
	"reflect"
	"strings"

	"github.com/jt0/gomer/gomerr"
)

type StoreError struct {
	gomerr.Gomerr
	Operation string
	Data      any
}

func Store(operation string, data any, ge gomerr.Gomerr) gomerr.Gomerr {
	if ge != nil && strings.Contains(reflect.TypeOf(ge).String(), "dataerr") {
		return ge.AddAttribute("operation", operation)
	}
	return gomerr.Build(new(StoreError), operation, data).Wrap(ge) // wrapping nil is a no-op
}
