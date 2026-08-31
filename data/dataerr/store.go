package dataerr

import (
	"reflect"
	"strings"

	"github.com/jt0/gomer/gomerr"
)

type StoreError struct {
	gomerr.Gomerr
	Operation string
}

func Store(operation string, ge gomerr.Gomerr) gomerr.Gomerr {
	if ge != nil && strings.Contains(reflect.TypeOf(ge).String(), "dataerr") {
		return ge.AddAttribute("operation", operation)
	}
	return gomerr.Build(new(StoreError), operation).Wrap(ge) // wrapping nil is a no-op
}
