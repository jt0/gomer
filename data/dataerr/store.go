package dataerr

import (
	"github.com/jt0/gomer/gomerr"
)

type StoreError struct {
	gomerr.Gomerr
	Operation string
	Data      any `gomerr:"include_type"`
}

func Store(operation string, data any) *StoreError {
	return gomerr.Build(new(StoreError), operation, data).(*StoreError)
}
