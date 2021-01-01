package dataerr

import (
	"github.com/jt0/gomer/gomerr"
)

type StoreError struct {
	gomerr.Gomerr
	Operation string
	Data      interface{} `gomerr:"include_type"`
}

func Store(operation string, data interface{}) *StoreError {
	return gomerr.Build(new(StoreError), operation, data).(*StoreError)
}
