package dataerr

import (
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type KeyNotFoundError struct {
	gomerr.Gomerr
	Name        string
	Attributes  []string
	Persistable data.Persistable `gomerr:"include_type"`
}

func KeyNotFound(name string, attributes []string, persistable data.Persistable) *NoIndexMatchError {
	return gomerr.Build(new(KeyNotFoundError), name, attributes, persistable).(*NoIndexMatchError)
}
