package dataerr

import (
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type KeyValueNotFoundError struct {
	gomerr.Gomerr
	KeyName     string
	KeyFields   []string
	Persistable data.Persistable `gomerr:"include_type"`
}

func KeyValueNotFound(keyName string, keyFields []string, persistable data.Persistable) *KeyValueNotFoundError {
	return gomerr.Build(new(KeyValueNotFoundError), keyName, keyFields, persistable).(*KeyValueNotFoundError)
}
