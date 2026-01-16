package dataerr

import (
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type KeyFieldMismatchError struct {
	gomerr.Gomerr
	FieldName     string
	ExistingValue string
	ConflictValue string
	ExistingKey   string
	ConflictKey   string
	Persistable   data.Persistable `gomerr:"include_type"`
}

func KeyFieldMismatch(fieldName, existingValue, conflictValue, existingKey, conflictKey string, persistable data.Persistable) *KeyFieldMismatchError {
	return gomerr.Build(new(KeyFieldMismatchError), fieldName, existingValue, conflictValue, existingKey, conflictKey, persistable).(*KeyFieldMismatchError)
}
