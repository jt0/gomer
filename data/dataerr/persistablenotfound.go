package dataerr

import (
	"github.com/jt0/gomer/gomerr"
)

type PersistableNotFoundError struct {
	gomerr.Gomerr
	TypeName string
	Key      interface{}
}

func PersistableNotFound(typeName string, key interface{}) *NoIndexMatchError {
	return gomerr.Build(new(PersistableNotFoundError), typeName, key).(*NoIndexMatchError)
}
