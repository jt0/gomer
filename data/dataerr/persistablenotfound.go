package dataerr

import (
	"github.com/jt0/gomer/gomerr"
)

type PersistableNotFoundError struct {
	gomerr.Gomerr
	TypeName string
	Key      interface{}
}

func PersistableNotFound(typeName string, key interface{}) *PersistableNotFoundError {
	return gomerr.Build(new(PersistableNotFoundError), typeName, key).(*PersistableNotFoundError)
}
