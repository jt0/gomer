package dataerr

import (
	"github.com/jt0/gomer/gomerr"
)

type PersistableNotFoundError struct {
	gomerr.Gomerr
	TypeName string
	Key      any
}

func PersistableNotFound(typeName string, key any) *PersistableNotFoundError {
	return gomerr.Build(new(PersistableNotFoundError), typeName, key).(*PersistableNotFoundError)
}
