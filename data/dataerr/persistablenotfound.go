package dataerr

import (
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type PersistableNotFoundError struct {
	gomerr.Gomerr
	Persistable data.Persistable
}

func PersistableNotFound(p data.Persistable) *PersistableNotFoundError {
	return gomerr.Build(new(PersistableNotFoundError), p).(*PersistableNotFoundError)
}
