package dataerr

import (
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type NoIndexMatchError struct {
	gomerr.Gomerr
	AvailableIndexes map[string]interface{}
	Listable         data.Listable `gomerr:"include_type"`
}

func NoIndexMatch(availableIndexes map[string]interface{}, listable data.Listable) *NoIndexMatchError {
	return gomerr.Build(new(NoIndexMatchError), availableIndexes, listable).(*NoIndexMatchError)
}
