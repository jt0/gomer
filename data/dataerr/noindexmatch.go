package dataerr

import (
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type NoIndexMatchError struct {
	gomerr.Gomerr
	AvailableIndexes map[string]interface{}
	Queryable        data.Queryable `gomerr:"include_type"`
}

func NoIndexMatch(availableIndexes map[string]interface{}, queryable data.Queryable) *NoIndexMatchError {
	return gomerr.Build(new(NoIndexMatchError), availableIndexes, queryable).(*NoIndexMatchError)
}
