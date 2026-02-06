package dataerr

import (
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type NoIndexMatchError struct {
	gomerr.Gomerr
	AvailableIndexes map[string]any
	Queryable        data.Queryable `gomerr:"include_type"`
}

func NoIndexMatch(availableIndexes map[string]any, queryable data.Queryable) *NoIndexMatchError {
	return gomerr.Build(new(NoIndexMatchError), availableIndexes, queryable).(*NoIndexMatchError)
}
