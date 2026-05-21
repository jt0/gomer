package rest

import (
	"github.com/jt0/gomer/gomerr"
)

type UnroutableError struct {
	gomerr.Gomerr
	Method string
	Path   string
}

func Unroutable() *UnroutableError {
	return gomerr.Build(new(UnroutableError)).(*UnroutableError)
}
