package http

import (
	"github.com/jt0/gomer/gomerr"
)

type UnroutableError struct {
	gomerr.Gomerr
	Route string
}

func Unroutable() *UnroutableError {
	return gomerr.Build(new(UnroutableError)).(*UnroutableError)
}
