package gomerr

import (
	. "github.com/jt0/gomer/gomerr/because"
)

type ConflictError struct {
	Gomerr
	This     interface{} `gomerr:"include_type"`
	That     interface{} `gomerr:"include_type"`
	Because_ Because
	On_      interface{} `gomerr:"include_type"`
}

func Conflict() *ConflictError {
	return Build(new(ConflictError)).(*ConflictError)
}

func ConflictWith(this interface{}) *ConflictError {
	return Build(new(ConflictError), this).(*ConflictError)
}

func ConflictBetween(this, that interface{}) *ConflictError {
	return Build(new(ConflictError), this, that).(*ConflictError)
}

func (c *ConflictError) Because(because Because) *ConflictError {
	c.Because_ = because
	return c
}

func (c *ConflictError) On(on interface{}) *ConflictError {
	c.On_ = on
	return c
}
