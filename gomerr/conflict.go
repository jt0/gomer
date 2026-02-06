package gomerr

type ConflictError struct {
	Gomerr
	With    any
	Problem string
	Source  any
}

func Conflict(with any, problem string) *ConflictError {
	return Build(new(ConflictError), with, problem).(*ConflictError)
}

func (c *ConflictError) WithSource(source any) *ConflictError {
	c.Source = source
	return c
}
