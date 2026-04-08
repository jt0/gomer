package gomerr

type ConflictError struct {
	Gomerr
	With    string
	Id      string
	Problem string
	Source  any
}

func Conflict(with, id, problem string) *ConflictError {
	return Build(new(ConflictError), with, id, problem).(*ConflictError)
}

func (c *ConflictError) WithSource(source any) *ConflictError {
	c.Source = source
	return c
}
