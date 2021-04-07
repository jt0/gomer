package gomerr

type ConflictError struct {
	Gomerr
	With    interface{}
	Problem string
	Source  interface{}
}

func Conflict(with interface{}, problem string) *ConflictError {
	return Build(new(ConflictError), with, problem).(*ConflictError)
}

func (c *ConflictError) WithSource(source interface{}) *ConflictError {
	c.Source = source
	return c
}
