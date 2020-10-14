package gomerr

type UnprocessableError struct {
	Gomerr
	Name       string
	Value      interface{} `gomerr:"include_type"`
	Constraint Constraint
}

func Unprocessable(name string, value interface{}, constraint Constraint) *UnprocessableError {
	return Build(new(UnprocessableError), name, value, constraint).(*UnprocessableError)
}
