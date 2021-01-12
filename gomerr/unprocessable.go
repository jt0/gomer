package gomerr

type UnprocessableError struct {
	Gomerr
	Name  string
	Value interface{} `gomerr:"include_type"`
}

func Unprocessable(name string, value interface{}) *UnprocessableError {
	return Build(new(UnprocessableError), name, value).(*UnprocessableError)
}
