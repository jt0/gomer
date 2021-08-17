package gomerr

type UnprocessableError struct {
	Gomerr
	Reason string
	Value  interface{} `gomerr:"include_type"`
}

func Unprocessable(reason string, value interface{}) *UnprocessableError {
	return Build(new(UnprocessableError), reason, value).(*UnprocessableError)
}
