package gomerr

type UnprocessableError struct {
	Gomerr
	Reason string
	Value  any `gomerr:"include_type"`
}

func Unprocessable(reason string, value any) *UnprocessableError {
	return Build(new(UnprocessableError), reason, value).(*UnprocessableError)
}
