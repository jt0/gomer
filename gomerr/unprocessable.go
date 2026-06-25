package gomerr

type UnprocessableError struct {
	Gomerr
	Reason string
	Value  any
}

func Unprocessable(reason string, value any) *UnprocessableError {
	return Build(new(UnprocessableError), reason, value).(*UnprocessableError)
}
