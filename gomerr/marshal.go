package gomerr

type MarshalError struct {
	Gomerr
	What  string
	Value any
}

func Marshal(what string, value any) *MarshalError {
	return Build(new(MarshalError), what, value).(*MarshalError)
}
