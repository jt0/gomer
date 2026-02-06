package gomerr

type MarshalError struct {
	Gomerr
	What  string
	Value any `gomerr:"include_type"`
}

func Marshal(what string, value any) *MarshalError {
	return Build(new(MarshalError), what, value).(*MarshalError)
}
