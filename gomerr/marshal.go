package gomerr

type MarshalError struct {
	Gomerr
	What  string
	Value interface{} `gomerr:"include_type"`
}

func Marshal(what string, value interface{}) *MarshalError {
	return Build(new(MarshalError), what, value).(*MarshalError)
}
