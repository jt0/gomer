package gomerr

type UnmarshalError struct {
	Gomerr
	What   string
	Data   any `gomerr:"include_type"`
	Target any `gomerr:"include_type"`
}

func Unmarshal(what string, data, target any) *UnmarshalError {
	return Build(new(UnmarshalError), what, data, target).(*UnmarshalError)
}
