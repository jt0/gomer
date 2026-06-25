package gomerr

type UnmarshalError struct {
	Gomerr
	What   string
	Data   any
	Target any
}

func Unmarshal(what string, data, target any) *UnmarshalError {
	return Build(new(UnmarshalError), what, data, target).(*UnmarshalError)
}
