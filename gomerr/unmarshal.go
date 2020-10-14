package gomerr

type UnmarshalError struct {
	Gomerr
	What   string
	Data   interface{} `gomerr:"include_type"`
	Target interface{} `gomerr:"include_type"`
}

func Unmarshal(what string, data, target interface{}) *UnmarshalError {
	return Build(new(UnmarshalError), what, data, target).(*UnmarshalError)
}
