package gomerr

type MissingError struct {
	Gomerr
	What   string
	From   interface{} `gomerr:"include_type"`
	Given_ interface{} `gomerr:"include_type"`
}

func Missing(what string, from interface{}) *MissingError {
	return Build(new(MissingError), what, from).(*MissingError)
}

func (m *MissingError) Given(given interface{}) *MissingError {
	m.Given_ = given
	return m
}
