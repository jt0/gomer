package gomerr

type NotFoundError struct {
	Gomerr
	Type string
	Id   string
}

func NotFound(type_ string, id string) *NotFoundError {
	return Build(new(NotFoundError), type_, id).(*NotFoundError)
}
