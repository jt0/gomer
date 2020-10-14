package gomerr

type InternalError struct {
	Gomerr
	Issue string
}

func Internal(issue string) *InternalError {
	return Build(new(InternalError), issue).(*InternalError)
}
