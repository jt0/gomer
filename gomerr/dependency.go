package gomerr

type DependencyError struct {
	Gomerr
	Service string
	Request any
}

func Dependency(service string, request any) *DependencyError {
	return Build(new(DependencyError), service, request).(*DependencyError)
}
