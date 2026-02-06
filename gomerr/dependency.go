package gomerr

type DependencyError struct {
	Gomerr
	Service string
	Request any `gomerr:"include_type"`
}

func Dependency(service string, request any) *DependencyError {
	return Build(new(DependencyError), service, request).(*DependencyError)
}
