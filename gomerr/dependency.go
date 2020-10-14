package gomerr

type DependencyError struct {
	Gomerr
	Service string
	Request interface{} `gomerr:"include_type"`
}

func Dependency(service string, request interface{}) *DependencyError {
	return Build(new(DependencyError), service, request).(*DependencyError)
}
