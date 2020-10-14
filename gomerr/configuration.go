package gomerr

type ConfigurationError struct {
	Gomerr
	Problem string
}

func Configuration(problem string) *ConfigurationError {
	return Build(new(ConfigurationError), problem).(*ConfigurationError)
}
