package fields

var fieldDefaultFunctions map[string]DefaultFunction

func RegisterFieldDefaultFunctions(functions map[string]DefaultFunction) {
	for k := range functions {
		if k[:1] != "$" {
			panic("Default functions must start with a '$' symbol")
		}

		if k[1:2] == "_" {
			panic("Default function names must not begin with an underscore")
		}
	}

	fieldDefaultFunctions = functions
}
