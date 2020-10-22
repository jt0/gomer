package constraint

var Required = required()

func required() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return !Nil.Test(value)
	}}.setDetails("Required", true, LookupName, "required")
}

var Invalid = invalid()

func invalid() Constrainer {
	return Constrainer{test: func(interface{}) bool {
		return false
	}}.setDetails("Invalid", true)
}
