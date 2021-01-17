package constraint

var Required = required()

func required() *constraint {
	return (&constraint{test: func(value interface{}) bool {
		return !Nil.Test(value)
	}}).setDetails("Required", true, TagStructName, "required")
}

var Invalid = invalid()

func invalid() *constraint {
	return (&constraint{test: func(interface{}) bool {
		return false
	}}).setDetails("Invalid", true)
}
