package constraint

var Nil = isNil()

func isNil() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return value == nil
	}}.setDetails("Nil", true)
}

var NotNil = notNil()

func notNil() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return value != nil
	}}.setDetails("Nil", false)
}
