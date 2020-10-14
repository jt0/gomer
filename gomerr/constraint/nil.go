package constraint

func Nil(value interface{}) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return value == nil
	}}.setDetails("IsNil", value)
}

func NotNil(value interface{}) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		return value != nil
	}}.setDetails("IsNotNil", value)
}
