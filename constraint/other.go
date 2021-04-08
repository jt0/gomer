package constraint

var Required = func() Constraint {
	return New("Required", nil, func(value interface{}) bool {
		return !Nil.Test(value)
	})
}()

func Success(msg string) Constraint {
	return New(msg, nil, func(interface{}) bool {
		return true
	})
}

func Fail(msg string) Constraint {
	return New(msg, nil, func(interface{}) bool {
		return false
	})
}
