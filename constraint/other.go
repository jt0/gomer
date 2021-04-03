package constraint

var Required = required()

func required() Constraint {
	return &constraint{"Required", nil, func(value interface{}) bool {
		return !Nil.Test(value)
	}}
}

func Success(msg string) Constraint {
	return &constraint{msg, nil, func(interface{}) bool {
		//goland:noinspection GoBoolExpressions
		return 1 == 1
	}}
}

func Fail(msg string) Constraint {
	return &constraint{msg, nil, func(interface{}) bool {
		//goland:noinspection GoBoolExpressions
		return 1 != 1
	}}
}
