package gomerr

type Constraint interface {
	Test(value interface{}) bool
	Details() map[string]interface{}
}

func Test(what string, value interface{}, constraint Constraint) *NotSatisfiedError {
	if !constraint.Test(value) {
		return Build(new(NotSatisfiedError), what, value, constraint.Details()).(*NotSatisfiedError)
	}

	return nil
}

func TestEach(what string, value interface{}, constraints ...Constraint) *BatchError {
	errors := make([]Gomerr, 0)
	for _, constraint := range constraints {
		if !constraint.Test(value) {
			errors = append(errors, Build(new(NotSatisfiedError), what, value, constraint.Details()).(*NotSatisfiedError))
		}
	}

	if len(errors) > 0 {
		return Batch(errors)
	}

	return nil
}

type NotSatisfiedError struct {
	Gomerr
	What       string
	Value      interface{} `gomerr:"include_type"`
	Constraint map[string]interface{}
}

func NotSatisfied(what string, value interface{}, constraint Constraint) *NotSatisfiedError {
	return Build(new(NotSatisfiedError), what, value, constraint).(*NotSatisfiedError)
}
