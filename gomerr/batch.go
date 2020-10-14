package gomerr

type BatchError struct {
	Gomerr
	Errors []Gomerr
}

func Batch(errors []Gomerr) *BatchError {
	return Build(&BatchError{}, errors).(*BatchError)
}

func (b *BatchError) ToMap() map[string]interface{} {
	m := b.Gomerr.ToMap()
	errors := make([]map[string]interface{}, len(b.Errors))
	for i, g := range b.Errors {
		errors[i] = g.(Gomerr).ToMap()
	}
	m["_Errors"] = errors

	return m
}
