package gomerr

type BatchError struct {
	Gomerr
	errors []Gomerr
}

func Batcher(errors []Gomerr) Gomerr {
	switch len(errors) {
	case 0:
		return nil
	case 1:
		return errors[0]
	default:
		b := Build(&BatchError{}).(*BatchError)
		b.errors = errors
		return b
	}
}

func (b *BatchError) Errors() []Gomerr {
	return b.errors
}

func (b *BatchError) ToMap() map[string]interface{} {
	m := b.Gomerr.ToMap()
	errors := make([]map[string]interface{}, len(b.errors))
	for i, g := range b.errors {
		errors[i] = g.ToMap()
	}
	m["_Errors"] = errors

	return m
}
