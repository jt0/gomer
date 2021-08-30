package gomerr

import (
	"reflect"
)

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

func Batch(errors ...Gomerr) Gomerr {
	var nnErrors []Gomerr
	for _, ge := range errors {
		if ge != nil {
			nnErrors = append(nnErrors, ge)
		}
	}
	return Batcher(nnErrors)
}

func (b *BatchError) Errors() []Gomerr {
	return b.errors
}

var batchTypeString = reflect.TypeOf((*BatchError)(nil)).String()

func (b *BatchError) ToMap() map[string]interface{} {
	errors := make([]map[string]interface{}, len(b.errors))
	for i, ge := range b.errors {
		errors[i] = ge.ToMap()
	}

	m := map[string]interface{}{
		"$.errorType": batchTypeString,
		"Errors":      errors,
	}

	g := b.Gomerr.(*gomerr)
	if g.attributes != nil && len(g.attributes) > 0 {
		m["_attributes"] = g.attributes
	}

	return m
}
