package gomerr

import (
	"reflect"
)

// ErrorBatch collects zero or more errors that are worth handing as a group if multiple
// errors might be produced in, for example, a processing or validation loop. Once the
// collection phase is complete, call GomerrOrNil to produce the minimal representation
// of the collected errors.
//
//	var eb gomerr.Batch
//	for ... {
//		eb.Add(ge)
//	}
//	return eb.GomerrOrNil()
//
// An ErrorBatch keeps at most maxBatchErrors errors; Capture drops the rest and the
// resulting error is marked as Full.
type ErrorBatch struct {
	errors []Gomerr
	state  state
}

// Capture checks the provided value, and if not nil, adds it to the ErrorBatch. If the
// Gomerr is a BatchError, Capture pulls out its errors, flattening them into this
// ErrorBatch.
//
// Any errors passed to Capture after ErrorBatch.Full returns true are dropped and the
// batch is marked as "truncated".
//
// Capture can simplify code where a nil check on a Gomerr is used to determine whether
// to add it to an ErrorBatch. Instead of:
//
//	if ge := maybeError(); if ge != nil {
//		eb.Capture(ge)
//	}
//
// you can do:
//
//	eb.Capture(maybeError())
func (eb *ErrorBatch) Capture(ge Gomerr) {
	if ge == nil {
		return
	}
	//goland:noinspection GoTypeAssertionOnErrors
	if nested, ok := ge.(*BatchError); ok {
		for _, e := range nested.errors {
			eb.add(e)
		}
		return
	}
	eb.add(ge)
}

// errorBatchCapacity bounds how many errors a ErrorBatch keeps, so input that fails
// element by element (an unbounded list, for example) can't produce an unbounded error.
const errorBatchCapacity = 100

type state byte

const (
	notEmpty state = 1 << iota
	full
	truncated
)

func (eb *ErrorBatch) add(ge Gomerr) {
	if eb.Full() {
		eb.state |= truncated
		return
	}
	eb.errors = append(eb.errors, ge)
	if len(eb.errors) < errorBatchCapacity {
		eb.state |= notEmpty
	} else {
		eb.state |= full
	}
}

// HasErrors reports whether Capture has collectd any errors or not.
func (eb *ErrorBatch) HasErrors() bool {
	return eb.state&notEmpty != 0
}

// Full reports whether Capture will accept further errors or not. Once full, Capture
// silently drops anything it receives, so callers should stop further processing that
// might produce errors that that would be added to this ErrorBatch.
func (eb *ErrorBatch) Full() bool {
	return eb.state&full != 0
}

// Truncated reports whether any errors passed to Capture have been dropped or not.
func (eb *ErrorBatch) Truncated() bool {
	return eb.state&truncated != 0
}

// TruncatedAttribute is set to true on a BatchError when its ErrorBatch dropped errors.
const TruncatedAttribute = "truncated"

// GomerrOrNil returns nil if the batch is empty, the error itself if it holds one, and
// otherwise a *BatchError over all of them.
func (eb *ErrorBatch) GomerrOrNil() Gomerr {
	switch len(eb.errors) {
	case 0:
		return nil
	case 1:
		return eb.errors[0]
	}
	be := &BatchError{errors: eb.errors}
	be.Gomerr = &gomerr{self: be}
	if eb.state&truncated != 0 {
		be.AddAttribute(TruncatedAttribute, true)
	}
	return be
}

// ToGomerrOrNil constructs an ErrorBatch, calls ErrorBatch.Capture on each error, then
// returns the result ErrorBatch.GomerrOrNil. It's usually better to perform these steps
// oneself since it avoids the need to self-manage an error slice or similar.
func ToGomerrOrNil(errors ...Gomerr) Gomerr {
	var eb ErrorBatch
	for _, ge := range errors {
		eb.Capture(ge)
	}
	return eb.GomerrOrNil()
}

// BatchError is the error for two or more errors collected by a ErrorBatch. Its errors are
// never themselves batches.
type BatchError struct {
	Gomerr
	errors []Gomerr
}

func (b *BatchError) Errors() []Gomerr {
	return b.errors
}

var batchTypeString = reflect.TypeFor[*BatchError]().String()

func (b *BatchError) ToMap() map[string]any {
	items := make([]map[string]any, len(b.errors))
	for i, ge := range b.errors {
		items[i] = ge.ToMap()
	}
	m := map[string]any{
		"$.errorType": batchTypeString,
		"Errors":      items,
	}
	if attrs := b.Attributes(); len(attrs) > 0 {
		m["_attributes"] = attrs
	}
	return m
}
