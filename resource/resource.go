package resource

import (
	"context"
	"unsafe"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
)

// Resource is the base interface for all domain resources. The type parameter T
// is the concrete type implementing the interface (F-bounded polymorphism).
type Resource[T any] interface {
	Metadata() *Metadata
	Subject() auth.Subject
	DoAction(context.Context, Action[T]) (T, gomerr.Gomerr)

	initialize(md *Metadata, sub auth.Subject)
}

// BaseResource provides the default implementation for Resource[T].
// Embed this in concrete resource types.
type BaseResource[T Resource[T]] struct {
	self T
	md   *Metadata
	sub  auth.Subject
}

func (b *BaseResource[T]) Metadata() *Metadata {
	return b.md
}

func (b *BaseResource[T]) Subject() auth.Subject {
	return b.sub
}

func (b *BaseResource[T]) DoAction(ctx context.Context, action Action[T]) (T, gomerr.Gomerr) {
	var zero T

	if ge := action.Pre(ctx, b.self); ge != nil {
		return zero, ge
	}

	if ge := action.Do(ctx, b.self); ge != nil {
		return zero, action.OnDoFailure(ctx, b.self, ge)
	}

	return action.OnDoSuccess(ctx, b.self)
}

func (b *BaseResource[T]) initialize(md *Metadata, sub auth.Subject) {
	// Compute self from receiver using offset stored in Metadata
	containerPtr := unsafe.Pointer(uintptr(unsafe.Pointer(b)) - md.baseOffset)
	b.self = *(*T)(unsafe.Pointer(&containerPtr))
	b.md = md
	b.sub = sub
}

// Category indicates whether a resource is an Instance or Collection.
type Category string

const (
	InstanceCategory   Category = "Instance"
	CollectionCategory Category = "Collection"
)
