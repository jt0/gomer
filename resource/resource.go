package resource

import (
	"context"
	"unsafe"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/log"
)

// Resource is the base interface for all domain resources. The type parameter T
// is the concrete type implementing the interface (F-bounded polymorphism).
type Resource[T any] interface {
	Subject() auth.Subject
	DoAction(context.Context, Action[T]) (T, gomerr.Gomerr)
	RegisteredType() RegisteredType
	MaxActionRetries() int

	registeredType() *registeredType
	initialize(rt *registeredType, sub auth.Subject)
}

// BaseResource provides the default implementation for Resource[T].
// Embed this in concrete resource types.
type BaseResource[T Resource[T]] struct {
	self T
	rt   *registeredType
	sub  auth.Subject
}

func (b *BaseResource[T]) Subject() auth.Subject {
	return b.sub
}

func (b *BaseResource[T]) Self() T {
	return b.self
}

func (b *BaseResource[T]) DoAction(ctx context.Context, action Action[T]) (T, gomerr.Gomerr) {
	var zero T
	if ge := action.Pre(ctx, b.self); ge != nil {
		return zero, ge
	}

	ge := action.Do(ctx, b.self)
	for i, maxRetries := 1, b.self.MaxActionRetries(); i <= maxRetries && ge != nil; i++ {
		if ge = action.Retry(ctx, b.self, ge); ge != nil {
			return zero, action.OnDoFailure(ctx, b.self, ge)
		}
		ge = action.Do(ctx, b.self)
	}
	if ge != nil {
		log.Error("exceeded max retry count; validate logic and override if needed", "maxRetries", b.self.MaxActionRetries())
	}

	return action.OnDoSuccess(ctx, b.self)
}

func (b *BaseResource[T]) RegisteredType() RegisteredType {
	return b.rt
}

const DefaultMaxActionRetries = 3

func (b *BaseResource[T]) MaxActionRetries() int {
	return DefaultMaxActionRetries
}

func (b *BaseResource[T]) registeredType() *registeredType {
	return b.rt
}

func (b *BaseResource[T]) initialize(rt *registeredType, sub auth.Subject) {
	// Compute self from receiver using offset stored in type info
	containerPtr := unsafe.Pointer(uintptr(unsafe.Pointer(b)) - rt.baseOffset)
	b.self = *(*T)(unsafe.Pointer(&containerPtr))
	b.rt = rt
	b.sub = sub
}

// Category indicates whether a resource is an Instance or Collection.
type Category string

const (
	InstanceCategory   Category = "Instance"
	CollectionCategory Category = "Collection"
)
