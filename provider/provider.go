package provider

import (
	"context"
	"sync"
	"time"
)

type Provider[T any] interface {
	Provide(context.Context) (T, error)
}

// SingletonProvider offers thread-safe initialization of a value. When Provide is called, the result of calling Once
// is cached permanently.
type SingletonProvider[T any] struct {
	Once func(context.Context) (T, error)

	once sync.Once
	t    T
	err  error
}

func (s *SingletonProvider[T]) Provide(ctx context.Context) (T, error) {
	s.once.Do(func() {
		s.t, s.err = s.Once(ctx)
	})
	return s.t, s.err
}

// RetryableSingleton offers thread-safe initialization of a value, with the ability to retry after a configurable
// period of time (default: 30s) if the Once function returned an error. Upon success, the value is cached permanently.
type RetryableSingleton[T comparable] struct {
	Once       func(context.Context) (T, error)
	RetryAfter time.Duration

	mu   sync.Mutex
	t    T
	err  error
	last time.Time
}

func (r *RetryableSingleton[T]) Provide(ctx context.Context) (T, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var zero T
	if r.t != zero {
		return r.t, nil
	}

	const defaultRetryAfter = 30 * time.Second
	if r.RetryAfter == 0 {
		r.RetryAfter = defaultRetryAfter
	}

	if r.err != nil && time.Since(r.last) < r.RetryAfter {
		return zero, r.err
	}

	r.last = time.Now()
	r.t, r.err = r.Once(ctx)
	return r.t, r.err
}
