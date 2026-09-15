package http

import (
	"context"
	"net/http"

	"github.com/jt0/gomer/resource"
)

// Handler wraps a ServeMux with middleware and sets up the ResponseWriter buffering
func Handler(registry *resource.Registry, mux *http.ServeMux, middleware ...func(http.Handler) http.Handler) http.Handler {
	// Outermost middleware that initializes ResponseWriter and finalizes response.
	outer := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Initialize buffered response writer
			rw := &ResponseWriter{}

			// Call middleware chain with response writer and registry
			next.ServeHTTP(rw, r.WithContext(context.WithValue(r.Context(), resource.RegistryCtxKey, registry)))

			// Write buffered response to actual ResponseWriter
			rw.WriteTo(w)
		})
	}

	// Chain all middleware, including 'outer'.
	return Chain(append([]func(http.Handler) http.Handler{outer}, middleware...)...)(mux)
}

// Chain creates a closure around the passed-in middleware, returning a function suitable
// for calling with an "inner" handler that implements whatever logic is needed. The
// returned function is reusable across handlers, and chains can be linked together as
// desired.
func Chain(middleware ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(inner http.Handler) http.Handler {
		for i := len(middleware) - 1; i >= 0; i-- {
			inner = middleware[i](inner)
		}
		return inner
	}
}

//func ChainedHandler(middleware ...func(http.Handler) http.Handler) http.Handler {
//	partial
//	for i := len(middleware) - 1; i >= 0; i-- {
//		inner = middleware[i](inner)
//	}
//	return inner
//}
