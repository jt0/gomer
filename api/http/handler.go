package http

import (
	"context"
	"net/http"
	"slices"

	"github.com/jt0/gomer/resource"
)

// Handler wraps a ServeMux with middleware and sets up the ResponseWriter buffering
func Handler(registry *resource.Registry, mux *http.ServeMux, middleware ...func(http.Handler) http.Handler) http.Handler {
	// Outermost middleware that initializes ResponseWriter and finalizes response.
	outer := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Buffer the response, writing it to the actual ResponseWriter once the chain returns
			rw, flush := AsResponseWriter(&w)
			defer flush()

			// Call middleware chain with response writer and registry
			next.ServeHTTP(rw, r.WithContext(context.WithValue(r.Context(), resource.RegistryCtxKey, registry)))
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
		for _, m := range slices.Backward(middleware) {
			inner = m(inner)
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
