package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
)

// withMiddleware wraps a ServeMux with middleware and sets up the ResponseWriter buffering
func withMiddleware(domain *resource.Domain, mux *http.ServeMux, gomerrRenderer func(gomerr.Gomerr) StatusCoder, middleware []func(http.Handler) http.Handler) http.Handler {
	// Outermost middleware that initializes ResponseWriter and finalizes response.
	outer := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Initialize buffered response writer
			rw := &ResponseWriter{}

			// Add gomerr renderer if provided
			if gomerrRenderer != nil {
				rw.errRenderers = []ErrRenderer{gomerrErrRenderer(gomerrRenderer, r)}
			}

			// Call middleware chain with response writer and domain
			next.ServeHTTP(rw, r.WithContext(context.WithValue(r.Context(), resource.DomainCtxKey, domain)))

			// Write buffered response to actual ResponseWriter
			rw.writeTo(w)
		})
	}

	// Chain all middleware, including 'outer'.
	return chain(append([]func(http.Handler) http.Handler{outer}, middleware...)...)(mux)
}

func gomerrErrRenderer(gomerrRenderer func(gomerr.Gomerr) StatusCoder, r *http.Request) ErrRenderer {
	return func(w http.ResponseWriter, err error) bool {
		if ge := gomerr.ErrorAs[gomerr.Gomerr](err); ge != nil {
			rendered := gomerrRenderer(ge)
			renderResult(rendered, w, r, "", rendered.StatusCode())
			return true
		}
		return false
	}
}

// chain combines multiple middleware into a single middleware
func chain(middlewares ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(final http.Handler) http.Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			final = middlewares[i](final)
		}
		return final
	}
}

type Options struct {
	Port int16
}

// Serve starts the HTTP server
func Serve(handler http.Handler, optFns ...func(*Options)) {
	o := &Options{
		Port: 8080,
	}

	for _, optFn := range optFns {
		optFn(o)
	}

	// log.Info("Serving on: {}", o.Port)
	println("Serving on:", "localhost:"+strconv.Itoa(int(o.Port)))
	err := http.ListenAndServe("localhost:"+strconv.Itoa(int(o.Port)), handler)

	var shutdownInfo string
	if err != nil {
		shutdownInfo = " due to: " + err.Error()
	} else {
		shutdownInfo = " cleanly"
	}
	// log.Error("Server shutdown{}", shutdownInfo)
	println("server shutdown", shutdownInfo)
}

func noOptFn(*Options) {}

func Port(p string) func(*Options) {
	i, err := strconv.ParseInt(p, 10, 16)
	if err != nil {
		if p != "" {
			// log.Error()
			println("invalid port value, ignoring:", p)
		}
		return noOptFn
	}
	return func(o *Options) {
		o.Port = int16(i)
	}
}

type ErrRenderer = func(w http.ResponseWriter, err error) bool

// ResponseWriter buffers the response and supports error rendering
type ResponseWriter struct {
	statusCode   int
	header       http.Header
	body         []byte
	err          error
	errRenderers []ErrRenderer
}

func (rw *ResponseWriter) Header() http.Header {
	if rw.header == nil {
		rw.header = make(http.Header)
	}
	return rw.header
}

func (rw *ResponseWriter) Write(b []byte) (int, error) {
	rw.body = b
	return len(b), nil
}

func (rw *ResponseWriter) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
}

func (rw *ResponseWriter) WriteError(err error, optionalRenderers ...ErrRenderer) {
	if len(optionalRenderers) > 0 {
		rw.errRenderers = append(rw.errRenderers, optionalRenderers...)
	}
	rw.err = err
}

func (rw *ResponseWriter) writeTo(w http.ResponseWriter) {
	// Check for errors and render if present
	if rw.err != nil {
		for _, render := range rw.errRenderers {
			if render(w, rw.err) {
				return
			}
		}
		// Default error rendering
		defaultErrorRenderer(w, rw.err)
		return
	}

	// Write buffered response
	if len(rw.header) > 0 {
		for h, hv := range rw.header {
			w.Header()[h] = hv
		}
	}
	if rw.statusCode != 0 {
		w.WriteHeader(rw.statusCode)
	}
	w.Write(rw.body)
}

func defaultErrorRenderer(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)

	// TODO: add flag to output details only if running in non-prod

	if ge, ok := err.(gomerr.Gomerr); ok {
		w.Write([]byte(ge.String()))
		return
	}

	var m map[string]any
	if me := json.Unmarshal([]byte(err.Error()), m); me != nil {
		escaped, _ := json.Marshal(err.Error())
		w.Write([]byte("{\"error\": " + string(escaped) + "}"))
	} else {
		out, _ := json.MarshalIndent(m, "", "  ")
		w.Write(out)
	}
}
