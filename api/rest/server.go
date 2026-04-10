package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"

	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
)

// withMiddleware wraps a ServeMux with middleware and sets up the ResponseWriter buffering
func withMiddleware(registry *resource.Registry, mux *http.ServeMux, middleware []func(http.Handler) http.Handler) http.Handler {
	// Outermost middleware that initializes ResponseWriter and finalizes response.
	outer := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Initialize buffered response writer
			rw := &ResponseWriter{}

			// Call middleware chain with response writer and registry
			next.ServeHTTP(rw, r.WithContext(context.WithValue(r.Context(), resource.RegistryCtxKey, registry)))

			// Write buffered response to actual ResponseWriter
			rw.writeTo(w)
		})
	}

	// Chain all middleware, including 'outer'.
	return chain(append([]func(http.Handler) http.Handler{outer}, middleware...)...)(mux)
}

// RenderErrorMiddleware returns middleware that renders gomerr errors using the provided renderer function.
// The renderer maps gomerr types to StatusCoder implementations (e.g., AWS exception structs) which are
// then serialized to the response using BindToResponse.
func RenderErrorMiddleware(renderer func(gomerr.Gomerr) StatusCoder) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)

			rw, ok := w.(*ResponseWriter)
			if !ok || rw.err == nil {
				return
			}

			if ge := gomerr.ErrorAs[gomerr.Gomerr](rw.err); ge != nil {
				rendered := renderer(ge)
				bytes, statusCode := BindToResponse(reflect.ValueOf(rendered), rw.Header(), "", r.Header.Get("Accept-Language"), rendered.StatusCode())
				rw.statusCode = statusCode
				rw.body = bytes
				rw.err = nil
			}
		})
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
	println("serving on", "localhost:"+strconv.Itoa(int(o.Port)))
	err := http.ListenAndServe("localhost:"+strconv.Itoa(int(o.Port)), handler)

	var shutdownInfo string
	if err != nil {
		shutdownInfo = " due to: " + err.Error()
	} else {
		shutdownInfo = " cleanly"
	}
	// log.Error("server shutdown{}", shutdownInfo)
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

// ResponseWriter buffers the response and supports error rendering
type ResponseWriter struct {
	statusCode int
	header     http.Header
	body       []byte
	err        error
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

func (rw *ResponseWriter) WriteError(err error) {
	rw.err = err
}

func (rw *ResponseWriter) StatusCode() int {
	return rw.statusCode
}

func (rw *ResponseWriter) Body() []byte {
	return rw.body
}

func (rw *ResponseWriter) writeTo(w http.ResponseWriter) {
	// If an error remains unhandled by middleware, use the default renderer
	if rw.err != nil {
		defaultErrorRenderer(w, rw.err)
		return
	}

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

	if ge := gomerr.ErrorAs[gomerr.Gomerr](err); ge != nil {
		println(ge.String())
		w.Write([]byte(ge.Error()))
		return
	}

	var m map[string]any
	if me := json.Unmarshal([]byte(err.Error()), &m); me != nil {
		escaped, _ := json.Marshal(err.Error())
		w.Write([]byte("{\"error\": " + string(escaped) + "}"))
	} else {
		out, _ := json.MarshalIndent(m, "", "  ")
		w.Write(out)
	}
}
