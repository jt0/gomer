package http

import (
	"errors"
	"net/http"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/log"
)

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

func (rw *ResponseWriter) Error() error {
	return rw.err
}

func (rw *ResponseWriter) WriteTo(w http.ResponseWriter) {
	// If an error remains unhandled by middleware, use the default renderer
	if rw.err != nil && !errors.Is(rw.err, gomerr.NotAnError) {
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
	log.Error("defaultErrorRender: unhandled error, returning 500", "error", err.Error())

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(`{"message": "internal server error"}`))
}
