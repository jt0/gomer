package http

import (
	"bufio"
	"errors"
	"maps"
	"net"
	"net/http"
	"slices"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/log"
)

// ResponseWriter buffers the response and supports error rendering
type ResponseWriter struct {
	statusCode int
	header     http.Header
	body       []byte
	err        error
	w          http.ResponseWriter
	hijacked   bool
	started    bool
}

// AsResponseWriter makes *w a *ResponseWriter, wrapping the original if it isn't one
// already. The returned flush writes the buffered response to the original writer and
// must be called, usually deferred, when the handler is done.
func AsResponseWriter(w *http.ResponseWriter) (rw *ResponseWriter, flush func()) {
	if existing, ok := (*w).(*ResponseWriter); ok {
		return existing, func() {}
	}
	rw = &ResponseWriter{w: *w}
	*w = rw
	return rw, func() { rw.Flush() }
}

func (rw *ResponseWriter) Header() http.Header {
	if rw.header == nil {
		rw.header = make(http.Header)
	}
	return rw.header
}

func (rw *ResponseWriter) Write(b []byte) (int, error) {
	rw.body = append(rw.body, b...)
	return len(b), nil
}

// Overwrite replaces the buffered body with b, which is taken to be unrelated to what it
// replaces (an error in place of a success, say), so the headers that described the old
// body are dropped too.
func (rw *ResponseWriter) Overwrite(b []byte) (int, error) {
	deleteRepresentationHeaders(rw.Header())
	rw.body = rw.body[:0]
	return rw.Write(b)
}

// deleteRepresentationHeaders removes the headers that describe a particular body.
func deleteRepresentationHeaders(h http.Header) {
	h.Del("Content-Length")
	h.Del("Content-Encoding")
	h.Del("Content-Range")
	h.Del("ETag")
	h.Del("Last-Modified")
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
	return slices.Clone(rw.body)
}

func (rw *ResponseWriter) Error() error {
	return rw.err
}

func (rw *ResponseWriter) WriteTo(w http.ResponseWriter) {
	if len(rw.header) > 0 {
		maps.Copy(w.Header(), rw.header)
	}

	// If an error remains unhandled by middleware, use the default renderer
	if rw.err != nil && !errors.Is(rw.err, gomerr.NotAnError) {
		defaultErrorRenderer(w, rw.err)
		rw.err = nil
		rw.body = rw.body[:0]
		return
	}

	if rw.statusCode != 0 {
		w.WriteHeader(rw.statusCode)
	}

	if len(rw.body) > 0 {
		w.Write(rw.body)
		rw.body = rw.body[:0]
	}
}

// Hijack hands the connection to the caller. The caller owns every byte from here on, so
// anything buffered is discarded and the final flush does nothing.
func (rw *ResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, brw, err := http.NewResponseController(rw.w).Hijack()
	if err != nil {
		return nil, nil, err // e.g. http.ErrNotSupported on HTTP/2
	}
	rw.hijacked = true
	return conn, brw, nil
}

func (rw *ResponseWriter) Flush() {
	if rw.hijacked {
		return
	}
	if !rw.started {
		rw.started = true
		rw.WriteTo(rw.w)
	} else {
		if rw.err != nil && !errors.Is(rw.err, gomerr.NotAnError) {
			log.Warn("error after the response started; it cannot be rendered", "error", rw.err.Error())
			rw.err = nil
		}
		rw.w.Write(rw.body)
		rw.body = rw.body[:0]
	}
	_ = http.NewResponseController(rw.w).Flush() // ErrNotSupported is fine; the body is written either way
}

func (rw *ResponseWriter) Unwrap() http.ResponseWriter {
	return rw.w
}

func defaultErrorRenderer(w http.ResponseWriter, err error) {
	log.Error("defaultErrorRender: unhandled error, returning 500", "error", err.Error())

	header := w.Header()
	deleteRepresentationHeaders(header)
	header.Set("Content-Type", "application/json")

	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(`{"message": "internal server error"}`))
}
