package middleware

import (
	"net/http"
	"reflect"

	api "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/log"
)

// LocalRedirectIfCanonical provides a "local" redirect when the net/http.ServeMux
// returns a "canonical path" redirection. That means the mux recognized that it has a
// match for the path '/foo/' and the client requested '/foo' without the trailing
// slash. Rather than return a 404, the mux sends a 301 response to the client with the
// correct location it should request from. , this redirects locally to
// skip the unnecessary roundtrip.
//
// Note: this is intended to directly wrap Go's http.ServeMux specifically to avoid the
// side effects of any other middleware that may have been called before it. If the
// passed in handler is some other type, this middleware is ignored.
func LocalRedirectIfCanonical(next http.Handler) http.Handler {
	// Intended to directly wrap Go's ServeMux. If it's a different type, ignore this.
	if _, ok := next.(*http.ServeMux); !ok {
		log.Warn("'next' is not an http.ServeMux, skipping local redirect middleware", "found", reflect.TypeOf(next).String())
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, ok := w.(*api.ResponseWriter)
		if !ok {
			rw = &api.ResponseWriter{}
			defer rw.WriteTo(w)
			w = rw
		}

		next.ServeHTTP(rw, r)

		if sc := rw.StatusCode(); sc != http.StatusTemporaryRedirect && sc != http.StatusMovedPermanently {
			return
		}

		if location := rw.Header().Get("Location"); location == r.URL.Path+"/" {
			// Update the request path, clear out http.Redirect()-written
			// headers, and clear out the body before we re-drive the request.
			r.URL.Path = location
			outHeader := rw.Header()
			outHeader.Del("Location")
			outHeader.Del("Content-Type")
			outHeader.Del("Content-Length")
			rw.Write(nil)

			next.ServeHTTP(w, r)
		}
	})
}
