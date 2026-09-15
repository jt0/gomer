package middleware

import (
	"errors"
	"net/http"
	"reflect"

	api "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/gomerr"
)

// RenderErrorMiddleware returns middleware that renders gomerr errors using the provided renderer function.
// The renderer maps gomerr types to StatusCoder implementations (e.g., AWS exception structs) which are
// then serialized to the response using BindToResponse.
func RenderErrorMiddleware(renderer func(gomerr.Gomerr) api.StatusCoder) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw, ok := w.(*api.ResponseWriter)
			if !ok {
				rw = &api.ResponseWriter{}
				defer rw.WriteTo(w)
				w = rw
			}

			next.ServeHTTP(w, r)

			if rw.Error() == nil || errors.Is(rw.Error(), gomerr.NotAnError) {
				return
			}

			if ge := gomerr.ErrorAs[gomerr.Gomerr](rw.Error()); ge != nil {
				if ue := gomerr.ErrorAs[*api.UnroutableError](ge); ue != nil {
					ue.Route = r.Method + " " + r.URL.Path
				}
				rendered := renderer(ge)
				bytes, statusCode := api.BindToResponse(reflect.ValueOf(rendered), rw.Header(), "", r.Header.Get("Accept-Language"), rendered.StatusCode())
				rw.WriteHeader(statusCode)
				rw.Write(bytes)
				rw.WriteError(nil)
			}
		})
	}
}
