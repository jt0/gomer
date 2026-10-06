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
			rw, flush := api.AsResponseWriter(&w)
			defer flush()

			next.ServeHTTP(w, r)

			if rw.Error() == nil || errors.Is(rw.Error(), gomerr.NotAnError) {
				return
			}

			if ge, ok := errors.AsType[gomerr.Gomerr](rw.Error()); ok {
				if ue, ueOk := errors.AsType[*api.UnroutableError](ge); ueOk {
					ue.Route = r.Method + " " + r.URL.Path
				}
				rendered := renderer(ge)
				bytes, statusCode := api.BindToResponse(reflect.ValueOf(rendered), rw.Header(), "", r.Header.Get("Accept-Language"), rendered.StatusCode())
				rw.WriteHeader(statusCode)
				rw.Overwrite(bytes)
				rw.WriteError(nil)
			}
		})
	}
}
