package middleware

import (
	"net/http"

	api "github.com/jt0/gomer/api/http"
)

func NoRedirect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, flush := api.AsResponseWriter(&w)
		defer flush()

		next.ServeHTTP(w, r)

		if rw.StatusCode()/100 == 3 {
			rw.WriteError(api.Unroutable())
		}
	})
}
