package middleware

import (
	"net/http"

	api "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/log"
)

func ValidateInput(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, flush := api.AsResponseWriter(&w)
		defer flush()

		ac := ApiContextFor(r)
		if ac == nil || ac.Instance == nil || ac.Action == nil {
			log.Warn("no api context to validate", "route", r.Method+" "+r.URL.Path)
			next.ServeHTTP(w, r)
			return
		}

		scope := ac.Action.Name()
		if ge := constraint.Validate(ac, scope); ge != nil {
			rw.WriteError(ge)
			return
		}

		next.ServeHTTP(w, r)
	})
}
