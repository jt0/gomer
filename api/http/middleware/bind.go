package middleware

import (
	"net/http"

	api "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/log"
)

type InputProvider interface {
	Input(apiContext *ApiContext) any
}

type OutputProvider interface {
	Output(apiContext *ApiContext) any
}

// Bind
func Bind(inProvider InputProvider, outProvider OutputProvider) func(http.Handler) http.Handler {
	if inProvider == nil {
		inProvider = ApiInput
	}
	if outProvider == nil {
		outProvider = ApiOutput
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw, ok := w.(*api.ResponseWriter)
			if !ok {
				rw = &api.ResponseWriter{}
				defer rw.WriteTo(w)
				w = rw
			}

			ac := ApiContextFor(r)
			if ac == nil || ac.Instance == nil || ac.Action == nil {
				log.Warn("no api context to bind", "route", r.Method+" "+r.URL.Path)
				next.ServeHTTP(w, r)
				return
			}

			in := inProvider.Input(ac)

			scope := ac.Action.Name()
			if ge := api.BindFromRequest(r, in, scope); ge != nil {
				rw.WriteError(ge)
				return
			}

			next.ServeHTTP(w, r)

			if rw.Error() != nil {
				return
			}

			out := outProvider.Output(ac)
			bytes, statusCode := api.BindToResponse(out, w.Header(), scope, r.Header.Get("Accept-Language"), ac.SuccessCode)
			rw.WriteHeader(statusCode)
			rw.Write(bytes)
		})
	}
}

var (
	ApiInput  = apiProvider{}
	ApiOutput = apiProvider{}
)

type apiProvider struct{}

func (apiProvider) Input(ac *ApiContext) any {
	return ac.Instance
}

func (apiProvider) Output(ac *ApiContext) any {
	if ac.Target != nil {
		return ac.Target
	}
	return ac.Instance
}
