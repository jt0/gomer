package middleware

import (
	"context"
	"errors"
	"net/http"

	api "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/log"
)

// RequestSubject returns the auth.Subject from the provided request. If the
// SubjectHandler middleware isn't used, or the middleware's SubjectProvider
// returns an error, this function returns nil.
var RequestSubject = func(*http.Request) auth.Subject {
	return nil
}

type SubjectProvider func(*http.Request) (auth.Subject, gomerr.Gomerr)

func SubjectHandler(subjectProvider SubjectProvider) func(http.Handler) http.Handler {
	if subjectProvider == nil {
		subjectProvider = func(*http.Request) (auth.Subject, gomerr.Gomerr) {
			return auth.NewSubject(auth.NoFieldAccess), nil
		}
	}

	RequestSubject = requestSubjectFromContext

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw, ok := w.(*api.ResponseWriter)
			if !ok {
				rw = &api.ResponseWriter{}
				defer rw.WriteTo(w)
				w = rw
			}

			subject, ge := subjectProvider(r)
			if ge != nil {
				rw.WriteError(ge)
				return
			}

			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), subjectKey{}, subject)))

			ge = subject.Release(rw.Error() != nil && !errors.Is(rw.Error(), gomerr.NotAnError))
			if ge != nil {
				log.Logger().Warn("failed to release subject", "error", ge)
			}
		})
	}
}

type subjectKey struct{}

func requestSubjectFromContext(r *http.Request) auth.Subject {
	return r.Context().Value(subjectKey{}).(auth.Subject)
}
