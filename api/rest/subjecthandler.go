package rest

import (
	"context"
	"net/http"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
)

var Subject = NilSubject

func NilSubject(*http.Request) auth.Subject {
	return nil
}

type SubjectProvider func(*http.Request) (auth.Subject, gomerr.Gomerr)

func SubjectHandler(subjectProvider SubjectProvider) func(http.Handler) http.Handler {
	if subjectProvider == nil {
		subjectProvider = func(*http.Request) (auth.Subject, gomerr.Gomerr) {
			return auth.NewSubject(auth.NoFieldAccess), nil
		}
	}

	Subject = SubjectHandlerSubject

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw := w.(*ResponseWriter)

			subject, ge := subjectProvider(r)
			if ge != nil {
				rw.WriteError(ge)
				return // Don't call next handler
			}

			// Store subject in context
			r = r.WithContext(context.WithValue(r.Context(), subjectKey{}, subject))

			// Call next handler
			next.ServeHTTP(w, r)

			// Post-processing: Release subject
			ge = subject.Release(rw.err != nil)
			if ge != nil {
				// TODO: log but don't error
			}
		})
	}
}

type subjectKey struct{}

func SubjectHandlerSubject(r *http.Request) auth.Subject {
	return r.Context().Value(subjectKey{}).(auth.Subject)
}
