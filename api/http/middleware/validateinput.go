package middleware

import (
	"net/http"

	api "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/log"
	"github.com/jt0/gomer/structs"
)

func ValidateInput(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, ok := w.(*api.ResponseWriter)
		if !ok {
			rw = &api.ResponseWriter{}
			defer rw.WriteTo(w)
			w = rw
		}

		ac := ApiContextFor(r)
		if ac == nil || ac.Instance == nil || ac.Action == nil {
			log.Warn("no api context to validate", "route", r.Method+" "+r.URL.Path)
			next.ServeHTTP(w, r)
			return
		}

		scope := ac.Action.Name()
		if ge := structs.ApplyTools(ApiContextFor(r), structs.ToolContextWithScope(scope), constraint.DefaultValidationTool); ge != nil {
			rw.WriteError(ge)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// func handler(rt resource.RegisteredType, actionFunc func() resource.AnyAction, successStatus int) http.Handler {
//	anyAction := actionFunc()
//	if anyAction == nil {
//		panic(gomerr.Configuration("cannot handle a nil action").String())
//	}
//	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//		rw, ok := w.(*api.ResponseWriter)
//		if !ok {
//			rw = &api.ResponseWriter{}
//			defer rw.WriteTo(w)
//		}
//
//		// Bind request data to new instance
//		res := rt.NewInstance(middleware.RequestSubject(r)).(resource.AnyResource)
//		if ge := api.BindAndValidateFromRequest(r, res, anyAction.Name()); ge != nil {
//			rw.WriteError(ge)
//			return
//		}
//
//		// If CollectionCategory, we use the bound instance as the prototype for its collection type
//		if anyAction.AppliesToCategory() == resource.CollectionCategory {
//			res = rt.NewCollection(res)
//		}
//
//		// Execute action via DoAction on the resource
//		result, ge := anyAction.ExecuteOn(r.Context(), res)
//		if ge != nil {
//			rw.WriteError(ge)
//			return
//		}
//
//		renderResult(result, rw, r, anyAction.Name(), successStatus)
//	})
// }
//
// func renderResult(result any, w http.ResponseWriter, r *http.Request, scope string, statusCode int) {
//	bytes, statusCode := api.BindToResponse(reflect.ValueOf(result), w.Header(), scope, r.Header.Get("Accept-Language"), statusCode)
//	w.WriteHeader(statusCode)
//	w.Write(bytes)
// }
