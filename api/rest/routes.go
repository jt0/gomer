package rest

import (
	"context"
	"net/http"
	"strings"

	api "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/api/http/middleware"
	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/log"
	"github.com/jt0/gomer/resource"
	"github.com/jt0/gomer/structs"
)

// ancestorContext holds information about an ancestor resource for path name derivation.
type ancestorContext struct {
	typeName string // The full type name of the ancestor (e.g., "ExtensionVersion")
	pathName string // The derived path name of the ancestor (e.g., "Version")
}

type HttpSpec struct {
	Method            string
	SuccessStatusCode int
}

var successStatusCodes = map[api.Op]int{
	api.PutCollection:     http.StatusAccepted,
	api.PostCollection:    http.StatusCreated,
	api.GetCollection:     http.StatusOK,
	api.PatchCollection:   http.StatusOK,
	api.DeleteCollection:  http.StatusAccepted,
	api.HeadCollection:    http.StatusOK,
	api.OptionsCollection: http.StatusOK,
	api.PutInstance:       http.StatusOK,
	api.PostInstance:      http.StatusCreated,
	api.GetInstance:       http.StatusOK,
	api.PatchInstance:     http.StatusOK,
	api.DeleteInstance:    http.StatusNoContent,
	api.HeadInstance:      http.StatusOK,
	api.OptionsInstance:   http.StatusOK,
}

// CrudlActions is a helper function to create standard resource actions for a given Instance[I] type.
func CrudlActions[I resource.Instance[I]]() map[any]func() resource.AnyAction {
	return map[any]func() resource.AnyAction{
		api.PostCollection: func() resource.AnyAction { return resource.CreateAction[I]() },
		api.GetInstance:    func() resource.AnyAction { return resource.ReadAction[I]() },
		api.PatchInstance:  func() resource.AnyAction { return resource.UpdateAction[I](resource.ReadAction[I]()) },
		api.DeleteInstance: func() resource.AnyAction { return resource.DeleteAction[I]() },
		api.GetCollection:  func() resource.AnyAction { return resource.ListAction[I]() },
	}
}

// ReadOnlyActions is a helper function to create Read and List actions for a given Instance[I] type.
func ReadOnlyActions[I resource.Instance[I]]() map[any]func() resource.AnyAction {
	return map[any]func() resource.AnyAction{
		api.GetInstance:   func() resource.AnyAction { return resource.ReadAction[I]() },
		api.GetCollection: func() resource.AnyAction { return resource.ListAction[I]() },
	}
}

// NoActions is an empty action map for resources that don't expose REST endpoints.
var NoActions = map[any]func() resource.AnyAction{}

var doAction = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	rw, ok := w.(*api.ResponseWriter)
	if !ok {
		rw = &api.ResponseWriter{}
		defer rw.WriteTo(w)
		w = rw
	}

	ac := middleware.ApiContextFor(r)
	if ac == nil || ac.Instance == nil || ac.Action == nil {
		rw.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	target := middleware.ApiOutput.Output(ac)

	// Execute action via DoAction on the resource
	result, ge := ac.Action.ExecuteOn(r.Context(), target)
	if ge != nil {
		rw.WriteError(ge)
		return
	}

	if result != nil && result != target {
		ac.Target = result
	}
})

func BuildApiContext(rt resource.RegisteredType, action resource.AnyAction, successCode int) func(http.Handler) http.Handler {
	if rt == nil {
		panic("rt cannot be nil")
	} else if action == nil {
		panic("action cannot be nil")
	}

	return func(next http.Handler) http.Handler {
		middleware.ApiContextFor = apiContextFromContext

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ac := &middleware.ApiContext{
				Instance:    rt.NewInstance(middleware.RequestSubject(r)),
				Action:      action,
				SuccessCode: successCode,
			}

			// If CollectionCategory, the instance is the prototype for its collection
			if action.AppliesToCategory() == resource.CollectionCategory {
				ac.Target = rt.NewCollection(ac.Instance)
			}

			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), apiContextKey{}, ac)))
		})
	}
}

type apiContextKey struct{}

func apiContextFromContext(r *http.Request) *middleware.ApiContext {
	return r.Context().Value(apiContextKey{}).(*middleware.ApiContext)
}

func NewRoutes(registry *resource.Registry, middleware ...func(http.Handler) http.Handler) *Routes {
	mux := http.NewServeMux()
	mux.Handle("/", noRouteHandler()) // include catchall handler for unmatched routes
	return &Routes{
		registry:   registry,
		mux:        mux,
		middleware: middleware,
		handler:    doAction,
	}
}

// noRouteHandler returns a handler for requests that don't match any registered route.
func noRouteHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, ok := w.(*api.ResponseWriter)
		if !ok {
			rw = &api.ResponseWriter{}
			defer rw.WriteTo(w)
		}
		rw.WriteError(api.Unroutable())
	})
}

type Routes struct {
	registry        *resource.Registry
	mux             *http.ServeMux
	middleware      []func(http.Handler) http.Handler
	routeMiddleware []func(http.Handler) http.Handler
	handler         http.Handler
}

func (r *Routes) WithRouteMiddleware(middleware ...func(http.Handler) http.Handler) *Routes {
	r.routeMiddleware = append(r.routeMiddleware, middleware...)
	return r
}

func (r *Routes) WithMainHandler(handler http.Handler) *Routes {
	r.handler = handler
	return r
}

func (r *Routes) Build() http.Handler {
	for _, root := range r.registry.RootTypes() {
		r.buildRoutes(root, "", nil)
	}
	return api.Handler(r.registry, r.mux, r.middleware...)
}

func (r *Routes) buildRoutes(rt resource.RegisteredType, parentPath string, ancestors []ancestorContext) {
	if ge := structs.Preprocess(rt.NewInstance(nil), api.DefaultBindFromRequestTool, constraint.DefaultValidationTool); ge != nil {
		panic(ge.String())
	}

	// Determine the path name for this resource's instance type
	instancePathName := pathName(rt.InstanceName(), ancestors)

	hasCollectionAction := false
	for key := range rt.Actions() {
		if key.(api.Op).ResourceType() == resource.CollectionCategory {
			hasCollectionAction = true
			break
		}
	}

	path := make(map[resource.Category]string, 2)
	if hasCollectionAction {
		// Normal CRUD: collections are derived from instance type
		collectionPathName := pathName(rt.CollectionName(), ancestors)
		path[resource.CollectionCategory] = parentPath + "/" + strings.ToLower(collectionPathName)
		path[resource.InstanceCategory] = path[resource.CollectionCategory] + "/{" + instancePathName + "Id}"
	} else {
		// Singleton: use singular path without ID placeholder
		path[resource.InstanceCategory] = parentPath + "/" + strings.ToLower(instancePathName)
	}

	var patterns []string
	for key, actionFunc := range rt.Actions() {
		op := key.(api.Op)

		relativePath, ok := path[op.ResourceType()]
		if !ok {
			panic("invalid resource type; does not map to a path: " + op.ResourceType())
		}

		successStatus, ok := successStatusCodes[op]
		if !ok {
			successStatus = http.StatusOK
		}

		// Register with method and path pattern
		pattern := op.Method() + " " + relativePath
		r.mux.Handle(pattern, r.routeHandler(rt, actionFunc, successStatus))
		patterns = append(patterns, pattern)
	}

	if len(patterns) > 0 {
		log.Debug("adding routes", "resource", rt.InstanceName(), "patterns", patterns)
	}

	// Prepend this resource's context to ancestors for children (closest ancestor first)
	childAncestors := append([]ancestorContext{{
		typeName: rt.InstanceName(),
		pathName: instancePathName,
	}}, ancestors...)
	for _, childMetadata := range rt.Children() {
		r.buildRoutes(childMetadata, path[resource.InstanceCategory], childAncestors)
	}
}

func (r *Routes) routeHandler(rt resource.RegisteredType, actionFunc func() resource.AnyAction, successStatus int) http.Handler {
	action := actionFunc()
	if action == nil {
		panic(gomerr.Configuration("cannot handle a nil action").String())
	}

	buildApiContext := []func(http.Handler) http.Handler{BuildApiContext(rt, action, successStatus)}
	routeChain := api.Chain(append(buildApiContext, r.routeMiddleware...)...)
	return routeChain(r.handler)
}

// pathName derives a path name, applying automatic trimming of redundant prefixes
// based on the ancestor chain.
func pathName(name string, ancestors []ancestorContext) string {
	// No ancestors means no trimming possible
	if len(ancestors) == 0 {
		return name
	}

	// Check each ancestor from closest to furthest
	for _, ancestor := range ancestors {
		// Try to trim ancestor's type name first (longer match takes precedence)
		if len(ancestor.typeName) > 0 && len(name) > len(ancestor.typeName) {
			if strings.EqualFold(name[:len(ancestor.typeName)], ancestor.typeName) {
				return name[len(ancestor.typeName):]
			}
		}

		// Try to trim ancestor's path name
		if len(ancestor.pathName) > 0 && len(name) > len(ancestor.pathName) {
			if strings.EqualFold(name[:len(ancestor.pathName)], ancestor.pathName) {
				return name[len(ancestor.pathName):]
			}
		}
	}
	return name
}
