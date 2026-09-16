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

func NewApi(registry *resource.Registry, apiMiddleware ...func(http.Handler) http.Handler) *Api {
	mux := http.NewServeMux()

	// "unroutable" handler for anything that doesn't match
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, ok := w.(*api.ResponseWriter)
		if !ok {
			rw = &api.ResponseWriter{}
			defer rw.WriteTo(w)
		}
		rw.WriteError(api.Unroutable())
	}))

	return &Api{
		registry:      registry,
		mux:           mux,
		apiMiddleware: apiMiddleware,
		handler:       DoResourceAction,
	}
}

type Api struct {
	registry           *resource.Registry
	mux                *http.ServeMux
	apiMiddleware      []func(http.Handler) http.Handler
	resourceMiddleware []func(http.Handler) http.Handler
	handler            http.Handler
}

func (r *Api) WithResourceMiddleware(middleware ...func(http.Handler) http.Handler) *Api {
	r.resourceMiddleware = append(r.resourceMiddleware, middleware...)
	return r
}

func (r *Api) WithResourceHandler(handler http.Handler) *Api {
	r.handler = handler
	return r
}

func (r *Api) Build() http.Handler {
	for _, root := range r.registry.RootTypes() {
		r.buildRoutes(root, "", nil)
	}
	return api.Handler(r.registry, r.mux, r.apiMiddleware...)
}

// ancestorContext holds information about an ancestor resource for path name derivation.
type ancestorContext struct {
	typeName string // The full type name of the ancestor (e.g., "ExtensionVersion")
	pathName string // The derived path name of the ancestor (e.g., "Version")
}

func (r *Api) buildRoutes(rt resource.RegisteredType, parentPath string, ancestors []ancestorContext) {
	if ge := structs.Preprocess(rt.NewInstance(nil), api.DefaultBindFromRequestTool, constraint.DefaultValidationTool); ge != nil {
		panic(ge.String())
	}

	// Determine the path name for this resource's instance type
	instancePathName := pathName(rt.InstanceName(), ancestors)

	hasCollectionAction := false
	for key := range rt.Actions() {
		if key.(Op).ResourceType() == resource.CollectionCategory {
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
		op := key.(Op)
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

func (r *Api) routeHandler(rt resource.RegisteredType, actionFunc func() resource.AnyAction, successStatus int) http.Handler {
	action := actionFunc()
	if action == nil {
		panic(gomerr.Configuration("cannot handle a nil action").String())
	}

	buildApiContext := []func(http.Handler) http.Handler{BuildApiContext(rt, action, successStatus)}
	routeChain := api.Chain(append(buildApiContext, r.resourceMiddleware...)...)
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
