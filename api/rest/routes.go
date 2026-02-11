package rest

import (
	"net/http"
	"reflect"
	"strings"

	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
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

var successStatusCodes = map[Op]int{
	PutCollection:     http.StatusAccepted,
	PostCollection:    http.StatusCreated,
	GetCollection:     http.StatusOK,
	PatchCollection:   http.StatusOK,
	DeleteCollection:  http.StatusAccepted,
	HeadCollection:    http.StatusOK,
	OptionsCollection: http.StatusOK,
	PutInstance:       http.StatusOK,
	PostInstance:      http.StatusCreated,
	GetInstance:       http.StatusOK,
	PatchInstance:     http.StatusOK,
	DeleteInstance:    http.StatusNoContent,
	HeadInstance:      http.StatusOK,
	OptionsInstance:   http.StatusOK,
}

// CrudlActions is a helper function to create standard resource actions for a given Instance[I] type.
func CrudlActions[I resource.Instance[I]]() map[any]func() resource.AnyAction {
	return map[any]func() resource.AnyAction{
		PostCollection: func() resource.AnyAction { return resource.CreateAction[I]() },
		GetInstance:    func() resource.AnyAction { return resource.ReadAction[I]() },
		PatchInstance:  func() resource.AnyAction { return resource.UpdateAction[I]() },
		DeleteInstance: func() resource.AnyAction { return resource.DeleteAction[I]() },
		GetCollection:  func() resource.AnyAction { return resource.ListAction[I]() },
	}
}

// NoActions is an empty action map for resources that don't expose REST endpoints.
var NoActions = map[any]func() resource.AnyAction{}

func BuildRoutes(registry *resource.Registry, middleware ...func(http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	for _, rt := range registry.RootTypes() {
		buildRoutes(mux, rt, "", nil)
	}

	// Register catchall handler for unmatched routes
	mux.Handle("/", noRouteHandler())

	return withMiddleware(registry, mux, nil, middleware)
}

// noRouteHandler returns a handler for requests that don't match any registered route.
func noRouteHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, ok := w.(*ResponseWriter)
		if !ok {
			rw = &ResponseWriter{}
			defer rw.writeTo(w)
		}

		rw.WriteError(gomerr.NotFound("Route", r.Method+" "+r.URL.Path))
	})
}

func buildRoutes(mux *http.ServeMux, rt resource.RegisteredType, parentPath string, ancestors []ancestorContext) {
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
		println("new route:", pattern)
		mux.Handle(pattern, handler(rt, actionFunc, successStatus))
	}

	// Prepend this resource's context to ancestors for children (closest ancestor first)
	childAncestors := append([]ancestorContext{{
		typeName: rt.InstanceName(),
		pathName: instancePathName,
	}}, ancestors...)
	for _, childMetadata := range rt.Children() {
		buildRoutes(mux, childMetadata, path[resource.InstanceCategory], childAncestors)
	}
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

func handler(rt resource.RegisteredType, actionFunc func() resource.AnyAction, successStatus int) http.Handler {
	anyAction := actionFunc()
	if anyAction == nil {
		panic(gomerr.Configuration("cannot handle a nil action").String())
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, ok := w.(*ResponseWriter)
		if !ok {
			rw = &ResponseWriter{}
			defer rw.writeTo(w)
			w = rw
		}

		// Bind request data to new instance
		res := rt.NewInstance(Subject(r))
		if ge := BindFromRequest(r, res, anyAction.Name()); ge != nil {
			rw.WriteError(ge)
			return
		}

		// If CollectionCategory, we use the bound instance as the prototype for its collection type
		if anyAction.AppliesToCategory() == resource.CollectionCategory {
			res = rt.NewCollection(res)
		}

		// Execute action via DoAction on the resource
		result, ge := anyAction.ExecuteOn(r.Context(), res)
		if ge != nil {
			rw.WriteError(ge)
			return
		}

		renderResult(result, rw, r, anyAction.Name(), successStatus)
	})
}

func renderResult(result any, w http.ResponseWriter, r *http.Request, scope string, statusCode int) {
	bytes, statusCode := BindToResponse(reflect.ValueOf(result), w.Header(), scope, r.Header.Get("Accept-Language"), statusCode)
	w.WriteHeader(statusCode)
	w.Write(bytes)
}
