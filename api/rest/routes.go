package rest

import (
	"net/http"
	"reflect"
	"strings"

	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
)

// PathNamer can be implemented by resources to provide a custom path name.
// If PathName() returns a non-empty string, it will be used instead of
// the automatically derived name.
type PathNamer interface {
	PathName() string
}

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

func BuildRoutes(domain *resource.Domain, middleware ...func(http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	for _, md := range domain.RootResources() {
		buildRoutes(mux, md, "", nil)
	}

	return withMiddleware(domain, mux, nil, middleware)
}

func buildRoutes(mux *http.ServeMux, md *resource.Metadata, parentPath string, ancestors []ancestorContext) {
	// Determine the path name for this resource's instance type
	instanceTypeName := typeName(md.InstanceType())
	instancePathName := pathName(md.InstanceType(), ancestors)

	// Check if this resource has any collection-level actions
	actions := md.ActionFuncs()
	hasCollectionAction := false
	for key := range actions {
		if key.(Op).ResourceType() == resource.CollectionCategory {
			hasCollectionAction = true
			break
		}
	}

	path := make(map[resource.Category]string, 2)
	if hasCollectionAction {
		// Normal CRUD: collections are derived from instance type
		collectionPathName := instancePathName + "s" // Simple pluralization
		path[resource.CollectionCategory] = parentPath + "/" + strings.ToLower(collectionPathName)
		path[resource.InstanceCategory] = path[resource.CollectionCategory] + "/{" + instancePathName + "Id}"
	} else {
		// Singleton: use singular path without ID placeholder
		path[resource.InstanceCategory] = parentPath + "/" + strings.ToLower(instancePathName)
	}

	for key, actionFunc := range actions {
		op := key.(Op)

		// Determine which category this action applies to
		category := op.ResourceType()

		relativePath, ok := path[category]
		if !ok {
			panic("invalid resource type; does not map to a path: " + category)
		}

		successStatus, ok := successStatusCodes[op]
		if !ok {
			successStatus = http.StatusOK
		}

		// Register with method and path pattern
		pattern := op.Method() + " " + relativePath
		mux.Handle(pattern, handler(md, actionFunc, successStatus))
	}

	// Prepend this resource's context to ancestors for children (closest ancestor first)
	childAncestors := append([]ancestorContext{{
		typeName: instanceTypeName,
		pathName: instancePathName,
	}}, ancestors...)
	for _, childMetadata := range md.Children() {
		buildRoutes(mux, childMetadata, path[resource.InstanceCategory], childAncestors)
	}
}

// pathName derives a path name for a resource type, applying automatic trimming
// of redundant prefixes based on the ancestor chain, unless the resource implements
// PathNamer to provide an explicit override.
func pathName(resourceType reflect.Type, ancestors []ancestorContext) string {
	name := typeName(resourceType)

	// Check for explicit override via PathNamer interface
	if resourceType.Kind() == reflect.Ptr {
		if impl, ok := reflect.New(resourceType.Elem()).Interface().(PathNamer); ok {
			if override := impl.PathName(); override != "" {
				return override
			}
		}
	}

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

func typeName(t reflect.Type) string {
	s := t.String()
	dotIndex := strings.Index(s, ".")

	return s[dotIndex+1:]
}

func handler(md *resource.Metadata, actionFunc func() resource.AnyAction, successStatus int) http.Handler {
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
		res := md.NewInstance(Subject(r))
		if ge := BindFromRequest(r, res, anyAction.Name()); ge != nil {
			rw.WriteError(ge)
			return
		}

		// If CollectionCategory, we use the bound instance as the prototype for its collection type
		if anyAction.AppliesToCategory() == resource.CollectionCategory {
			res = md.NewCollection(res)
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
