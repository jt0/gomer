package rest

import (
	"net/http"
	"reflect"
	"strings"

	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/resource"
)

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

var CRUDL = map[any]func() resource.Action{
	PostCollection: resource.CreateAction,
	GetInstance:    resource.ReadAction,
	PatchInstance:  resource.UpdateAction,
	DeleteInstance: resource.DeleteAction,
	GetCollection:  resource.ListAction,
}

var NoActions = map[any]func() resource.Action{}

func BuildRoutes(domain *resource.Domain, middleware ...func(http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	for _, md := range domain.RootResources() {
		buildRoutes(mux, md, "")
	}

	// // Add gomerr renderer if provided
	// if gomerrRenderer != nil {
	// 	rw.errRenderers = []func(w http.ResponseWriter, err error) bool{
	// 		gomerrErrRenderer(gomerrRenderer, r),
	// 	}
	// }

	return withMiddleware(mux, nil, middleware)
}

func buildRoutes(mux *http.ServeMux, md *resource.Metadata, parentPath string) {
	instanceType := md.ResourceType(resource.InstanceCategory)
	collectionType := md.ResourceType(resource.CollectionCategory)

	path := make(map[resource.Category]string, 2)
	if collectionType == nil {
		path[resource.InstanceCategory] = namedPath(instanceType, parentPath)
	} else {
		path[resource.CollectionCategory] = namedPath(collectionType, parentPath)
		path[resource.InstanceCategory] = variablePath(instanceType, path[resource.CollectionCategory])
	}

	for key, action := range md.Actions() {
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
		mux.Handle(pattern, handler(md.ResourceType(action().AppliesToCategory()), action, successStatus))
	}

	if collectionType != nil { // Cannot have resources other than instances under a collection
		for _, childMetadata := range md.Children() {
			buildRoutes(mux, childMetadata, path[resource.InstanceCategory])
		}
	}
}

func variablePath(resourceType reflect.Type, path string) string {
	return path + "/{" + typeName(resourceType) + "Id}"
}

func namedPath(resourceType reflect.Type, path string) string {
	return path + "/" + strings.ToLower(typeName(resourceType))
}

func typeName(t reflect.Type) string {
	s := t.String()
	dotIndex := strings.Index(s, ".")

	return s[dotIndex+1:]
}

func handler(resourceType reflect.Type, actionFunc func() resource.Action, successStatus int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw, ok := w.(*ResponseWriter)
		if !ok {
			// If not wrapped with ResponseWriter, create one (shouldn't happen in normal flow)
			rw = &ResponseWriter{}
			defer rw.writeTo(w)
			w = rw
		}

		action := actionFunc()
		res, ge := BindFromRequest(r, resourceType, Subject(r), action.Name())
		if ge != nil {
			rw.WriteError(ge)
			return
		}

		res, ge = res.DoAction(r.Context(), action)
		if ge != nil {
			rw.WriteError(ge)
			return
		}

		renderResult(res, rw, r, action.Name(), successStatus)
	})
}

func renderResult(result any, w http.ResponseWriter, r *http.Request, scope string, statusCode int) {
	bytes, statusCode := BindToResponse(reflect.ValueOf(result), w.Header(), scope, r.Header.Get("Accept-Language"), statusCode)
	w.WriteHeader(statusCode)
	w.Write(bytes)
}
