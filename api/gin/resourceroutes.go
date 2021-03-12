package gin

import (
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"

	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/gomerr"
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

var crudlActions = map[interface{}]func() resource.Action{
	PostCollection: resource.CreateAction,
	GetInstance:    resource.ReadAction,
	PatchInstance:  resource.UpdateAction,
	DeleteInstance: resource.DeleteAction,
	GetCollection:  resource.ListAction,
}

func CrudlActions() map[interface{}]func() resource.Action {
	return crudlActions // returns a copy
}

var noActions = map[interface{}]func() resource.Action{}

func NoActions() map[interface{}]func() resource.Action {
	return noActions
}

type PreRender func(resource.Resource) gomerr.Gomerr

func BuildRoutes(r *gin.Engine, topLevelResources ...resource.Metadata) {
	for _, md := range topLevelResources {
		buildRoutes(r, md, "")
	}
}

func buildRoutes(r *gin.Engine, md resource.Metadata, parentPath string) {
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

		r.Handle(op.Method(), relativePath, handler(md.ResourceType(action().AppliesToCategory()), action, successStatus))
	}

	if collectionType != nil { // Cannot have resources other than instances under a collection
		for _, childMetadata := range md.Children() {
			buildRoutes(r, childMetadata, path[resource.InstanceCategory])
		}
	}
}

func variablePath(resourceType reflect.Type, path string) string {
	return path + "/:" + typeName(resourceType) + "Id"
}

func namedPath(resourceType reflect.Type, path string) string {
	return path + "/" + strings.ToLower(typeName(resourceType))
}

func typeName(t reflect.Type) string {
	s := t.String()
	dotIndex := strings.Index(s, ".")

	return s[dotIndex+1:]
}

func handler(resourceType reflect.Type, actionFunc func() resource.Action, successStatus int) func(c *gin.Context) {
	return func(c *gin.Context) {
		action := actionFunc()
		if inResource, ge := BindFromRequest(c.Request, resourceType, Subject(c), action.Name()); ge != nil {
			_ = c.Error(ge)
		} else if outResource, ge := inResource.DoAction(action); ge != nil {
			_ = c.Error(ge)
		} else if ge = BindToResponse(outResource, successStatus, c, action.Name()); ge != nil {
			_ = c.Error(ge)
		}
	}
}
