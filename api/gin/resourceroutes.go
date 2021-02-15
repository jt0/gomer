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

var crudqActions = map[interface{}]func() resource.Action{
	PostCollection: resource.CreateAction,
	GetInstance:    resource.ReadAction,
	PatchInstance:  resource.UpdateAction,
	DeleteInstance: resource.DeleteAction,
	GetCollection:  resource.QueryAction,
}

func CrudqActions() map[interface{}]func() resource.Action {
	return crudqActions // returns a copy
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
	instanceType := md.ResourceType(resource.InstanceType)
	collectionType := md.ResourceType(resource.CollectionType)

	path := make(map[resource.Type]string, 2)
	if collectionType == nil {
		path[resource.InstanceType] = namedPath(instanceType, parentPath)
	} else {
		path[resource.CollectionType] = namedPath(collectionType, parentPath)
		path[resource.InstanceType] = variablePath(instanceType, path[resource.CollectionType])
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

		r.Handle(op.Method(), relativePath, handler(md.ResourceType(action().ResourceType()), action, successStatus))
	}

	if collectionType != nil { // Cannot have resources other than instances under a collection
		for _, childMetadata := range md.Children() {
			buildRoutes(r, childMetadata, path[resource.InstanceType])
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
		var ge gomerr.Gomerr
		defer func() {
			if ge != nil {
				_ = c.Error(ge)
			} else if r := recover(); r != nil {
				_ = c.Error(gomerr.Panic(r))
			}
		}()

		action := actionFunc()
		if inResource, ge := BindFromRequest(c.Request, resourceType, Subject(c)); ge != nil {
			_ = c.Error(ge)
		} else if outResource, ge := inResource.DoAction(action); ge != nil {
			_ = c.Error(ge)
		} else if ge = BindToResponse(outResource, successStatus, c); ge != nil {
			_ = c.Error(ge)
		}
	}
}
