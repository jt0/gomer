package gin

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"

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
	PostCollection: resource.CreateInstanceAction,
	GetInstance:    resource.ReadInstanceAction,
	PatchInstance:  resource.UpdateInstanceAction,
	DeleteInstance: resource.DeleteInstanceAction,
	GetCollection:  resource.QueryCollectionAction,
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

		r.Handle(op.Method(), relativePath, handler(md.ResourceType(action().ResourceType()), action, md.ExternalNameToFieldName, successStatus, resource.ReadableData))
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

func handler(resourceType reflect.Type, action func() resource.Action, externalToFieldName func(string) (string, bool), successStatus int, preRender PreRender) func(c *gin.Context) {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				_ = c.Error(gomerr.Panic(r))
			}
		}()

		bytes, ge := getBytes(c, externalToFieldName)
		if ge != nil {
			_ = c.Error(ge)
			return
		}

		resource_, ge := resource.Unmarshal(resourceType, Subject(c), bytes)
		if ge != nil {
			_ = c.Error(ge)
			return
		}

		if resource_, ge = resource.DoAction(resource_, action()); ge != nil {
			_ = c.Error(ge)
		} else {
			if ge := preRender(resource_); ge != nil {
				_ = c.Error(ge)
			} else {
				c.Render(successStatus, render.IndentedJSON{Data: resource_})
			}
		}
	}
}

func getBytes(c *gin.Context, externalToFieldName func(string) (string, bool)) ([]byte, gomerr.Gomerr) {
	var jsonMap map[string]interface{}
	bodyBytes, _ := ioutil.ReadAll(c.Request.Body)
	queryParams := c.Request.URL.Query()

	if len(bodyBytes) == 0 {
		jsonMap = make(map[string]interface{}, len(c.Params)+len(queryParams))
	} else if err := json.Unmarshal(bodyBytes, &jsonMap); err != nil {
		return nil, gomerr.Unmarshal("Request body", bodyBytes, jsonMap)
	}

	for key, value := range queryParams {
		// TODO: decide if body values should be overwritten by query params
		// Test if the query param will apply to some field in the targeted type and only apply if yes
		if _, ok := externalToFieldName(key); ok && len(value) > 0 {
			jsonMap[key] = value[0]
		} // TODO: arrays or key-only query params
	}

	for _, param := range c.Params {
		jsonMap[param.Key] = param.Value
	}

	bodyBytes, _ = json.Marshal(jsonMap)

	return bodyBytes, nil
}
