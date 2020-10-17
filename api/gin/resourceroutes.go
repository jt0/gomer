package gin

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
)

const (
	PutCollection    = "PutCollection"
	PostCollection   = "PostCollection"
	GetCollection    = "GetCollection"
	PatchCollection  = "PatchCollection"
	DeleteCollection = "DeleteCollection"

	PutInstance    = "PutInstance"
	PostInstance   = "PostInstance"
	GetInstance    = "GetInstance"
	PatchInstance  = "PatchInstance"
	DeleteInstance = "DeleteInstance"
)

type HttpSpec struct {
	Method            string
	SuccessStatusCode int
}

var unqualifiedOps = map[string]HttpSpec{
	PutCollection:    {"PUT", http.StatusAccepted},
	PostCollection:   {"POST", http.StatusCreated},
	GetCollection:    {"GET", http.StatusOK},
	PatchCollection:  {"PATCH", http.StatusOK},
	DeleteCollection: {"DELETE", http.StatusAccepted},
}

var qualifiedOps = map[string]HttpSpec{
	PutInstance:    {"PUT", http.StatusOK},
	PostInstance:   {"POST", http.StatusCreated},
	GetInstance:    {"GET", http.StatusOK},
	PatchInstance:  {"PATCH", http.StatusOK},
	DeleteInstance: {"DELETE", http.StatusNoContent},
}

var defaultInstanceActions = resource.InstanceActions{
	PostCollection: resource.CreateInstance,
	GetInstance:    resource.ReadInstance,
	PatchInstance:  resource.UpdateInstance,
	DeleteInstance: resource.DeleteInstance,
}

func CrudActions() resource.InstanceActions {
	return defaultInstanceActions
}

func BuildRoutes(r *gin.Engine, topLevelResources ...resource.Metadata) {
	for _, md := range topLevelResources {
		buildRoutes(r, md, "")
	}
}

func buildRoutes(r *gin.Engine, md resource.Metadata, path string) {
	resourceType := md.InstanceName()

	if md.CollectionQueryName() != "" {
		path = collectionPathFor(md.CollectionQueryName(), path)

		if action, ok := md.InstanceActions()[PostCollection]; ok {
			r.POST(path, instanceHandler(resourceType, action, md.ExternalNameToFieldName, true, http.StatusCreated))
		}

		r.GET(path, collectionQueryHandler(resourceType))

		path = instancePathFor(resourceType, path)
	} else {
		path = singletonPathFor(resourceType, path)
	}

	for k, op := range qualifiedOps {
		if action, ok := md.InstanceActions()[k]; ok {
			r.Handle(op.Method, path, instanceHandler(resourceType, action, md.ExternalNameToFieldName, true, op.SuccessStatusCode))
		}
	}

	if md.CollectionQueryName() != "" { // Cannot have resources other than instances under a collection
		for _, childMetadata := range md.Children() {
			buildRoutes(r, childMetadata, path)
		}
	}
}

func collectionPathFor(resourceName, path string) string {
	lowerCaseCollectionQueryName := strings.ToLower(resourceName)

	if strings.HasSuffix(lowerCaseCollectionQueryName, "collectionquery") {
		return path + "/" + strings.TrimSuffix(lowerCaseCollectionQueryName, "collectionquery")
	} else if strings.HasSuffix(lowerCaseCollectionQueryName, "query") {
		return path + "/" + strings.TrimSuffix(lowerCaseCollectionQueryName, "query")
	} else {
		return path + "/" + lowerCaseCollectionQueryName
	}
}

func instancePathFor(resourceName string, path string) string {
	return path + "/:" + resourceName + "Id"
}

func singletonPathFor(resourceName string, path string) string {
	return path + "/" + strings.ToLower(resourceName)
}

func instanceHandler(resourceType string, action func() resource.InstanceAction, externalToFieldName func(string) (string, bool), readBody bool, successStatus int) func(c *gin.Context) {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				_ = c.Error(gomerr.Panic(r))
			}
		}()

		bytes, ge := getBytes(c, externalToFieldName, readBody)
		if ge != nil {
			_ = c.Error(ge)
			return
		}

		instance, ge := resource.UnmarshalInstance(resourceType, Subject(c), bytes)
		if ge != nil {
			_ = c.Error(ge)
			return
		}

		if result, ge := resource.DoInstanceAction(instance, action()); ge != nil {
			_ = c.Error(ge)
		} else {
			c.IndentedJSON(successStatus, result)
		}
	}
}

func collectionQueryHandler(resourceType string /*, action resource.CollectionAction */) func(c *gin.Context) {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				_ = c.Error(gomerr.Panic(r))
			}
		}()

		bytes, ge := getBytes(c, s, false)
		if ge != nil {
			_ = c.Error(ge)
			return
		}

		collectionQuery, ge := resource.UnmarshalCollectionQuery(resourceType, Subject(c), bytes)
		if ge != nil {
			_ = c.Error(ge)
			return
		}

		if result, ge := resource.DoQuery(collectionQuery); ge != nil {
			_ = c.Error(ge)
		} else {
			c.IndentedJSON(http.StatusOK, result)
		}
	}
}

func getBytes(c *gin.Context, externalToFieldName func(string) (string, bool), readBody bool) ([]byte, gomerr.Gomerr) {
	var jsonMap map[string]interface{}

	var bytes []byte
	if readBody {
		bytes, _ = ioutil.ReadAll(c.Request.Body)
	}

	queryParams := c.Request.URL.Query()

	if len(bytes) == 0 {
		jsonMap = make(map[string]interface{}, len(c.Params)+len(queryParams))
	} else if err := json.Unmarshal(bytes, &jsonMap); err != nil {
		return nil, gomerr.Unmarshal("Request body", bytes, jsonMap)
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

	bytes, _ = json.Marshal(jsonMap)

	return bytes, nil
}

func s(s string) (string, bool) {
	return s, true
}
