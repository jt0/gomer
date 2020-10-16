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

func BuildRoutes(r *gin.Engine, resourceMetadata ...resource.Metadata) {
	for _, md := range resourceMetadata {
		buildRoutes(r, md, "")
	}
}

func buildRoutes(r *gin.Engine, md resource.Metadata, path string) {
	resourceType := md.InstanceName()

	if md.CollectionQueryName() != "" {
		lowerCaseCollectionQueryName := strings.ToLower(md.CollectionQueryName())
		var collectionName string
		if strings.HasSuffix(lowerCaseCollectionQueryName, "collectionquery") {
			collectionName = strings.TrimSuffix(lowerCaseCollectionQueryName, "collectionquery")
		} else if strings.HasSuffix(lowerCaseCollectionQueryName, "query") {
			collectionName = strings.TrimSuffix(lowerCaseCollectionQueryName, "query")
		} else {
			collectionName = lowerCaseCollectionQueryName
		}
		path = path + "/" + collectionName

		// FIXME: Examine metadata to determine if create/get are supported
		r.POST(path, instanceHandler(resourceType, md.ExternalNameToFieldName, resource.DoCreate, true, http.StatusCreated))
		r.GET(path, collectionQueryHandler(resourceType))
	}

	// FIXME: support singleton (non-query) types that don't have an identifier
	pathKey := resourceType + "Id"
	path = path + "/:" + pathKey

	// TODO: Examine metadata to determine if put/get/patch/delete are supported and if special handlers are needed
	r.GET(path, instanceHandler(resourceType, md.ExternalNameToFieldName, resource.DoGet, false, http.StatusOK))
	r.PATCH(path, instanceHandler(resourceType, md.ExternalNameToFieldName, resource.DoUpdate, true, http.StatusOK))
	r.DELETE(path, instanceHandler(resourceType, md.ExternalNameToFieldName, resource.DoDelete, false, http.StatusNoContent))

	for _, childMetadata := range md.Children() {
		buildRoutes(r, childMetadata, path)
	}
}

type instanceAction func(i resource.Instance) (result interface{}, ge gomerr.Gomerr)

func instanceHandler(resourceType string, externalToFieldName func(string) (string, bool), doAction instanceAction, readBody bool, successStatus int) func(c *gin.Context) {
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

		if result, ge := doAction(instance); ge != nil {
			_ = c.Error(ge)
		} else {
			c.IndentedJSON(successStatus, result)
		}
	}
}

func collectionQueryHandler(resourceType string) func(c *gin.Context) {
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
