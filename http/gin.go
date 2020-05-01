package http

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/gin-gonic/gin"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/logs"
	"github.com/jt0/gomer/resource"
)

type SubjectProvider func(*gin.Context) (auth.Subject, *gomerr.ApplicationError)
type SubjectRelease func(auth.Subject)

func GinEngine(rootResources []resource.Metadata, subjectProvider SubjectProvider) *gin.Engine {
	r := gin.Default()

	r.Use(subjectHandler(subjectProvider))

	for _, rootResource := range rootResources {
		buildApi(r, rootResource, "", []string{})
	}

	return r
}

func subjectHandler(subjectProvider SubjectProvider) func(c *gin.Context) {
	return func(c *gin.Context) {
		if subject, ae := subjectProvider(c); ae != nil {
			c.Abort()
			errorResponse(c, ae)
		} else {
			c.Set("subject", subject)
			c.Next()
			subject.Release()
		}
	}
}

func subject(c *gin.Context) auth.Subject {
	return c.MustGet("subject").(auth.Subject)
}

func buildApi(r *gin.Engine, resourceMetadata resource.Metadata, path string, pathKeys []string) {
	resourceType := resourceMetadata.InstanceName()

	if resourceMetadata.CollectionQueryName() != "" {
		path = path + "/" + resourceMetadata.CollectionQueryName()

		// FIXME: Examine metadata to determine if create/get are supported
		r.POST(path, createHandler(resourceType, pathKeys))
		r.GET(path, queryHandler(resourceType, pathKeys))
	}

	// FIXME: support singleton (non-query) types that don't have an identifier
	pathKey := resourceType + "Id"
	path = path + "/:" + pathKey
	pathKeys = append(pathKeys, pathKey)

	// TODO: Examine metadata to determine if put/get/patch/delete are supported and if special handlers are needed
	r.GET(path, getHandler(resourceType, pathKeys))
	r.PATCH(path, patchHandler(resourceType, pathKeys))
	r.DELETE(path, deleteHandler(resourceType, pathKeys))

	for _, childMetadata := range resourceMetadata.Children() {
		buildApi(r, childMetadata, path, pathKeys)
	}
}

func createHandler(resourceType string, pathKeys []string) func(c *gin.Context) {
	return func(c *gin.Context) {
		bytes := getBytes(c, rawData(c), pathKeys)

		instance, ae := resource.UnmarshalInstance(resourceType, subject(c), bytes)
		if ae != nil {
			errorResponse(c, ae)

			return
		}

		if result, ae := resource.DoCreate(instance); ae != nil {
			errorResponse(c, ae)
		} else {
			successResponse(c, http.StatusCreated, result)
		}
	}
}

func getHandler(resourceType string, pathKeys []string) func(c *gin.Context) {
	return func(c *gin.Context) {
		bytes := getBytes(c, nil, pathKeys)

		instance, ae := resource.UnmarshalInstance(resourceType, subject(c), bytes)
		if ae != nil {
			errorResponse(c, ae)

			return
		}

		if result, ae := resource.DoGet(instance); ae != nil {
			errorResponse(c, ae)
		} else {
			successResponse(c, http.StatusOK, result)
		}
	}
}

func patchHandler(resourceType string, pathKeys []string) func(c *gin.Context) {
	return func(c *gin.Context) {
		if c.ContentType() != "application/json-patch+json" {
			errorResponse(c, gomerr.UnsupportedMediaType(c.ContentType(), []string{"application/json-patch+json"}))

			return
		}
		patchBytes, _ := c.GetRawData()

		var patch jsonpatch.Patch
		if err := json.Unmarshal(patchBytes, &patch); err != nil {
			logs.Error.Println("Unmarshal error: " + err.Error())

			errorResponse(c, gomerr.BadRequest("Unable to create patch instance."))
		}

		bytes := getBytes(c, nil, pathKeys)

		instance, ae := resource.UnmarshalInstance(resourceType, subject(c), bytes)
		if ae != nil {
			errorResponse(c, ae)

			return
		}

		if result, ae := resource.DoPatch(instance, patch); ae != nil {
			errorResponse(c, ae)
		} else {
			successResponse(c, http.StatusOK, result)
		}
	}
}

func deleteHandler(resourceType string, pathKeys []string) func(c *gin.Context) {
	return func(c *gin.Context) {
		bytes := getBytes(c, nil, pathKeys)

		instance, ae := resource.UnmarshalInstance(resourceType, subject(c), bytes)
		if ae != nil {
			errorResponse(c, ae)

			return
		}

		if result, ae := resource.DoDelete(instance); ae != nil {
			errorResponse(c, ae)
		} else {
			successResponse(c, http.StatusNoContent, result)
		}
	}
}

func queryHandler(resourceType string, pathKeys []string) func(c *gin.Context) {
	return func(c *gin.Context) {
		bytes := getBytes(c, nil, pathKeys)

		collectionQuery, ae := resource.UnmarshalCollectionQuery(resourceType, subject(c), bytes)
		if ae != nil {
			errorResponse(c, ae)

			return
		}

		if result, ae := resource.DoQuery(collectionQuery); ae != nil {
			errorResponse(c, ae)
		} else {
			successResponse(c, http.StatusOK, result)
		}
	}
}

func rawData(c *gin.Context) []byte {
	bytes, _ := ioutil.ReadAll(c.Request.Body)

	return bytes
}

func getBytes(c *gin.Context, bytes []byte, pathKeys []string) []byte {
	query := c.Request.URL.Query()
	if len(pathKeys) == 0 && len(query) == 0 {
		if len(bytes) == 0 {
			return []byte("{}")
		} else {
			return bytes
		}
	}

	var jsonMap map[string]interface{}

	if len(bytes) == 0 {
		jsonMap = make(map[string]interface{}, len(pathKeys)+len(query))
	} else if err := json.Unmarshal(bytes, &jsonMap); err != nil {
		logs.Error.Println("Unmarshal error: " + err.Error())
		// TODO: return 400
	}

	for _, key := range pathKeys {
		if value := c.Param(key); value != "" {
			jsonMap[key] = value
		}
	}

	for key, value := range query {
		if len(value) > 0 {
			jsonMap[key] = value[0]
		} // TODO: arrays or key-only query params
	}

	bytes, _ = json.Marshal(jsonMap)

	return bytes
}

func errorResponse(c *gin.Context, ae *gomerr.ApplicationError) {
	c.IndentedJSON(ae.StatusCode(), ae)
}

func successResponse(c *gin.Context, httpStatus int, result interface{}) {
	c.IndentedJSON(httpStatus, result)
}
