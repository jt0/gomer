package http

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

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

func buildApi(r *gin.Engine, md resource.Metadata, path string, pathKeys []string) {
	resourceType := md.InstanceName()

	if md.CollectionQueryName() != "" {
		path = path + "/" + md.CollectionQueryName()

		// FIXME: Examine metadata to determine if create/get are supported
		r.POST(path, instanceHandler(resourceType, md.ExternalNameToFieldName, resource.DoCreate, true, http.StatusCreated))
		r.GET(path, collectionQueryHandler(resourceType))
	}

	// FIXME: support singleton (non-query) types that don't have an identifier
	pathKey := resourceType + "Id"
	path = path + "/:" + pathKey
	pathKeys = append(pathKeys, pathKey)

	// TODO: Examine metadata to determine if put/get/patch/delete are supported and if special handlers are needed
	r.GET(path, instanceHandler(resourceType, md.ExternalNameToFieldName, resource.DoGet, false, http.StatusOK))
	r.PATCH(path, instanceHandler(resourceType, md.ExternalNameToFieldName, resource.DoUpdate, true, http.StatusOK))
	r.DELETE(path, instanceHandler(resourceType, md.ExternalNameToFieldName, resource.DoDelete, false, http.StatusNoContent))

	for _, childMetadata := range md.Children() {
		buildApi(r, childMetadata, path, pathKeys)
	}
}

type instanceAction func(i resource.Instance) (result interface{}, ae *gomerr.ApplicationError)

func instanceHandler(resourceType string, externalToFieldName func(string) (string, bool), doAction instanceAction, readBody bool, successStatus int) func(c *gin.Context) {
	return func(c *gin.Context) {
		instance, ae := resource.UnmarshalInstance(resourceType, subject(c), getBytes(c, externalToFieldName, readBody))
		if ae != nil {
			errorResponse(c, ae)

			return
		}

		if result, ae := doAction(instance); ae != nil {
			errorResponse(c, ae)
		} else {
			successResponse(c, successStatus, result)
		}
	}
}

func collectionQueryHandler(resourceType string) func(c *gin.Context) {
	return func(c *gin.Context) {
		collectionQuery, ae := resource.UnmarshalCollectionQuery(resourceType, subject(c), getBytes(c, s, false))
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

func getBytes(c *gin.Context, externalToFieldName func(string) (string, bool), readBody bool) []byte {
	var jsonMap map[string]interface{}

	var bytes []byte
	if readBody {
		bytes, _ = ioutil.ReadAll(c.Request.Body)
	}

	query := c.Request.URL.Query()

	if len(bytes) == 0 {
		jsonMap = make(map[string]interface{}, len(c.Params)+len(query))
	} else if err := json.Unmarshal(bytes, &jsonMap); err != nil {
		logs.Error.Println("Unmarshal error: " + err.Error())
		// TODO: return 400
	}

	for key, value := range query {
		if fieldName, ok := externalToFieldName(key); ok && len(value) > 0 {
			jsonMap[fieldName] = value[0]
		} // TODO: arrays or key-only query params
	}

	for _, param := range c.Params {
		jsonMap[param.Key] = param.Value
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

func s(s string) (string, bool) {
	return s, true
}
