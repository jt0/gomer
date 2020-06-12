package http

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
	"github.com/jt0/gomer/util"
)

type ErrorRenderer func(gomerr.Gomerr) (statusCode int, responsePayload interface{})
type GinContextSubjectProvider func(*gin.Context) (auth.Subject, gomerr.Gomerr)

func GinEngine(rootResources []resource.Metadata, errorRenderer ErrorRenderer, subjectProvider GinContextSubjectProvider) (*gin.Engine, gomerr.Gomerr) {
	r := gin.Default()

	r.Use(errorHandler(errorRenderer), subjectHandler(subjectProvider))

	r.NoMethod() // Make sure to write output to avoid gin providing default text
	r.NoRoute()  // Make sure to write output to avoid gin providing default text

	for _, rootResource := range rootResources {
		buildApi(r, rootResource, "", []string{})
	}

	return r, nil
}

func errorHandler(errorRenderer ErrorRenderer) func(c *gin.Context) {
	if errorRenderer == nil {
		println("[Warning] Using default error renderer - do not use in production!!")
		errorRenderer = DefaultErrorRenderer
	}

	return func(c *gin.Context) {
		c.Next()

		if len(c.Errors) == 0 {
			return
		}

		if c.Writer.Written() {
			println(c.Errors.String())
			return
		}

		if len(c.Errors) > 1 {
			println(c.Errors.String())
		}

		ge, ok := c.Errors.Last().Err.(gomerr.Gomerr)
		if !ok {
			ge = gomerr.InternalServer(c.Errors.Last().Err)
		}

		c.IndentedJSON(errorRenderer(ge))
	}
}

func DefaultErrorRenderer(ge gomerr.Gomerr) (statusCode int, responsePayload interface{}) {
	if ae, ok := ge.(gomerr.ApplicationError); ok {
		statusCode = ae.StatusCode
	} else if ge.Culprit() == gomerr.Client {
		statusCode = http.StatusBadRequest
	} else {
		statusCode = http.StatusInternalServerError
	}

	errorDetails := make(map[string]interface{})
	responsePayload = errorDetails
	for {
		attributes := ge.Attributes()
		errorDetails[util.UnqualifiedTypeName(ge)] = attributes
		attributes["_Location"] = ge.Location()
		if ge.Culprit() != gomerr.Unspecified {
			attributes["_Culprit"] = ge.Culprit()
		}
		if len(ge.Notes()) > 0 {
			attributes["_Notes"] = ge.Notes()
		}

		err := ge.Cause()
		if err == nil {
			return
		}

		causeDetails := make(map[string]interface{})
		attributes["_Cause"] = causeDetails
		var ok bool
		ge, ok = err.(gomerr.Gomerr)
		if ok {
			errorDetails = causeDetails
		} else {
			causeDetails[util.UnqualifiedTypeName(err)] = err
			return
		}
	}
}

func subjectHandler(subjectProvider GinContextSubjectProvider) func(c *gin.Context) {
	if subjectProvider == nil {
		// TODO: provide default subjectProvider that at least includes resource.ReadWriteAll
	}

	return func(c *gin.Context) {
		if subject, ge := subjectProvider(c); ge != nil {
			_ = c.Error(ge)
			c.Abort()
		} else {
			c.Set("subject", subject)
			c.Next()
			ge := subject.Release(c.IsAborted() || len(c.Errors) > 0)
			if ge != nil {
				// TODO: log but don't error
			}
		}
	}
}

func subject(c *gin.Context) auth.Subject {
	return c.MustGet("subject").(auth.Subject)
}

func buildApi(r *gin.Engine, md resource.Metadata, path string, pathKeys []string) {
	resourceType := md.InstanceName()

	if md.CollectionQueryName() != "" {
		var collectionName string
		if strings.HasSuffix(md.CollectionQueryName(), "collectionquery") {
			collectionName = strings.TrimSuffix(md.CollectionQueryName(), "collectionquery")
		} else if strings.HasSuffix(md.CollectionQueryName(), "query") {
			collectionName = strings.TrimSuffix(md.CollectionQueryName(), "query")
		} else {
			collectionName = md.CollectionQueryName()
		}
		path = path + "/" + collectionName

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

		instance, ge := resource.UnmarshalInstance(resourceType, subject(c), bytes)
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

		collectionQuery, ge := resource.UnmarshalCollectionQuery(resourceType, subject(c), bytes)
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
		return nil, gomerr.Unmarshal(err, bytes, jsonMap)
	}

	for key, value := range queryParams {
		if fieldName, ok := externalToFieldName(key); ok && len(value) > 0 {
			jsonMap[fieldName] = value[0]
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
