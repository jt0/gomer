package gin

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/util"
)

type GomerrErrorRenderer func(gomerr.Gomerr) (statusCode int, responsePayload interface{})

func GomerrRenderHandler(errorRenderer GomerrErrorRenderer) gin.HandlerFunc {
	if errorRenderer == nil {
		println("[Warning] Using default error renderer - do not use in production!!")
		errorRenderer = defaultErrorRenderer
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

func defaultErrorRenderer(ge gomerr.Gomerr) (statusCode int, responsePayload interface{}) {
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
