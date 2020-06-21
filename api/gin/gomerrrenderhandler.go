package gin

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/util"
)

type GomerrRenderer func(gomerr.Gomerr) (statusCode int, responsePayload interface{})
type GomerrBatchRenderer func(batchError gomerr.BatchError) (statusCode int, responsePayload interface{})

func GomerrRenderHandler(errorRenderer GomerrRenderer, batchRenderer GomerrBatchRenderer) gin.HandlerFunc {
	if errorRenderer == nil {
		println("[Warning] Using default GomerrRenderer - do not use in production!!")
		errorRenderer = defaultGomerrRenderer
	}

	if batchRenderer == nil {
		println("[Warning] Using default GomerrBatchRenderer - do not use in production!!")
		batchRenderer = defaultGomerrBatchRenderer
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

		lastErr := c.Errors.Last().Err
		if bge, ok := lastErr.(gomerr.BatchError); ok {
			c.IndentedJSON(batchRenderer(bge))
		} else if ge, ok := lastErr.(gomerr.Gomerr); ok {
			c.IndentedJSON(errorRenderer(ge))
		} else {
			c.IndentedJSON(errorRenderer(gomerr.InternalServer(lastErr).AddNotes("wrapping non-gomerr error at render time")))
		}
	}
}

const (
	// No predefined HTTP code for LimitExceeded, but this appears to be growing in adoption
	HttpStatusLimitExceeded = 402
)

var GomerrToStatusCode = map[string]int{
	util.UnqualifiedTypeName(gomerr.BadValueError{}):      http.StatusBadRequest,
	util.UnqualifiedTypeName(gomerr.LimitExceededError{}): HttpStatusLimitExceeded,
	util.UnqualifiedTypeName(gomerr.NotFoundError{}):      http.StatusNotFound,

	// XXX: finish populating..
}

func defaultGomerrRenderer(ge gomerr.Gomerr) (statusCode int, responsePayload interface{}) {
	if sc, ok := GomerrToStatusCode[util.UnqualifiedTypeName(ge)]; ok {
		// TODO: logic to verify 4xx/5xx based on culprit...
		statusCode = sc
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

func defaultGomerrBatchRenderer(bge gomerr.BatchError) (statusCode int, responsePayload interface{}) {
	// XXX
	return 0, nil
}
