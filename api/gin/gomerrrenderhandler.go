package gin

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/limit"
	"github.com/jt0/gomer/util"
)

type GomerrRenderer func(gomerr.Gomerr, *gin.Context) (statusCode int, responsePayload interface{})
type GomerrBatchRenderer func(gomerr.BatchError, *gin.Context) (statusCode int, responsePayload interface{})

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
			c.IndentedJSON(batchRenderer(bge, c))
		} else if ge, ok := lastErr.(gomerr.Gomerr); ok {
			c.IndentedJSON(errorRenderer(ge, c))
		} else {
			c.IndentedJSON(errorRenderer(gomerr.Internal("Unexpected error type").Wrap(lastErr), c))
		}
	}
}

const (
	// No predefined HTTP code for Exceeded, but this appears to be growing in adoption
	HttpStatusLimitExceeded = 402
)

var GomerrToStatusCode = map[string]int{
	util.UnqualifiedTypeName(gomerr.UnprocessableError{}): http.StatusBadRequest,
	util.UnqualifiedTypeName(limit.ExceededError{}):       HttpStatusLimitExceeded,
	util.UnqualifiedTypeName(gomerr.NotFoundError{}):      http.StatusNotFound,

}

func defaultGomerrRenderer(ge gomerr.Gomerr, _ *gin.Context) (statusCode int, responsePayload interface{}) {
	if sc, ok := GomerrToStatusCode[util.UnqualifiedTypeName(ge)]; ok {
		// TODO: logic to verify 4xx/5xx based on culprit...
		statusCode = sc
	} else {
		statusCode = http.StatusInternalServerError
	}

	return statusCode, ge.ToMap()
}

func defaultGomerrBatchRenderer(bge gomerr.BatchError, _ *gin.Context) (statusCode int, responsePayload interface{}) {
	// TODO:p1 Basic impl
	return 0, nil
}
