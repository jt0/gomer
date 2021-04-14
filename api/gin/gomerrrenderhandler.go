package gin

import (
	"reflect"

	"github.com/gin-gonic/gin"

	"github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/gomerr"
)

func GomerrRenderHandler(gomerrRenderer func(gomerr.Gomerr) http.StatusCoder) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if len(c.Errors) == 0 {
			return
		}

		lastErr := c.Errors.Last().Err
		ge, ok := lastErr.(gomerr.Gomerr)
		if !ok {
			ge = gomerr.Internal("Unexpected error type").Wrap(lastErr)
		}

		statusCoder := gomerrRenderer(ge)
		rv := reflect.ValueOf(statusCoder)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		if rge := renderResult(rv, c, "", statusCoder.StatusCode()); rge != nil {
			panic(rge)
		}
	}
}
