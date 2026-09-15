package middleware

import (
	"net/http"

	"github.com/jt0/gomer/resource"
)

type ApiContext struct {
	Instance    resource.AnyInstance
	Action      resource.AnyAction
	SuccessCode int
	Target      any
}

// ApiContextFor returns the resource.AnyInstance from the provided request. If the
// BuildApiContext middleware isn't used or hasn't yet been reached, this function
// returns nil.
var ApiContextFor = func(*http.Request) *ApiContext {
	return nil
}
