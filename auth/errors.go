package auth

import (
	"github.com/jt0/gomer/gomerr"
)

type AuthenticatedError struct {
	gomerr.Gomerr
	Reason string
}

func Unauthenticated(reason string) gomerr.Gomerr {
	return gomerr.Build(new(AuthenticatedError), reason).(*AuthenticatedError)
}

type AuthorizationError struct {
	gomerr.Gomerr
	Reason string
}

func Unauthorized(reason string) gomerr.Gomerr {
	return gomerr.Build(new(AuthorizationError), reason).(*AuthorizationError)
}
