package gomerr

import (
	"net/http"

	"github.com/jt0/gomer/util"
)

const (
	httpStatusLimitExceeded = 402
)

type ApplicationError struct {
	Gomerr
	StatusCode int
}

type BadRequestError struct {
	ApplicationError
}

type LimitExceededError struct {
	ApplicationError
	LimitType string
	Limit     util.Amount
	Current   util.Amount
	Attempted util.Amount
}

type ForbiddenError struct {
	ApplicationError
	Needed string
	Source string
}

type ResourceNotFoundError struct {
	ApplicationError
	ResourceType string
	Id           string
}

type MethodNotAllowedError struct {
	ApplicationError
	Method string
}

type ConflictError struct {
	ApplicationError
}

type InternalServerError struct {
	ApplicationError
}

func BadRequest(cause error) Gomerr {
	return BuildWithCause(cause, new(BadRequestError), http.StatusBadRequest)
}

func LimitExceeded(limitType string, limit util.Amount, current util.Amount, attempted util.Amount) Gomerr {
	return Build(new(LimitExceededError), httpStatusLimitExceeded, limitType, limit, current, attempted)
}

func Forbidden(needed string, source string) Gomerr {
	return Build(new(ForbiddenError), http.StatusForbidden, needed, source)
}

func ResourceNotFound(resourceType string, id string) Gomerr {
	return Build(new(ResourceNotFoundError), http.StatusNotFound, resourceType, id)
}

func MethodNotAllowed(method string) Gomerr {
	return Build(new(MethodNotAllowedError), http.StatusMethodNotAllowed, method)
}

func Conflict(cause error) Gomerr {
	return BuildWithCause(cause, new(ConflictError), http.StatusConflict)
}

func InternalServer(cause error) Gomerr {
	return BuildWithCause(cause, new(InternalServerError), http.StatusInternalServerError)
}

//func UnsupportedMediaType(receivedContentType string, supportedContentTypes []string) *ApplicationError {
//	details := make([]interface{}, 2)
//
//	details[0] = map[string]string{
//		"received": receivedContentType,
//	}
//
//	details[1] = map[string][]string{
//		"supported": supportedContentTypes,
//	}
//
//	return &ApplicationError{UnsupportedMediaTypeType, "Unsupported media type", details}
//}

//func ValidationFailure(err error) *ApplicationError {
//	// this check is only needed when your code could produce
//	// an invalid value for validation such as interface with nil
//	// value most including myself do not usually have code like this.
//	if _, ok := err.(*validator.InvalidValidationError); ok {
//		logs.Error.Print(err)
//
//		return InternalServerError("Error while validating input")
//	}
//
//	var details []interface{}
//
//	for _, err := range err.(validator.ValidationErrors) {
//		//fmt.Println(err.Namespace())
//		//fmt.Println(err.Field())
//		//fmt.Println(err.StructNamespace()) // can differ when a custom TagNameFunc is registered or
//		//fmt.Println(err.StructField())     // by passing alt name to ReportError like below
//		//fmt.Println(err.Tag())
//		//fmt.Println(err.ActualTag())
//		//fmt.Println(err.Kind())
//		//fmt.Println(err.Type())
//		//fmt.Println(err.Value())
//		//fmt.Println(err.Param())
//
//		if err.Tag() == "required" {
//			details = append(details, fmt.Sprintf("Validation error: missing %s attribute", err.Field()))
//		} else {
//			details = append(details, fmt.Sprintf("Validation error: %s %s %s", err.Field(), err.Value(), err.Tag()))
//		}
//	}
//
//	if len(details) == 0 {
//		return nil
//	}
//
//	return &ApplicationError{BadRequestType, "Validation errors in request.", details}
//}
