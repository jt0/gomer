package gomerr

import (
	"fmt"
	"strings"

	"gopkg.in/go-playground/validator.v9"

	"github.com/jt0/gomer/logs"
)

type applicationErrorType string

const (
	BadRequestType           applicationErrorType = "BadRequest"
	LimitExceededType        applicationErrorType = "LimitExceeded"
	ForbiddenType            applicationErrorType = "Forbidden"
	ResourceNotFoundType     applicationErrorType = "ResourceNotFound"
	MethodNotAllowedType     applicationErrorType = "MethodNotAllowed"
	ConflictExceptionType    applicationErrorType = "ConflictException"
	UnsupportedMediaTypeType applicationErrorType = "UnsupportedMediaTypeType"
	InternalServerErrorType  applicationErrorType = "InternalServerError"
	//NotImplementedErrorType  applicationErrorType = "NotImplementedError"
)

// TODO: Add a 'code' attribute that provides set values (and is used to provide the message)
type ApplicationError struct {
	ErrorType applicationErrorType `json:"errorType,omitempty"`
	Message   string               `json:"message"`
	Details   []interface{}        `json:"details,omitempty"`
}

var statusCodes = map[applicationErrorType]int{
	BadRequestType:           400,
	LimitExceededType:        402,
	ForbiddenType:            403,
	ResourceNotFoundType:     404,
	MethodNotAllowedType:     405,
	ConflictExceptionType:    409,
	UnsupportedMediaTypeType: 415,
	InternalServerErrorType:  500,
	//NotImplementedErrorType:  501,
}

func (e *ApplicationError) StatusCode() int {
	errorType := e.ErrorType

	e.ErrorType = ""

	return statusCodes[errorType]
}

func BadRequest(message string, details ...interface{}) *ApplicationError {
	return &ApplicationError{BadRequestType, message, details}
}

func LimitExceeded(message string, details ...interface{}) *ApplicationError {
	return &ApplicationError{LimitExceededType, message, details}
}

func Forbidden(details ...interface{}) *ApplicationError {
	return &ApplicationError{ForbiddenType, "Not authorized", details}
}

type Identifiable interface {
	Id() string
}

func ResourceNotFound(identifiables ...Identifiable) *ApplicationError {
	details := make([]interface{}, len(identifiables))

	for i, identifiable := range identifiables {
		qualifiedType := fmt.Sprintf("%T", identifiable)
		simpleType := qualifiedType[strings.IndexByte(qualifiedType, '.')+1:]

		details[i] = map[string]string{
			"type": simpleType,
			"id":   identifiable.Id(),
		}
	}

	return &ApplicationError{ResourceNotFoundType, "Resource not found", details}
}

func MethodNotAllowed(method string, details ...interface{}) *ApplicationError {
	return &ApplicationError{MethodNotAllowedType, method + " not supported on this resource type", details}
}

func ConflictException(message string, details ...interface{}) *ApplicationError {
	return &ApplicationError{ConflictExceptionType, message, details}
}

func UnsupportedMediaType(receivedContentType string, supportedContentTypes []string) *ApplicationError {
	details := make([]interface{}, 2)

	details[0] = map[string]string{
		"received": receivedContentType,
	}

	details[1] = map[string][]string{
		"supported": supportedContentTypes,
	}

	return &ApplicationError{UnsupportedMediaTypeType, "Unsupported media type", details}
}

func InternalServerError(message string, details ...interface{}) *ApplicationError {
	return &ApplicationError{InternalServerErrorType, message, details}
}

func ValidationFailure(err error) *ApplicationError {
	// this check is only needed when your code could produce
	// an invalid value for validation such as interface with nil
	// value most including myself do not usually have code like this.
	if _, ok := err.(*validator.InvalidValidationError); ok {
		logs.Error.Print(err)

		return InternalServerError("Error while validating input")
	}

	var details []interface{}

	for _, err := range err.(validator.ValidationErrors) {
		//fmt.Println(err.Namespace())
		//fmt.Println(err.Field())
		//fmt.Println(err.StructNamespace()) // can differ when a custom TagNameFunc is registered or
		//fmt.Println(err.StructField())     // by passing alt name to ReportError like below
		//fmt.Println(err.Tag())
		//fmt.Println(err.ActualTag())
		//fmt.Println(err.Kind())
		//fmt.Println(err.Type())
		//fmt.Println(err.Value())
		//fmt.Println(err.Param())

		if err.Tag() == "required" {
			details = append(details, fmt.Sprintf("Validation error: missing %s attribute", err.Field()))
		} else {
			details = append(details, fmt.Sprintf("Validation error: %s %s %s", err.Field(), err.Value(), err.Tag()))
		}
	}

	if len(details) == 0 {
		return nil
	}

	return &ApplicationError{BadRequestType, "Validation errors in request.", details}
}
