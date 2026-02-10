package http_test

import (
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/_test/helpers/stores"
	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/resource"
)

type Person struct {
	resource.BaseInstance[*Person] `structs:"ignore"`

	FirstName *string `in:"+" out:"+"`
	LastName  *string `in:"+" out:"+"`

	MiddleName *string
}

var personActions = map[any]func() resource.AnyAction{PostCollection: func() resource.AnyAction { return resource.CreateAction[*Person]() }}

func init() {
	_, ge := resource.Register[*Person](domain, nil, personActions, stores.PanicStore)
	if ge != nil {
		panic(ge)
	}
}

func TestConfigure_PascalCaseFields(t *testing.T) {
	Configure(PascalCaseFields)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"FirstName": "Alice", "LastName": "Wonder"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithDomain, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "Alice", *person.FirstName)
	assert.Equals(t, "Wonder", *person.LastName)
}

func TestConfigure_CamelCaseFields(t *testing.T) {
	Configure(CamelCaseFields)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Bruce", "lastName": "Wayne"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithDomain, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "Bruce", *person.FirstName)
	assert.Equals(t, "Wayne", *person.LastName)
}

func TestConfigure_PascalCaseFields_CamelCaseInput(t *testing.T) {
	// Configure with multiple options
	Configure(PascalCaseFields)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Betty", "lastName": "Crocker"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithDomain, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Nil(t, person.FirstName) // nil since field case is different from expected
	assert.Nil(t, person.LastName)  // ^^
}

func TestConfigure_MultipleOptions(t *testing.T) {
	// Configure with multiple options
	Configure(CamelCaseFields, IncludeEmptyDirective)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "James", "middleName": "Earl", "lastName": "Jones"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithDomain, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "James", *person.FirstName)
	assert.Equals(t, "Earl", *person.MiddleName) // IncludeEmptyDirective should cause this to be set
	assert.Equals(t, "Jones", *person.LastName)
}

func TestNewBindingConfiguration_SetAsDefault(t *testing.T) {
	// Create and set configuration explicitly
	NewBindingConfiguration(CamelCaseFields).SetAsDefault()

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Plato"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithDomain, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "Plato", *person.FirstName)
	assert.Nil(t, person.LastName)
}

func TestConfigure_CamelCaseFields_DefaultNaming(t *testing.T) {
	Configure(CamelCaseFields)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Charlie", "lastName": "Brown"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithDomain, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "Charlie", *person.FirstName)
	assert.Equals(t, "Brown", *person.LastName)
}

func TestConfigure_RequestOption(t *testing.T) {
	// Configure with request-specific override
	Configure(
		CamelCaseFields,
		RequestOption(IncludeEmptyDirective),
	)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Edgar", "middleName": "Allan", "lastName": "Poe"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithDomain, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "Allan", *person.MiddleName)
}

func TestConfigure_ResponseOption(t *testing.T) {
	// Configure with response-specific override
	Configure(
		CamelCaseFields,
		OmitEmptyValues,
		ResponseOption(IncludeEmptyValues),
	)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Madonna"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithDomain, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "Madonna", *person.FirstName)
	assert.Nil(t, person.LastName) // nil since not provided and OmitEmptyValues is set

	// Bind to response - should include lastName:null due to ResponseOption(IncludeEmptyValues)
	header := make(http.Header)
	bytes, statusCode := BindToResponse(reflect.ValueOf(person), header, "test", "", http.StatusOK)
	assert.Equals(t, http.StatusOK, statusCode)

	responseBody := string(bytes)
	assert.Assert(t, strings.Contains(responseBody, `"firstName":"Madonna"`))
	assert.Assert(t, strings.Contains(responseBody, `"lastName":null`))
}
