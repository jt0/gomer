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

	FirstName  *string `in:"+" out:"+"`
	LastName   *string `in:"+" out:"+"`
	MiddleName *string `in:"" out:""`
}

var personActions = map[any]func() resource.AnyAction{PostCollection: func() resource.AnyAction { return resource.CreateAction[*Person]() }}

func init() {
	resource.Register[*Person](registry, resource.WithActions(personActions), resource.WithStore(stores.PanicStore))
}

func TestConfigure_PascalCaseFields(t *testing.T) {
	Configure(PascalCaseFields)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"FirstName": "Alice", "LastName": "Wonder"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
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

	person, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
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

	person, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Nil(t, person.FirstName) // nil since field case is different from expected
	assert.Nil(t, person.LastName)  // ^^
}

func TestConfigure_MultipleOptions(t *testing.T) {
	// Configure with multiple options including IncludeEmptyValues
	Configure(CamelCaseFields, IncludeEmptyValues)

	// Test with non-empty middleName
	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "James", "middleName": "Earl", "lastName": "Jones"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "James", *person.FirstName)
	assert.Equals(t, "Earl", *person.MiddleName)
	assert.Equals(t, "Jones", *person.LastName)

	// Test with empty middleName - IncludeEmptyValues means pointer should be non-nil, pointing to ""
	req2 := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "James", "middleName": "", "lastName": "Jones"}`)),
	}

	person2, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
	ge = BindFromRequest(req2, person2, "test")
	assert.Success(t, ge)

	assert.Equals(t, "James", *person2.FirstName)
	assert.Equals(t, "", *person2.MiddleName) // non-nil pointer to empty string
	assert.Equals(t, "Jones", *person2.LastName)
}

func TestConfigure_OmitEmptyValues(t *testing.T) {
	// Configure with default OmitEmptyValues (explicitly set for clarity)
	Configure(CamelCaseFields, OmitEmptyValues)

	// Test with empty middleName - OmitEmptyValues means pointer should remain nil
	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "James", "middleName": "", "lastName": "Jones"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "James", *person.FirstName)
	assert.Nil(t, person.MiddleName) // nil because empty value is omitted
	assert.Equals(t, "Jones", *person.LastName)
}

func TestNewBindingConfiguration_SetAsDefault(t *testing.T) {
	// Create and set configuration explicitly
	NewBindingConfiguration(CamelCaseFields).SetAsDefault()

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Plato"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
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

	person, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "Charlie", *person.FirstName)
	assert.Equals(t, "Brown", *person.LastName)
}

func TestConfigure_RequestOption(t *testing.T) {
	// Configure with request-specific override for IncludeEmptyValues
	Configure(
		CamelCaseFields,
		RequestOption(IncludeEmptyValues),
	)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Edgar", "middleName": "", "lastName": "Poe"}`)),
	}

	person, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "", *person.MiddleName) // IncludeEmptyValues means non-nil pointer to empty string
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

	person, _ := resource.NewInstance[*Person](ctxWithRegistry, subject)
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
