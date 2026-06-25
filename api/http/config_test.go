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
	MiddleName *string `in:"+" out:"+"`
	Title      *string `in:"" out:""`
	Suffix     string  `in:"" out:""`
}

var personActions = map[any]func() resource.AnyAction{PostCollection: func() resource.AnyAction { return resource.CreateAction[*Person]() }}

func init() {
	resource.Register[*Person](ctx, resource.WithActions(personActions), resource.WithStore(stores.PanicStore))
}

func TestConfigure_PascalCaseFields(t *testing.T) {
	Configure(PascalCaseFields)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"FirstName": "Alice", "LastName": "Wonder"}`)),
	}

	person := resource.NewInstance[*Person](ctx, subject)
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

	person := resource.NewInstance[*Person](ctx, subject)
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

	person := resource.NewInstance[*Person](ctx, subject)
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

	person := resource.NewInstance[*Person](ctx, subject)
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

	person2 := resource.NewInstance[*Person](ctx, subject)
	ge = BindFromRequest(req2, person2, "test")
	assert.Success(t, ge)

	assert.Equals(t, "James", *person2.FirstName)
	assert.Equals(t, "", *person2.MiddleName) // non-nil pointer to empty string
	assert.Equals(t, "Jones", *person2.LastName)
}

func TestConfigure_SkipEmptyDirectives(t *testing.T) {
	Configure(CamelCaseFields)

	// Test with empty middleName - OmitEmptyValues means pointer should remain nil
	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Alec", "lastName": "Guinness", "":"", "title": "Sir", "suffix": "ignoreme"}`)),
	}

	person := resource.NewInstance[*Person](ctx, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "Alec", *person.FirstName)
	assert.Equals(t, "Guinness", *person.LastName)
	assert.Nil(t, person.MiddleName)    // nil because value isn't in payload
	assert.Nil(t, person.Title)         // nil because empty directive
	assert.Equals(t, "", person.Suffix) // zero because empty directive
}

func TestNewBindingConfiguration_SetAsDefault(t *testing.T) {
	// Create and set configuration explicitly
	NewBindingConfiguration(CamelCaseFields).SetAsDefault()

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Plato"}`)),
	}

	person := resource.NewInstance[*Person](ctx, subject)
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

	person := resource.NewInstance[*Person](ctx, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "Charlie", *person.FirstName)
	assert.Equals(t, "Brown", *person.LastName)
}

func TestConfigure_RequestOption(t *testing.T) {
	Configure(CamelCaseFields, IncludeEmptyValues)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Edgar", "middleName": "", "lastName": "Poe"}`)),
	}

	person := resource.NewInstance[*Person](ctx, subject)
	ge := BindFromRequest(req, person, "test")
	assert.Success(t, ge)

	assert.Equals(t, "", *person.MiddleName)
}

func TestConfigure_ResponseOption(t *testing.T) {
	// Configure with response-specific override
	Configure(CamelCaseFields, IncludeEmptyValues)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Madonna"}`)),
	}

	person := resource.NewInstance[*Person](ctx, subject)
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
