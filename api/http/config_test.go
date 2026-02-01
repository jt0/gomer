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
	resource.BaseInstance `structs:"ignore"`

	FirstName *string `in:"+" out:"+"`
	LastName  *string `in:"+" out:"+"`

	MiddleName *string
}

func init() {
	d := resource.NewDomain()
	_, ge := d.Register(&Person{}, nil, actions, stores.PanicStore)
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

	r, ge := BindFromRequest(req, reflect.TypeOf(&Person{}), subject, "test")
	assert.Success(t, ge)

	p := r.(*Person)
	assert.Equals(t, "Alice", *p.FirstName)
	assert.Equals(t, "Wonder", *p.LastName)
}

func TestConfigure_CamelCaseFields(t *testing.T) {
	Configure(CamelCaseFields)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Bruce", "lastName": "Wayne"}`)),
	}

	r, ge := BindFromRequest(req, reflect.TypeOf(&Person{}), subject, "test")
	assert.Success(t, ge)

	p := r.(*Person)
	assert.Equals(t, "Bruce", *p.FirstName)
	assert.Equals(t, "Wayne", *p.LastName)
}

func TestConfigure_PascalCaseFields_CamelCaseInput(t *testing.T) {
	// Configure with multiple options
	Configure(PascalCaseFields)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Betty", "lastName": "Crocker"}`)),
	}

	r, ge := BindFromRequest(req, reflect.TypeOf(&Person{}), subject, "test")
	assert.Success(t, ge)

	p := r.(*Person)
	assert.Nil(t, p.FirstName) // nil since field case is different from expected
	assert.Nil(t, p.LastName)  // ^^
}

func TestConfigure_MultipleOptions(t *testing.T) {
	// Configure with multiple options
	Configure(CamelCaseFields, IncludeEmptyDirective)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "James", "middleName": "Earl", "lastName": "Jones"}`)),
	}

	r, ge := BindFromRequest(req, reflect.TypeOf(&Person{}), subject, "test")
	assert.Success(t, ge)

	p := r.(*Person)
	assert.Equals(t, "James", *p.FirstName)
	assert.Equals(t, "Earl", *p.MiddleName) // IncludeEmptyDirective should cause this to be set
	assert.Equals(t, "Jones", *p.LastName)
}

func TestNewBindingConfiguration_SetAsDefault(t *testing.T) {
	// Create and set configuration explicitly
	NewBindingConfiguration(CamelCaseFields).SetAsDefault()

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Plato"}`)),
	}

	r, ge := BindFromRequest(req, reflect.TypeOf(&Person{}), subject, "test")
	assert.Success(t, ge)

	p := r.(*Person)
	assert.Equals(t, "Plato", *p.FirstName)
	assert.Nil(t, p.LastName)
}

func TestConfigure_CamelCaseFields_DefaultNaming(t *testing.T) {
	Configure(CamelCaseFields)

	req := &http.Request{
		URL:  &url.URL{Path: "/"},
		Body: io.NopCloser(strings.NewReader(`{"firstName": "Charlie", "lastName": "Brown"}`)),
	}

	r, ge := BindFromRequest(req, reflect.TypeOf(&Person{}), subject, "test")
	assert.Success(t, ge)

	p := r.(*Person)
	assert.Equals(t, "Charlie", *p.FirstName)
	assert.Equals(t, "Brown", *p.LastName)
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

	r, ge := BindFromRequest(req, reflect.TypeOf(&Person{}), subject, "test")
	assert.Success(t, ge)

	p := r.(*Person)
	assert.Equals(t, "Allan", *p.MiddleName)
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

	r, ge := BindFromRequest(req, reflect.TypeOf(&Person{}), subject, "test")
	assert.Success(t, ge)

	p := r.(*Person)
	assert.Equals(t, "Madonna", *p.FirstName)
	assert.Nil(t, p.LastName) // nil since not provided and OmitEmptyValues is set

	// Bind to response - should include lastName:null due to ResponseOption(IncludeEmptyValues)
	header := make(http.Header)
	bytes, statusCode := BindToResponse(reflect.ValueOf(p), header, "test", "", http.StatusOK)
	assert.Equals(t, http.StatusOK, statusCode)

	responseBody := string(bytes)
	assert.Assert(t, strings.Contains(responseBody, `"firstName":"Madonna"`))
	assert.Assert(t, strings.Contains(responseBody, `"lastName":null`))
}
