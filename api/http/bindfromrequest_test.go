package http_test

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/_test/helpers/stores"
	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/resource"
)

var (
	subject         = auth.NewSubject(auth.ReadWriteAllFields)
	actions         = map[any]func() resource.AnyAction{PostCollection: func() resource.AnyAction { return resource.CreateAction[*Greeting]() }}
	registry        = resource.NewRegistry()
	ctxWithRegistry = context.WithValue(context.TODO(), resource.RegistryCtxKey, registry)
)

//goland:noinspection GoSnakeCaseUsage
type Greeting struct {
	resource.BaseInstance[*Greeting] `structs:"ignore"`

	Style_path       string `in:"path.0"`
	Recipient_path   string `in:"path.1"`
	Style_query      string `in:"query.+"` // same name as attribute
	Recipient_query  string `in:"query.recipient"`
	Style_header     string `in:"header.+"` // same name as attribute
	Recipient_header string `in:"header.x-recipient"`
	Style_body       string `in:"Style"`
	Recipient_body   string `in:"Recipient"`
}

const (
	Path = iota
	Query
	Header
	Body
)

func (g Greeting) style(location int) string {
	switch location {
	case Path:
		return g.Style_path
	case Query:
		return g.Style_query
	case Header:
		return g.Style_header
	default:
		return g.Style_body
	}
}

func (g Greeting) recipient(location int) string {
	switch location {
	case Path:
		return g.Recipient_path
	case Query:
		return g.Recipient_query
	case Header:
		return g.Recipient_header
	default:
		return g.Recipient_body
	}
}

func TestBindInTypes(t *testing.T) {
	resource.Register[*Greeting](registry, resource.WithActions(actions), resource.WithStore(stores.PanicStore))

	const (
		hello = "hello"
		kitty = "kitty"
	)

	type testcase struct {
		name     string
		location int
		request  *http.Request
	}
	tests := []testcase{
		{"BindFromPath", Path, &http.Request{URL: &url.URL{Path: "/" + hello + "/" + kitty}, Body: body("")}},
		{"BindFromQuery", Query, &http.Request{URL: &url.URL{RawQuery: "Style_query=" + hello + "&recipient=" + kitty}, Body: body("")}},
		// NB: header names can have different casing from the 'in' header config
		{"BindFromHeader", Header, &http.Request{URL: &url.URL{Path: "/"}, Header: http.Header{"Style_header": []string{hello}, "X-Recipient": []string{kitty}}, Body: body("")}},
		{"BindFromBody", Body, &http.Request{URL: &url.URL{Path: "/"}, Body: body("{ \"Style\": \"" + hello + "\", \"Recipient\": \"" + kitty + "\" }")}},
	}

	greeting, ge := resource.NewInstance[*Greeting](ctxWithRegistry, subject)
	assert.Success(t, ge)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ge = BindFromRequest(tt.request, greeting, "some_scope")
			assert.Success(t, ge)
			assert.Equals(t, hello, greeting.style(tt.location))
			assert.Equals(t, kitty, greeting.recipient(tt.location))
		})
	}
}

func body(input string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(input))
}
