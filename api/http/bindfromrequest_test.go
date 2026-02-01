package http_test

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/_test/helpers/stores"
	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/resource"
)

var (
	subject = auth.NewSubject(auth.ReadWriteAllFields)
	actions = map[interface{}]func() resource.Action{PostCollection: resource.CreateAction}
)

//goland:noinspection GoSnakeCaseUsage
type Greeting struct {
	resource.BaseInstance `structs:"ignore"`

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
	d := resource.NewDomain()
	_, ge := d.Register(&Greeting{}, nil, actions, stores.PanicStore)
	assert.Success(t, ge)

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

	greetingsType := reflect.TypeOf(&Greeting{})
	var r resource.Resource
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, ge = BindFromRequest(tt.request, greetingsType, subject, "some_scope")
			assert.Success(t, ge)
			greeting := r.(*Greeting)
			assert.Equals(t, hello, greeting.style(tt.location))
			assert.Equals(t, kitty, greeting.recipient(tt.location))
		})
	}
}

func body(input string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(input))
}
