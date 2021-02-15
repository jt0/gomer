package http_test

import (
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/_test/helpers/stores"
	. "github.com/jt0/gomer/api/http"
	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/resource"
)

var (
	subject = auth.NewSubject(auth.ReadWriteAllFields)
	actions = map[interface{}]func() resource.Action{PostCollection: resource.CreateAction}
)

//goland:noinspection GoSnakeCaseUsage
type Greeting struct {
	resource.BaseInstance `fields:"ignore"`

	Style_path       string `bind.path:"0"`
	Recipient_path   string `bind.path:"1"`
	Style_query      string `bind.query:""` // same name as attribute
	Recipient_query  string `bind.query:"recipient"`
	Style_header     string `bind.header:""` // same name as attribute
	Recipient_header string `bind.header:"x-recipient"`
}

const (
	Path = iota
	Query
	Header
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
		return ""
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
		return ""
	}
}

func TestBindTypes(t *testing.T) {
	fields.TagToFieldToolAssociations(map[string]fields.FieldTool{
		"bind.path":   BindPathTool,
		"bind.query":  BindQueryParamTool,
		"bind.header": BindHeaderTool,
		"bind.body":   BindBodyTool,
	})

	_, ge := resource.Register(&Greeting{}, nil, actions, stores.PanicStore, nil)
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
		{"BindFromPath", Path, &http.Request{URL: &url.URL{Path: "/" + hello + "/" + kitty}}},
		{"BindFromQuery", Query, &http.Request{URL: &url.URL{RawQuery: "Style_query=" + hello + "&recipient=" + kitty}}},
		{"BindFromHeader", Header, &http.Request{URL: &url.URL{Path: "/"}, Header: http.Header{"Style_header": []string{hello}, "X-Recipient": []string{kitty}}}},
	}

	greetingsType := reflect.TypeOf(&Greeting{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, ge := BindFromRequest(tt.request, greetingsType, subject)
			assert.Success(t, ge)
			greeting := r.(*Greeting)
			assert.Equals(t, hello, greeting.style(tt.location))
			assert.Equals(t, kitty, greeting.recipient(tt.location))
		})
	}
}
