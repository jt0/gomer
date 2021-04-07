package http_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// String attribute
func Attr(k, v string) string {
	if v == "" {
		return ""
	}
	return fmt.Sprintf(`"%s": "%s"`, k, v)
}

// String attribute
func AttrJson(k, v string) string {
	if v == "" {
		return ""
	}
	vb, _ := json.Marshal(v)
	return fmt.Sprintf(`"%s": %s`, k, string(vb))
}

// Pointer to string attribute
func AttrP(k string, v *string) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf(`"%s": "%s"`, k, *v)
}

// Raw string to attribute
func AttrR(k, v string) string {
	if v == "" {
		return ""
	}
	return fmt.Sprintf(`"%s": %s`, k, v)
}

// Int to attribute
func AttrI(k string, i int) string {
	if i == 0 {
		return ""
	}
	return fmt.Sprintf(`"%s": %d`, k, i)
}

// Uint to attribute
func AttrU(k string, u uint) string {
	if u == 0 {
		return ""
	}
	return fmt.Sprintf(`"%s": %d`, k, u)
}

// Float to attribute
func AttrF(k string, f float64) string {
	if f == 0 {
		return ""
	}
	return fmt.Sprintf(`"%s": %f`, k, f)
}

// Float to attribute
func AttrPF(k string, f *float64) string {
	if f == nil || *f == 0 {
		return ""
	}
	return fmt.Sprintf(`"%s": %f`, k, *f)
}

// Float to attribute
func AttrPF32(k string, f *float32) string {
	if f == nil || *f == 0 {
		return ""
	}
	return fmt.Sprintf(`"%s": %f`, k, *f)
}

// Object from processed attributes
func Obj(attrs ...string) string {
	return wrap('{', attrs, '}')
}

// Slice from processed attributes
func Slice(attrs ...string) string {
	return wrap('[', attrs, ']')
}

func Path(parts ...string) string {
	var b bytes.Buffer
	b.Grow(32)

	for _, p := range parts {
		b.WriteByte('/')
		b.WriteString(p)
	}

	return b.String()
}

func QAttr(k, v string) string {
	if v == "" {
		return ""
	}
	return fmt.Sprintf("%s=%s", k, v)
}

func QAttrP(k string, v *string) string {
	if v == nil {
		return ""
	}
	return QAttr(k, *v)
}

func QAttrI(k string, i int) string {
	if i == 0 {
		return ""
	}
	return fmt.Sprintf("%s=%d", k, i)
}

func Query(attrs ...string) string {
	var b bytes.Buffer
	b.Grow(32)

	b.WriteByte('?')
	for _, a := range attrs {
		b.WriteString(omitempty(a, "&"))
	}
	b.Truncate(b.Len() - 1) // either trims the trailing & or initial ?

	return b.String()
}

const NoBody = ""

func Request(pathAndQueryParams string, body string) *http.Request {
	parsed, err := url.Parse(pathAndQueryParams)
	if err != nil {
		fmt.Print("Failed to parse url:", pathAndQueryParams, " Error:", err.Error())
	}
	return &http.Request{URL: parsed, Body: ioutil.NopCloser(strings.NewReader(body))}
}

func AddHeaders(req *http.Request, attrs ...string) {
}

func wrap(open byte, attrs []string, close byte) string {
	var b bytes.Buffer
	b.Grow(64)

	b.WriteByte(open)
	for _, a := range attrs {
		b.WriteString(omitempty(a, ", ")) // adds ", " to all non-empty values
	}
	if bLen := b.Len(); bLen > 1 {
		b.Truncate(bLen - 2) // trims trailing ", "
	}
	b.WriteByte(close)

	return b.String()
}

func omitempty(s, nonEmptySuffix string) string {
	if s == "" || s == "{}" || s == "[]" {
		return ""
	}

	return s + nonEmptySuffix
}
