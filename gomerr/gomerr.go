package gomerr

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"unicode"

	"github.com/jt0/gomer/util"
)

type Gomerr interface {
	Wrap(err error) Gomerr
	AddAttribute(key string, value interface{}) Gomerr
	AddAttributes(keysAndValues ...interface{}) Gomerr
	WithAttributes(attributes map[string]interface{}) Gomerr
	//Retryable(retryable bool) Gomerr

	Unwrap() error
	Attributes() map[string]interface{}
	Stack() []string
	ToMap() map[string]interface{}
	Error() string
	//IsRetryable() bool
}

var gomerrType = reflect.TypeOf((*Gomerr)(nil)).Elem()

func Build(g Gomerr, attributes ...interface{}) Gomerr {
	build(reflect.ValueOf(g).Elem(), attributes, newGomerr(4, g))

	return g
}

func build(v reflect.Value, attributes []interface{}, gomerr *gomerr) (attributesProcessed int) {
	attributesProcessed = 0
	attributesLength := len(attributes)
	for i := 0; i < v.NumField(); i++ {
		fv := v.Field(i)

		if !fv.CanSet() || !fv.IsZero() {
			continue
		}

		if v.Type().Field(i).Anonymous {
			if gomerrType.AssignableTo(fv.Type()) {
				fv.Set(reflect.ValueOf(gomerr))
				continue
			}

			// follow anonymous structs or pointers to structs
			if fv.Type().Kind() == reflect.Struct {
				attributesProcessed += build(fv, attributes[attributesProcessed:], gomerr)
			} else if fv.Type().Kind() == reflect.Ptr && fv.Type().Elem().Kind() == reflect.Struct {
				attributesProcessed += build(fv.Elem(), attributes[attributesProcessed:], gomerr)
			}
		}

		if attributesProcessed < attributesLength && reflect.TypeOf(attributes[attributesProcessed]).AssignableTo(fv.Type()) {
			fv.Set(reflect.ValueOf(attributes[attributesProcessed]))
			attributesProcessed++
			continue
		}
	}

	return
}

type gomerr struct {
	self       Gomerr
	wrapped    error
	attributes map[string]interface{}
	stack      []string
	//retryable    bool
}

func newGomerr(stackSkip int, self Gomerr) *gomerr {
	g := &gomerr{}
	if self == nil {
		g.self = g
	} else {
		g.self = self
	}

	g.stack = fillStack(stackSkip)

	return g
}

func fillStack(stackSkip int) []string {
	callers := make([]uintptr, 30)
	depth := runtime.Callers(stackSkip+1, callers) // +1 for compared to runtime.Caller()
	callers = callers[:depth]

	stack := make([]string, depth)
	frames := runtime.CallersFrames(callers)
	for i := 0; i < depth; i++ {
		frame, _ := frames.Next()
		if strings.HasPrefix(frame.Function, "runtime.") {
			stack = stack[:i]
			break
		}
		function := frame.Function[strings.LastIndexByte(frame.Function, '/')+1:]
		stack[i] = fmt.Sprintf("%s -- %s:%d", function, frame.File, frame.Line)
	}

	return stack
}

//func relative(file string) string {
//	_, thisFile, _, _ := runtime.Caller(0)
//
//	gomerPath := thisFile[:strings.LastIndex(thisFile, "/gomerr/")]
//	basePath := gomerPath[:strings.LastIndex(gomerPath, "/")]
//	rel, err := filepath.Rel(basePath, file)
//	if err != nil {
//		return file
//	}
//
//	return strings.TrimLeft(rel, "./")
//}

func (g *gomerr) Wrap(err error) Gomerr {
	if g.wrapped != nil {
		panic("cannot change wrapped error once set")
	}

	g.wrapped = err
	return g.self // Ensure we don't lose the actual error struct
}

func (g *gomerr) AddAttribute(key string, value interface{}) Gomerr {
	// XXX newGomerr? (or maybe we can just always add to g itself)
	//gw := newGomerr(2, g.self) // wrap first to get line/file info
	//
	//// If the notes are being added in the same place g is introduced, use g instead of the new one
	//if g.stack[0].Line == gw.stack[0].Line && g.stack[0].File == gw.stack[0].File {
	//	gw = g
	//}

	if g.attributes == nil {
		g.attributes = make(map[string]interface{})
	}

	addAttribute(key, value, &g.attributes)

	return g.self
}

func (g *gomerr) AddAttributes(keysAndValues ...interface{}) Gomerr {
	if len(keysAndValues)%2 != 0 {
		return Configuration("AddAttributes() requires an even number of arguments for keysAndValues").AddAttributes("Input", keysAndValues, "TargetedError", g)
	}

	if g.attributes == nil {
		g.attributes = make(map[string]interface{})
	}

	for i := 0; i < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			keyStringer, ok := keysAndValues[i].(fmt.Stringer)
			if !ok {
				key = fmt.Sprintf("[Non-string key type %T]: %v", keysAndValues[i], keysAndValues[i])
			}
			key = keyStringer.String()
		}

		addAttribute(key, keysAndValues[i+1], &g.attributes)
	}

	return g
}

func (g *gomerr) WithAttributes(attributes map[string]interface{}) Gomerr {
	// Short-circuit if no attributes yet
	if g.attributes == nil {
		g.attributes = attributes
		return g.self
	}

	// Add each individually to handle potential name conflict
	for k, v := range attributes {
		addAttribute(k, v, &g.attributes)
	}

	return g.self
}

func addAttribute(key string, value interface{}, m *map[string]interface{}) {
	_, exists := (*m)[key]
	if exists {
		// TODO: generate alternate key
	}

	(*m)[key] = value
}

// Implicitly used by errors.Is()/errors.As()
func (g *gomerr) Unwrap() error {
	return g.wrapped
}

func (g *gomerr) Attributes() map[string]interface{} {
	return g.attributes
}

func (g *gomerr) Stack() []string {
	return g.stack
}

func (g *gomerr) ToMap() map[string]interface{} {
	gt := reflect.TypeOf(g.self).Elem()
	gv := reflect.ValueOf(g.self).Elem()
	m := make(map[string]interface{}, gt.NumField())

	for i := 0; i < gt.NumField(); i++ {
		ft := gt.Field(i)
		fv := gv.Field(i)

		if ft.Anonymous || unicode.IsLower([]rune(ft.Name)[0]) || fv.IsZero() {
			continue
		}

		name := ft.Name
		// conventionally, strip of trailing underscore when calling ToMap()
		if strings.HasSuffix(name, "_") {
			name = name[0 : len(name)-1]
		}

		fi := fv.Interface()
		m[name] = fi
		if tag := ft.Tag.Get("gomerr"); tag != "" {
			if tag == "include_type" {
				m["_"+name+"Type"] = util.UnqualifiedTypeName(fi)
			}
		}
	}

	m["_Gomerr"] = util.UnqualifiedTypeName(g.self)
	m["_Stack"] = g.stack //  XXX: make sure not repeated...maybe remove overlapping parts from child...

	if g.attributes != nil && len(g.attributes) > 0 {
		m["_Attributes"] = g.attributes
	}

	if wrapped := g.Unwrap(); wrapped != nil {
		if g, ok := wrapped.(Gomerr); ok {
			m["_Cause"] = g.ToMap()
		} else {
			m["_Cause"] = map[string]interface{}{
				util.UnqualifiedTypeName(wrapped): wrapped,
				"_Error()":                        wrapped.Error(),
			}
		}
	}

	return m
}

func (g *gomerr) Error() string {
	return g.String()
}

func (g *gomerr) String() string {
	if bytes, err := json.MarshalIndent(g.ToMap(), "  ", "  "); err != nil {
		return "Failed to create gomerr string representation: " + err.Error()
	} else {
		return string(bytes)
	}
}

//func (g *gomerr) Retryable(retryable bool) Gomerr {
//	g.retryable = retryable
//	return g
//}

//func (g *gomerr) IsRetryable() bool {
//	return g.retryable
//}
