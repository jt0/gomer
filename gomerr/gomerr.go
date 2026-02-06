package gomerr

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"unicode"
)

type Gomerr interface {
	error
	Unwrap() error
	Is(err error) bool

	Wrap(err error) Gomerr
	AddAttribute(key string, value any) Gomerr
	ReplaceAttribute(key string, value any) Gomerr
	DeleteAttribute(key string) Gomerr
	AddAttributes(keysAndValues ...any) Gomerr
	WithAttributes(attributes map[string]any) Gomerr

	Attribute(key string) (value any)
	AttributeLookup(key string) (value any, ok bool)
	Attributes() map[string]any
	String() string
	ToMap() map[string]any

	// Ensures that Gomerrs behave as expected
	isFromBuildFunc() bool
}

func ErrorAs[E error](err error) (e E) {
	errors.As(err, &e)
	return
}

var gomerrType = reflect.TypeOf((*Gomerr)(nil)).Elem()

func Build(g Gomerr, attributes ...any) Gomerr {
	build(reflect.ValueOf(g).Elem(), attributes, newGomerr(4, g))

	return g
}

func build(v reflect.Value, attributes []any, gomerr *gomerr) (attributesProcessed int) {
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

		if attributesProcessed < attributesLength {
			av := reflect.ValueOf(attributes[attributesProcessed])
			if av.IsValid() && av.Type().AssignableTo(fv.Type()) {
				fv.Set(av)
			}
			attributesProcessed++
		}
	}

	return
}

type gomerr struct {
	self       Gomerr
	wrapped    error
	attributes map[string]any
	stack      []string
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
		function := frame.Function[strings.LastIndexByte(frame.Function, '/')+1:]
		stack[i] = fmt.Sprintf("%s -- %s:%d", function, frame.File, frame.Line)
	}

	return stack
}

// func relative(file string) string {
// 	_, thisFile, _, _ := runtime.Caller(0)
//
// 	gomerPath := thisFile[:strings.LastIndex(thisFile, "/gomerr/")]
// 	basePath := gomerPath[:strings.LastIndex(gomerPath, "/")]
// 	rel, err := filepath.Rel(basePath, file)
// 	if err != nil {
// 		return file
// 	}
//
// 	return strings.TrimLeft(rel, "./")
// }

func (g *gomerr) Wrap(err error) Gomerr {
	if g.wrapped != nil {
		panic("cannot change wrapped error once set")
	}

	g.wrapped = err

	return g.self
}

func (g *gomerr) Attribute(key string) any {
	return g.attributes[key]
}

func (g *gomerr) AttributeLookup(key string) (value any, ok bool) {
	value, ok = g.attributes[key]
	return
}

func (g *gomerr) AddAttribute(key string, value any) Gomerr {
	g.addAttribute(key, value, add)
	return g.self
}

func (g *gomerr) ReplaceAttribute(key string, value any) Gomerr {
	g.addAttribute(key, value, replace)
	return g.self
}

func (g *gomerr) DeleteAttribute(key string) Gomerr {
	delete(g.attributes, key)
	return g.self
}

func (g *gomerr) AddAttributes(keysAndValues ...any) Gomerr {
	if len(keysAndValues)%2 != 0 {
		return Configuration("AddAttributes() requires an even number of arguments for keysAndValues").AddAttributes("Input", keysAndValues, "TargetedError", g)
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

		g.addAttribute(key, keysAndValues[i+1], add)
	}

	return g.self
}

func (g *gomerr) WithAttributes(attributes map[string]any) Gomerr {
	// Short-circuit if no attributes yet
	if g.attributes == nil {
		g.attributes = attributes
		return g.self
	}

	// Add each individually to handle potential name conflict
	for k, v := range attributes {
		g.addAttribute(k, v, replace)
	}

	return g.self
}

type addType uint8

const (
	add addType = iota + 1
	replace
)

func (g *gomerr) addAttribute(key string, toAdd any, addType addType) {
	// gw := newGomerr(2, g.self) // wrap first to get line/file info
	//
	// // If the notes are being added in the same place g is introduced, use g instead of the new one
	// if g.Stack[0].Line == gw.Stack[0].Line && g.Stack[0].File == gw.Stack[0].File {
	// 	gw = g
	// }

	if g.attributes == nil {
		g.attributes = make(map[string]any)
	}

	if value, exists := g.attributes[key]; exists && addType == add {
		if valueSlice, ok := value.([]any); !ok {
			g.attributes[key] = []any{value, toAdd}
		} else {
			g.attributes[key] = append(valueSlice, toAdd)
		}
	} else {
		g.attributes[key] = toAdd
	}
}

func (g *gomerr) Is(err error) bool {
	return reflect.TypeOf(g.self) == reflect.TypeOf(err)
}

// Implicitly used by errors.Is()/errors.As()

func (g *gomerr) Unwrap() error {
	return g.wrapped
}

func (g *gomerr) Attributes() map[string]any {
	return g.attributes
}

func (g *gomerr) Stack() []string {
	return g.stack
}

func (g *gomerr) ToMap() map[string]any {
	gt := reflect.TypeOf(g.self)
	gte := gt.Elem()
	gve := reflect.ValueOf(g.self).Elem()

	m := make(map[string]any, gte.NumField()+1)
	m["$.errorType"] = gt.String()

	for i := 0; i < gte.NumField(); i++ {
		ft := gte.Field(i)
		fv := gve.Field(i)
		if ft.Anonymous || unicode.IsLower([]rune(ft.Name)[0]) || !fv.IsValid() {
			continue
		}

		fieldKey := ft.Name
		fi := fv.Interface()
		if tag := ft.Tag.Get("gomerr"); tag != "" {
			if tag == "include_type" {
				fieldKey += " (" + fv.Type().String() + ")"
			}
		}
		if s, ok := fi.(fmt.Stringer); ok {
			fi = s.String()
		}
		m[fieldKey] = fi
	}

	if g.attributes != nil && len(g.attributes) > 0 {
		m["_attributes"] = g.attributes
	}

	if wrapped := g.Unwrap(); wrapped != nil {
		var w map[string]any
		if gWrapped, ok := wrapped.(Gomerr); ok {
			w = gWrapped.ToMap()
		} else {
			w = make(map[string]any, 3)
			w["$.errorType"] = reflect.TypeOf(wrapped).String()
			w["_errorString"] = wrapped.Error()
			if marshaled, err := json.Marshal(wrapped); err == nil {
				wm := make(map[string]any)
				if err = json.Unmarshal(marshaled, &wm); err == nil {
					w["_error"] = wm
				}
			}
			w["_stack"] = g.stack // provide a stack for the deepest error (non-Gomerr)
		}
		m["_wrapped"] = w
	} else {
		m["_stack"] = g.stack // provide a stack for the deepest error (Gomerr)
	}

	return m
}

func (g *gomerr) Error() string {
	return g.string(json.Marshal)
}

func (g *gomerr) String() string {
	return g.string(func(v any) ([]byte, error) {
		return json.MarshalIndent(v, "", "  ")
	})
}

func (g *gomerr) string(marshal func(any) ([]byte, error)) string {
	if bytes, err := marshal(g.self.ToMap()); err != nil {
		return "Failed to create gomerr string representation: " + err.Error()
	} else {
		return string(bytes)
	}
}

func (g *gomerr) isFromBuildFunc() bool {
	return true
}
