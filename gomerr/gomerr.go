package gomerr

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"unicode"
)

type Gomerr interface {
	Wrap(err error) Gomerr
	Attribute(key string) (value interface{}, ok bool)
	AddAttribute(key string, value interface{}) Gomerr
	AddAttributes(keysAndValues ...interface{}) Gomerr
	WithAttributes(attributes map[string]interface{}) Gomerr

	Is(error) bool
	Unwrap() error
	Attributes() map[string]interface{}
	Stack() []string
	ToMap() map[string]interface{}
	Error() string
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
	attributes map[string]interface{}
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
		if strings.HasPrefix(frame.Function, "runtime.") {
			stack = stack[:i]
			break
		}
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

func (g *gomerr) Attribute(key string) (value interface{}, ok bool) {
	value, ok = g.attributes[key]
	return
}

func (g *gomerr) AddAttribute(key string, value interface{}) Gomerr {
	// gw := newGomerr(2, g.self) // wrap first to get line/file info
	//
	// // If the notes are being added in the same place g is introduced, use g instead of the new one
	// if g.stack[0].Line == gw.stack[0].Line && g.stack[0].File == gw.stack[0].File {
	// 	gw = g
	// }

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

	return g.self
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
	if existing, exists := (*m)[key]; exists {
		valueSlice, ok := value.([]interface{})
		if !ok {
			valueSlice = []interface{}{existing, value}
		} else {
			valueSlice = append(valueSlice, value)
		}

		(*m)[key] = valueSlice
	} else {
		(*m)[key] = value
	}
}

func (g *gomerr) Is(err error) bool {
	return reflect.TypeOf(g.self) == reflect.TypeOf(err)
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
		if ft.Anonymous || unicode.IsLower([]rune(ft.Name)[0]) || !fv.IsValid() || fv.IsZero() {
			continue
		}

		fieldKey := ft.Name
		fi := fv.Interface()
		if tag := ft.Tag.Get("gomerr"); tag != "" {
			if tag == "include_type" {
				fieldKey += " (" + reflect.TypeOf(fi).String() + ")"
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
		var val interface{}
		if gWrapped, ok := wrapped.(Gomerr); ok {
			val = gWrapped.ToMap()
		} else {
			val = wrapped.Error()
		}
		m[reflect.TypeOf(wrapped).String()] = val
	}

	return m
}

func (g *gomerr) Error() string {
	return g.string(json.Marshal)
}

func (g *gomerr) String() string {
	return g.string(func(v interface{}) ([]byte, error) {
		return json.MarshalIndent(v, "", "  ")
	})
}

func (g *gomerr) string(marshal func(interface{}) ([]byte, error)) string {
	var innermost Gomerr = g
	for candidate, ok := innermost.Unwrap().(Gomerr); ok; candidate, ok = innermost.Unwrap().(Gomerr) {
		innermost = candidate
	}

	asMap := map[string]interface{}{
		reflect.TypeOf(g.self).String(): g.self.ToMap(),
		"stack":                         innermost.Stack(),
	}

	if bytes, err := marshal(asMap); err != nil {
		return "Failed to create gomerr string representation: " + err.Error()
	} else {
		return string(bytes)
	}
}
