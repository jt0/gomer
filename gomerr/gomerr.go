package gomerr

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"unicode"

	"github.com/jt0/gomer/util"
)

type Culprit string

const (
	Unspecified   Culprit = ""
	Client        Culprit = "Client"
	Internal      Culprit = "Internal"
	Configuration Culprit = "Configuration"
)

type Gomerr interface {
	error
	Unwrap() error

	Cause() error
	Culprit() Culprit
	Notes() []string
	Location() Location
	Attributes() map[string]interface{}

	WithCause(err error) Gomerr
	AddCulprit(source Culprit) Gomerr
	AddNotes(note ...string) Gomerr
	//Retryable(retryable bool) Gomerr
	//IsRetryable() bool
}

var gomerrType = reflect.TypeOf((*Gomerr)(nil)).Elem()

func Build(g Gomerr, attributes ...interface{}) Gomerr {
	build(reflect.ValueOf(g).Elem(), attributes, _newGomerr(3, nil, g))

	return g
}

func BuildWithCause(cause error, g Gomerr, attributes ...interface{}) Gomerr {
	build(reflect.ValueOf(g).Elem(), attributes, _newGomerr(3, cause, g))

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

type BatchError struct {
	Gomerr
	Errors []Gomerr
}

func Batch(errors []Gomerr) Gomerr {
	return Build(&BatchError{}, errors)
}

type Location struct {
	File     string
	Line     int
	Function string
}

func (l Location) equal(l2 Location) bool {
	return l.File == l2.File && l.Line == l2.Line
}

func (l Location) string() string {
	if l.Function == "" {
		return fmt.Sprintf("\n --- at %v:%v ---", l.File, l.Line)
	} else {
		return fmt.Sprintf("\n --- at %v:%v (%v) ---", l.File, l.Line, l.Function)
	}
}

type gomerr struct {
	self     Gomerr
	cause    error
	culprit  Culprit
	notes    []string
	location Location
	//retryable    bool
}

func _newGomerr(stackSkip int, cause error, self Gomerr) *gomerr {
	g := &gomerr{cause: cause}
	if self == nil {
		g.self = g
	} else {
		g.self = self
	}

	if cg, ok := cause.(Gomerr); ok {
		g.culprit = cg.Culprit()
	}

	pc, file, line, ok := runtime.Caller(stackSkip)
	if !ok {
		return g
	}
	g.location = Location{
		File: relative(file),
		Line: line,
	}

	f := runtime.FuncForPC(pc)
	if f == nil {
		return g
	}
	fullFunctionName := f.Name()
	g.location.Function = fullFunctionName[strings.LastIndex(fullFunctionName, "/")+1:]

	return g
}

func relative(file string) string {
	_, thisFile, _, _ := runtime.Caller(0)

	gomerPath := thisFile[:strings.LastIndex(thisFile, "/gomerr/")]
	basePath := gomerPath[:strings.LastIndex(gomerPath, "/")]
	rel, err := filepath.Rel(basePath, file)
	if err != nil {
		return file
	}

	return strings.TrimLeft(rel, "./")
}

func (g *gomerr) Cause() error {
	return g.cause
}

func (g *gomerr) Culprit() Culprit {
	return g.culprit
}

func (g *gomerr) Notes() []string {
	return g.notes
}

func (g *gomerr) Location() Location {
	return g.location
}

func (g *gomerr) Attributes() map[string]interface{} {
	gt := reflect.TypeOf(g.self).Elem()
	gv := reflect.ValueOf(g.self).Elem()
	attributes := make(map[string]interface{}, gt.NumField())

	for i := 0; i < gt.NumField(); i++ {
		ft := gt.Field(i)
		fv := gv.Field(i)

		if ft.Anonymous || unicode.IsLower([]rune(ft.Name)[0]) || fv.IsZero() {
			continue
		}

		// TODO: Access levels - remove here or (perhaps) creator could retract non-accessible data.
		attributes[ft.Name] = fv.Interface()
	}

	return attributes
}

func (g *gomerr) WithCause(err error) Gomerr {
	if g.cause != nil {
		panic("cannot change cause once set")
	}

	g.cause = err
	return g.self
}

func (g *gomerr) AddCulprit(culprit Culprit) Gomerr {
	gw := _newGomerr(2, g.self, nil) // wrap first to get line/file info

	// If the culprit is being added in the same place g is introduced, use g instead of the new one
	if g.location.equal(gw.location) {
		gw = g
	}

	gw.culprit = culprit

	return gw.self
}

func (g *gomerr) AddNotes(notes ...string) Gomerr {
	gw := _newGomerr(2, g.self, nil) // wrap first to get line/file info

	// If the notes are being added in the same place g is introduced, use g instead of the new one
	if g.location.equal(gw.location) {
		gw = g
	}

	gw.notes = append(gw.notes, notes...)

	return gw.self
}

//func (g *gomerr) Retryable(retryable bool) Gomerr {
//	g.retryable = retryable
//	return g
//}

//func (g *gomerr) IsRetryable() bool {
//	return g.retryable
//}

func (g *gomerr) Error() string {
	return fmt.Sprintf("%+s", g)
}

// Implicitly used by errors.Is()/errors.As()
func (g *gomerr) Unwrap() error {
	return g.cause
}

func (g *gomerr) Format(f fmt.State, c rune) {
	var text = format(g)

	formatString := "%"
	// keep the flags recognized by fmt package
	for _, flag := range "-+# 0" {
		if f.Flag(int(flag)) {
			formatString += string(flag)
		}
	}
	if width, has := f.Width(); has {
		formatString += fmt.Sprint(width)
	}
	if precision, has := f.Precision(); has {
		formatString += "."
		formatString += fmt.Sprint(precision)
	}
	formatString += string(c)
	_, _ = fmt.Fprintf(f, formatString, text)
}

func format(g Gomerr) string {
	s := util.UnqualifiedTypeName(g)

	if g.Culprit() == Unspecified {
		s += ":"
	} else {
		s += "(due to " + string(g.Culprit()) + ")"
	}

	if len(g.Notes()) > 0 {
		s += "\n\t" + strings.Join(g.Notes(), "\n\t")
	}

	s += g.Location().string()

	if g.Cause() != nil {
		s += "\nCaused by: "

		if gCause, ok := g.Cause().(Gomerr); ok {
			s += format(gCause)
		} else {
			s += g.Cause().Error()
		}
	}

	return s
}
