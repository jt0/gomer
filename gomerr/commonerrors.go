package gomerr

import (
	"time"

	"github.com/jt0/gomer/constraint"
)

type NotFoundError struct {
	Gomerr
	Type string
	Id   string
}

type BadValueError struct {
	Gomerr
	What     string
	Actual   interface{}
	Expected constraint.Constraint
}

type MarshalError struct {
	Gomerr
	ToMarshal interface{}
}

type UnmarshalError struct {
	Gomerr
	ToUnmarshal interface{}
	Target      interface{}
}

type TokenExpiredError struct {
	Gomerr
	ExpiredAt  time.Time
	ServerTime time.Time
}

type DependencyError struct {
	Gomerr
	Service string
}

type UnsupportedError struct {
	Gomerr
	CorrectiveAction string
}

type PanicError struct {
	Gomerr
	Recover interface{}
}

func NotFound(type_ string, id string) Gomerr {
	return Build(new(NotFoundError), type_, id)
}

func BadValue(what string, actual interface{}, expected constraint.Constraint) Gomerr {
	return Build(new(BadValueError), what, actual, expected)
}

func Marshal(cause error, toMarshal interface{}) Gomerr {
	return BuildWithCause(cause, new(MarshalError), toMarshal)
}

func Unmarshal(cause error, toUnmarshal interface{}, target interface{}) Gomerr {
	return BuildWithCause(cause, new(UnmarshalError), toUnmarshal, target)
}

func TokenExpired(expiredAt time.Time) Gomerr {
	return Build(new(TokenExpiredError), expiredAt.UTC(), time.Now().UTC())
}

func Dependency(cause error, input interface{}) Gomerr {
	return BuildWithCause(cause, new(DependencyError), input)
}

func Unsupported(correctiveAction string) Gomerr {
	return Build(new(UnsupportedError), correctiveAction)
}

func Panic(recover interface{}) Gomerr {
	return Build(new(PanicError), recover)
}
