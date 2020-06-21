package gomerr

import (
	"time"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/util"
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

type LimitExceededError struct {
	Gomerr
	LimitType string
	Limit     util.Amount
	Current   util.Amount
	Attempted util.Amount
}

type ConflictError struct {
	Gomerr
	Conflict string
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

type InternalError struct {
	Gomerr
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

func LimitExceeded(limitType string, limit util.Amount, current util.Amount, attempted util.Amount) Gomerr {
	return Build(new(LimitExceededError), limitType, limit, current, attempted)
}

func Conflict(conflict string) Gomerr {
	return Build(new(ConflictError), conflict)
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

func InternalServer(cause error) Gomerr {
	// XXX: remove cause
	return BuildWithCause(cause, new(InternalError))
}

func Panic(recover interface{}) Gomerr {
	return Build(new(PanicError), recover)
}
