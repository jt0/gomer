package gomerr

import "time"

type BadValueType string

const (
	ExpiredValueType    BadValueType = "Expired"
	GenericBadValueType BadValueType = "BadValue"
	InvalidValueType    BadValueType = "Invalid"
	MalformedValueType  BadValueType = "Malformed"

	reasonAttributeKey   = "Reason"
	expectedAttributeKey = "Expected"
)

type BadValueError struct {
	Gomerr
	Type  BadValueType
	Name  string
	Value interface{}
}

func BadValue(badValueType BadValueType, name string, value interface{}) *BadValueError {
	return Build(new(BadValueError), badValueType, name, value).(*BadValueError)
}

func InvalidValue(name string, value interface{}, expected interface{}) *BadValueError {
	return Build(new(BadValueError), InvalidValueType, name, value).AddAttributes(expectedAttributeKey, expected).(*BadValueError)
}

func MalformedValue(name string, value interface{}) *BadValueError {
	return Build(new(BadValueError), MalformedValueType, name, value).(*BadValueError)
}

func ValueExpired(name string, expiredAt time.Time) *BadValueError {
	return Build(new(BadValueError), ExpiredValueType, name, expiredAt).(*BadValueError)
}

func (bve *BadValueError) WithReason(reason string) *BadValueError {
	return bve.AddAttribute(reasonAttributeKey, reason).(*BadValueError)
}
