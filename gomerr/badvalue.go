package gomerr

import "time"

type BadValueType string

const (
	GenericBadValueType BadValueType = "BadValue"
	InvalidValueType    BadValueType = "Invalid"
	MalformedValueType  BadValueType = "Malformed"
	ExpiredValueType    BadValueType = "Expired"

	DefaultReasonAttributeKey    = "Reason"
	DefaultValidAttributeKey     = "Valid"
	DefaultExpiredAtAttributeKey = "ExpiredAt"
)

type BadValueError struct {
	Gomerr
	Type  BadValueType
	Name  string
	Value interface{}
}

func (bve *BadValueError) WithReasons(reasons ...string) *BadValueError {
	for _, reason := range reasons {
		_ = bve.AddAttribute(DefaultReasonAttributeKey, reason)
	}

	return bve
}

func BadValue(badValueType BadValueType, name string, value interface{}) *BadValueError {
	return Build(new(BadValueError), badValueType, name, value).(*BadValueError)
}

func InvalidValue(name string, value interface{}, valid interface{}) *BadValueError {
	return Build(new(BadValueError), InvalidValueType, name, value).AddAttribute(DefaultValidAttributeKey, valid).(*BadValueError)
}

func MalformedValue(name string, value interface{}) *BadValueError {
	return Build(new(BadValueError), MalformedValueType, name, value).(*BadValueError)
}

func ValueExpired(name string, expiredAt time.Time) *BadValueError {
	return Build(new(BadValueError), ExpiredValueType, name, time.Now()).AddAttribute(DefaultExpiredAtAttributeKey, expiredAt).(*BadValueError)
}
