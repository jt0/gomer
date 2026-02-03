package assert

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/jt0/gomer/gomerr"
)

func Assert(tb testing.TB, condition bool, msgAndArgs ...any) {
	if !condition {
		msg, msgArgs := split(msgAndArgs)
		fmt.Printf("Assert failed. "+msg+"\n", msgArgs...)
		tb.FailNow()
	}
}

func True(tb testing.TB, condition bool, msgAndArgs ...any) {

}

func Success(tb testing.TB, err error) {
	if err != nil {
		fmt.Printf("Expected success, but got: %s\n", errString(err))
		tb.FailNow()
	}
}

func Fail(tb testing.TB, err error) {
	if err == nil {
		fmt.Println("Expected an error, but err is nil")
		tb.FailNow()
	}
}

func Error(tb testing.TB, err error, msgAndArgs ...any) {
	if err == nil {
		msg, msgArgs := split(msgAndArgs)
		fmt.Printf("Expected error to be non-nil. "+msg+"\n", msgArgs...)
		tb.FailNow()
	}
}

func ErrorType(tb testing.TB, err error, target error, msgAndArgs ...any) {
	msg, msgArgs := split(msgAndArgs)

	if err == nil {
		fmt.Printf("Expected an error. "+msg+"\n", msgArgs...)
		tb.FailNow()
	}

	isTargetType := errors.Is(err, target)

	// If a batch error, validate each one in turn. TODO:p3 support an array of target error types
	if be, ok := err.(*gomerr.BatchError); ok && !isTargetType {
		for _, ge := range be.Errors() {
			ErrorType(tb, ge, target, msgAndArgs)
		}
		return
	}

	if !isTargetType {
		fmt.Printf("Wrong error type. Expected 'errors.Is(%s, %s)' to succeed. "+msg+"\n", append([]any{reflect.TypeOf(err).String(), reflect.TypeOf(target).String()}, msgArgs...)...)
		fmt.Printf("Received: " + errString(err) + "\n")
		tb.FailNow()
	}
}

func Equals(tb testing.TB, expected, actual any, msgAndArgs ...any) {
	if !reflect.DeepEqual(expected, actual) {
		msg, msgArgs := split(msgAndArgs)
		fmt.Printf("Failed equality check: "+msg+"\n\tExpected: %#v\n\tActual:   %#v\n", append(append([]any{}, msgArgs...), expected, actual)...)
		tb.FailNow()
	}
}

func NotEquals(tb testing.TB, expected, actual any, msgAndArgs ...any) {
	if reflect.DeepEqual(expected, actual) {
		msg, msgArgs := split(msgAndArgs)
		fmt.Printf("Failed non-equality check: "+msg+"\n\tExpected: %#v\n Actual: %#v\n", append(append([]any{}, msgArgs...), expected, actual)...)
		tb.FailNow()
	}
}

func Nil(tb testing.TB, v any, msgAndArgs ...any) {
	switch vv := reflect.ValueOf(v); vv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if !vv.IsNil() {
			msg, msgArgs := split(msgAndArgs)
			fmt.Printf("Expected value to be nil. "+msg+"\n", msgArgs...)
			tb.FailNow()
		}
	default:
		msg, msgArgs := split(msgAndArgs)
		fmt.Printf("Value is not a nilable type. "+msg+"\n", msgArgs...)
		tb.FailNow()
	}
}

func NotNil(tb testing.TB, v any, msgAndArgs ...any) {
	switch vv := reflect.ValueOf(v); vv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if vv.IsNil() {
			msg, msgArgs := split(msgAndArgs)
			fmt.Printf("Expected value to be non-nil. "+msg+"\n", msgArgs...)
			tb.FailNow()
		}
	default:
		msg, msgArgs := split(msgAndArgs)
		fmt.Printf("Value is not a nilable type. "+msg+"\n", msgArgs...)
		tb.FailNow()
	}
}

func errString(err error) string {
	if ge, ok := err.(gomerr.Gomerr); ok {
		return "\n" + ge.String()
	}
	return err.Error()
}

func split(msgAndArgs []any) (string, []any) {
	var msg string
	var msgArgs []any
	if len(msg) > 0 {
		msg = msgAndArgs[0].(string)
		if len(msg) > 1 {
			msgArgs = msgAndArgs[1:]
		}
	}
	return msg, msgArgs
}
