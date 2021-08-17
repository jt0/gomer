package assert

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/jt0/gomer/gomerr"
)

func Assert(tb testing.TB, condition bool, msgAndArgs ...interface{}) {
	var msg string
	var msgArgs []interface{}
	if len(msg) > 0 {
		msg = msgAndArgs[0].(string)
		if len(msg) > 1 {
			msgArgs = msgAndArgs[1:]
		}
	}

	if !condition {
		fmt.Printf("Assert failed. "+msg+"\n", msgArgs...)
		tb.FailNow()
	}
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

func Error(tb testing.TB, err error, msgAndArgs ...interface{}) {
	var msg string
	var msgArgs []interface{}
	if len(msg) > 0 {
		msg = msgAndArgs[0].(string)
		if len(msg) > 1 {
			msgArgs = msgAndArgs[1:]
		}
	}

	if err == nil {
		fmt.Printf("Expected error to be non-nil. "+msg+"\n", msgArgs...)
		tb.FailNow()
	}
}

func ErrorType(tb testing.TB, err error, target error, msgAndArgs ...interface{}) {
	var msg string
	var msgArgs []interface{}
	if len(msg) > 0 {
		msg = msgAndArgs[0].(string)
		if len(msg) > 1 {
			msgArgs = msgAndArgs[1:]
		}
	}

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
		fmt.Printf("Wrong error type. Expected 'errors.Is(%s, %s)' to succeed. "+msg+"\n", append([]interface{}{reflect.TypeOf(err).String(), reflect.TypeOf(target).String()}, msgArgs...)...)
		fmt.Printf("Received: " + errString(err) + "\n")
		tb.FailNow()
	}
}

func Equals(tb testing.TB, expected, actual interface{}, msgAndArgs ...interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		var msg string
		var msgArgs []interface{}
		if len(msg) > 0 {
			msg = msgAndArgs[0].(string)
			if len(msg) > 1 {
				msgArgs = msgAndArgs[1:]
			}
		}
		fmt.Printf("Failed equality check: "+msg+"\n\tExpected: %#v\n\tActual:   %#v\n", append(append([]interface{}{}, msgArgs...), expected, actual)...)
		tb.FailNow()
	}
}

func NotEquals(tb testing.TB, expected, actual interface{}, msgAndArgs ...interface{}) {
	if reflect.DeepEqual(expected, actual) {
		var msg string
		var msgArgs []interface{}
		if len(msg) > 0 {
			msg = msgAndArgs[0].(string)
			if len(msg) > 1 {
				msgArgs = msgAndArgs[1:]
			}
		}
		fmt.Printf("Failed non-equality check: "+msg+"\n\tExpected: %#v\n Actual: %#v\n", append(append([]interface{}{}, msgArgs...), expected, actual)...)
		tb.FailNow()
	}
}

func errString(err error) string {
	if ge, ok := err.(gomerr.Gomerr); ok {
		return "\n" + ge.String()
	}
	return err.Error()
}
