package assert

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/jt0/gomer/gomerr"
)

func Assert(tb testing.TB, condition bool, msg string, msgArgs ...interface{}) {
	if !condition {
		fmt.Printf("Assert failed: "+msg+"\n", msgArgs...)
		printStack()
		tb.FailNow()
	}
}

func Success(tb testing.TB, err error) {
	if err != nil {
		fmt.Printf("Expected success, but got: %s\n", err.Error())
		if _, ok := err.(gomerr.Gomerr); !ok {
			printStack() // Gomerr already includes stack info
		}
		tb.FailNow()
	}
}

func Error(tb testing.TB, err error, msg string, msgArgs ...interface{}) {
	if err == nil {
		fmt.Printf("Expected error to be non-nil: "+msg+"\n", msgArgs...)
		printStack()
		tb.FailNow()
	}
}

func ErrorType(tb testing.TB, actual error, expected error, msg string, msgArgs ...interface{}) {
	if actual == nil {
		fmt.Printf("Expected an error: "+msg+"\n", msgArgs...)
		printStack()
		tb.FailNow()
	}

	if !errors.Is(actual, expected) {
		fmt.Printf("Wrong error type. Expected 'errors.Is(actual, %s)' to succeed: "+msg+"\n", append([]interface{}{reflect.TypeOf(expected).String()}, msgArgs...)...)
		fmt.Printf("Actual: " + actual.Error() + "\n")
		if _, ok := actual.(gomerr.Gomerr); !ok {
			printStack() // Gomerr already includes stack info
		}
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
		fmt.Printf("Failed equality check: "+msg+"\n\tExpected: %#v\n Actual: %#v\n", append(append([]interface{}{}, msgArgs...), expected, actual)...)
		printStack()
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
		printStack()
		tb.FailNow()
	}
}

func printStack() {
	callers := make([]uintptr, 30)
	depth := runtime.Callers(3, callers)
	callers = callers[:depth]

	stack := make([]string, depth)
	frames := runtime.CallersFrames(callers)
	fmt.Println("\nStack:")
	for i := 0; i < depth; i++ {
		frame, _ := frames.Next()
		if strings.HasPrefix(frame.Function, "runtime.") {
			stack = stack[:i]
			break
		}
		function := frame.Function[strings.LastIndexByte(frame.Function, '/')+1:]
		fmt.Printf("%s -- %s:%d\n", function, frame.File, frame.Line)
	}
}
