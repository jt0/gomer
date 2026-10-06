package constraint

import (
	"github.com/jt0/gomer/gomerr"
)

// NotSatisfiedError reports a value that failed a check. Node is the failing node, or the
// zero node when the failure has none, and Expected a description of what would have
// satisfied it, such as "len(1,64)".
type NotSatisfiedError struct {
	gomerr.Gomerr
	ToTest   any
	Target   string
	Node     Node
	Expected string
}

func NotSatisfied(toTest any) *NotSatisfiedError {
	return gomerr.BuildWithoutStack(new(NotSatisfiedError), toTest).(*NotSatisfiedError)
}
