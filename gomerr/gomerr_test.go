package gomerr

import (
	"errors"
	"reflect"
	"testing"
)

func TestErrorAs(t *testing.T) {
	var err error

	err = MalformedValue("foo", "bar")
	if got := ErrorAs[*BadValueError](err); got == nil && !reflect.DeepEqual(got, err) {
		t.Errorf("ErrorAs() = %v, want %v", got, err)
	}

	if got := ErrorAs[*BadValueError](Internal("wrapper").Wrap(err)); got == nil && !reflect.DeepEqual(got, err) {
		t.Errorf("ErrorAs() = %v, want %v", got, err)
	}

	err = errors.New("malformed")
	if got := ErrorAs[*BadValueError](err); got != nil {
		t.Errorf("expected nil, go %v", got)
	}

	// Not that useful since sentinel errors don't commonly provide additional behavior. Further, most Go standard
	// sentinel errors (e.g. context.DeadlineExceeded, net.ErrClosed) don't export their type. Likely better to use:
	//
	//     !errors.Is(Internal("wrapper").Wrap(ErrSentinel), ErrSentinel)
	if got := ErrorAs[sentinel](Internal("wrapper").Wrap(ErrSentinel)); !reflect.DeepEqual(got, ErrSentinel) {
		t.Errorf("ErrorAs() = %v, want %v", got, ErrSentinel)
	}
}

type sentinel struct{}

var ErrSentinel error = sentinel{}

func (sentinel) Error() string { return "sentinel" }
