package id

import (
	"fmt"
	"strconv"
)

type Uint uint

func (u Uint) Format(f fmt.State, c rune) {
	if width, ok := f.Width(); ok {
		_, _ = fmt.Fprintf(f, "%0*d", width, uint(u))
	} else {
		_, _ = fmt.Fprint(f, uint(u))
	}
}

func (u Uint) String() string {
	return strconv.FormatUint(uint64(u), 10)
}
