package constraint

import (
	"fmt"
	"os"
)

type Constrainer struct {
	test    func(value interface{}) bool
	details map[string]interface{}
}

func (c Constrainer) Test(value interface{}) bool {
	return c.test(value)
}

func (c Constrainer) Details() map[string]interface{} {
	return c.details
}

func (c Constrainer) setDetails(detailParts ...interface{}) Constrainer {
	numParts := len(detailParts)

	if numParts%2 != 0 {
		_, _ = fmt.Fprintf(os.Stderr, "Odd number of elements. Expecting key/value pairs so ignoring last one: %v\n", detailParts[len(detailParts)-1])
	}

	if c.details == nil {
		c.details = make(map[string]interface{}, numParts/2)
	}
	for i := 0; i < numParts; i += 2 {
		key, ok := detailParts[i].(string)
		if !ok {
			_, _ = fmt.Fprintf(os.Stderr, "Skipping pair with non-string key value: %v\n", detailParts[i])
		} else {
			c.details[key] = detailParts[i+1]
		}
	}

	return c
}
