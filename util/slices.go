package util

import (
	"fmt"
)

func InsertStringAtIndex(slice []string, value string, index int) []string {
	if slice == nil || cap(slice) == 0 {
		slice = make([]string, 0, index+1)
	}

	lenKeyFields := len(slice)
	capKeyFields := cap(slice)
	if index < lenKeyFields {
		if slice[index] != "" {
			panic(fmt.Sprintf("already found value '%s' at index %d", slice[index], index))
		}
	} else if index < capKeyFields {
		slice = slice[0 : index+1]
	} else {
		slice = append(slice, make([]string, index+1-capKeyFields)...)
	}

	slice[index] = value

	return slice
}
