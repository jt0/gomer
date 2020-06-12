package util

import (
	"fmt"
	"reflect"
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

func EmptySliceForType(t reflect.Type) interface{} {
	slice := reflect.MakeSlice(reflect.SliceOf(t), 0, 0)

	// Create a pointer to a slice value and set it to the slice
	slicePtr := reflect.New(slice.Type())
	slicePtr.Elem().Set(slice)

	return slicePtr.Interface()
}
