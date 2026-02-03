package bind

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

func RegisterStashFieldFunction(name, sourceKey string, include InclusionPredicate) {
	_ = structs.RegisterToolFunction("$_stash."+name, func(sv reflect.Value, _ reflect.Value, tc structs.ToolContext) (interface{}, gomerr.Gomerr) {
		stashData := tc.Get(sourceKey)
		if stashData == nil {
			return nil, nil
		}

		sdv := reflect.ValueOf(stashData)
		// TODO:p3 sources other than maps, such as a struct or slice.
		if sdv.Kind() != reflect.Map {
			return nil, gomerr.Unprocessable("Expected data map", sdv.Type().String())
		}

		out := make(map[string]interface{})
		iter := sdv.MapRange()
		for iter.Next() { // TODO:p1 should only be from tc or can be from struct?
			key := iter.Key().String()
			if include(key, iter.Value(), sv) {
				out[key] = iter.Value().Interface()
			}
		}
		return out, nil
	})
}

func RegisterUnstashFieldFunction(name, destinationKey string, include InclusionPredicate, createIntermediates bool) {
	_ = structs.RegisterToolFunction("$_unstash."+name, func(sv reflect.Value, fv reflect.Value, tc structs.ToolContext) (interface{}, gomerr.Gomerr) {
		if !fv.IsValid() {
			return nil, nil // TODO: return an error?
		} // TODO:p3 handle fv as a ptr type

		destination, ok := tc.Descend(destinationKey, createIntermediates)
		if !ok {
			return nil, nil
		}

		// TODO:p3 destinations to types other than maps, such as a struct or slice.
		iter := fv.MapRange()
		for iter.Next() {
			// TODO:p1 We need a way to encode names that have a '.' in them, since that's the delimiter we use for
			//         describing a path and location.
			key := iter.Key().String()
			stashValue := iter.Value()
			stashValueType := stashValue.Type()
			switch stashValueType.Kind() {
			case reflect.Struct:
				itemDestination, itemOk := destination.Descend(key, createIntermediates)
				if !itemOk {
					continue
				}
				for i := 0; i < stashValueType.NumField(); i++ {
					tf := stashValueType.Field(i)
					vf := stashValue.Field(i)
					if include(tf.Name, vf, stashValue) {
						// m := make(map[string]interface{})
						// for
						itemDestination.Put(tf.Name, vf.Interface())
					}
				}
			case reflect.Map:
				fmt.Printf("Unstash map not yet supported. Key = %s, Value =\n%v\n", key, stashValue.Interface())
			case reflect.Slice, reflect.Array:
				fmt.Printf("Unstash slice/array not yet supported. Key = %s, Value =\n%v\n", key, stashValue.Interface())
			case reflect.Ptr:
				fmt.Printf("Unstash pointer not yet supported. Key = %s, Value =\n%v", key, stashValue.Elem().Interface())
			default:
				destination.Put(key, stashValue.Interface())
			}
		}

		return nil, nil
	})
}

// InclusionPredicate and helper functions

type InclusionPredicate func(key string, value interface{}, sv reflect.Value) bool

func IsField(key string, _ interface{}, sv reflect.Value) bool {
	return sv.FieldByName(strings.Title(key)).IsValid()
}

func IsNotField(key string, _ interface{}, sv reflect.Value) bool {
	return !sv.FieldByName(strings.Title(key)).IsValid()
}

func All(_ string, _ interface{}, _ reflect.Value) bool {
	return true
}

func NameMatches(names ...string) InclusionPredicate {
	nm := make(map[string]bool, len(names))
	for _, name := range names {
		nm[name] = true
	}
	return func(key string, _ interface{}, _ reflect.Value) bool {
		return nm[key]
	}
}

func IfAll(predicates ...InclusionPredicate) InclusionPredicate {
	return func(key string, value interface{}, sv reflect.Value) bool {
		for _, p := range predicates {
			if !p(key, value, sv) {
				return false
			}
		}
		return true
	}
}

func IfAny(predicates ...InclusionPredicate) InclusionPredicate {
	return func(key string, value interface{}, sv reflect.Value) bool {
		for _, p := range predicates {
			if p(key, value, sv) {
				return true
			}
		}
		return false
	}
}

// UnstashConflictResolver and helper functions

type UnstashConflictResolver func(stashed, destination interface{}) interface{}

func UseStashed(stashed, _ interface{}) interface{} {
	return stashed
}

// func UseDestination(_, destination interface{}) interface{} {
// 	return destination
// }

// type MergeType int
// const (
// 	Stashed MergeType = 1 << iota
// 	Destination
// 	Select
// 	// Combine // based on type. Examples: 1 combine 2 -> 3; "1" combine "2" -> "12"
// 	// LogDropped
// )

// TODO: needs to be merged into unstash function above and drop

func MergeStashed(include InclusionPredicate) UnstashConflictResolver {
	// TODO: revisit name; revisit resolver options
	return func(stashed, destination interface{}) interface{} {

		return nil // FIXME: dummy val
	}
}

// func MergeSlice(mergeType MergeType) UnstashConflictResolver {
// }
