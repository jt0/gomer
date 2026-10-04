package constraint

import (
	"fmt"
	"reflect"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

// UseBracketsForContainedTargets specifies whether the generated target value is in 'json' or 'Gomer' format. The
// former is consistent w/ the representation emitted by the JsonSchema validation. The latter conforms to the fields
// directives format. Examples of target values when UseBracketsForContainedTargets is true or false:
//
//	type SomeStruct struct {
//	  Foo struct {
//	    S []Bar          // true: Foo.S[3]; false: Foo.S.3
//	    M map[string]Bar // true: Foo.M[cat]; false: Foo.M.cat
//	  }
//	}
//
// Note that when UseBracketsForContainedTargets is false, both the key and value will have the same target (e.g.
// Foo.M.Cat). If the value is true, the key value will be Foo.M.cat and the latter will be Foo.M[cat].
var UseBracketsForContainedTargets = false

func Struct(validationTool *structs.Tool) Constraint {
	return New("struct", nil, func(toTest any) gomerr.Gomerr {
		if _, ok := flect.ReadableIndirectValue(toTest); !ok {
			return nil
		}

		// TODO:p1 support scope
		// TODO:p1 should need to have validationTool?
		return structs.ApplyTools(toTest, structs.EnsureContext(), validationTool)
	})
}

func Union(validationTool *structs.Tool) Constraint {
	return New("union", "one", func(toTest any) gomerr.Gomerr {
		v, ok := flect.ReadableIndirectValue(toTest)
		if !ok {
			return nil
		}

		var found reflect.Value
		for _, fv := range v.Fields() {
			if !fv.IsValid() || fv.IsZero() {
				continue
			} else if found.IsValid() {
				return NotSatisfied("multiple")
			}
			found = fv
		}
		if !found.IsValid() {
			return NotSatisfied("none")
		}
		if reflect.Indirect(found).Kind() == reflect.Struct {
			toTest = found
		}
		// TODO:p1 support scope
		return structs.ApplyTools(toTest, structs.EnsureContext(), validationTool)
	})
}

func MapKeys(keyConstraint Constraint) Constraint {
	return Map(keyConstraint, nil)
}

func MapValues(valueConstraint Constraint) Constraint {
	return Map(nil, valueConstraint)
}

func Map(keyConstraint Constraint, valueConstraint Constraint) Constraint {
	var kvConstraints []Constraint
	if keyConstraint != nil {
		kvConstraints = append(kvConstraints, keyConstraint)
	}
	if valueConstraint != nil {
		kvConstraints = append(kvConstraints, valueConstraint)
	}
	var cp any
	if len(kvConstraints) == 1 {
		cp = kvConstraints[0]
	} else {
		cp = kvConstraints
	}
	return dynamicIfNeeded(New("map", cp, func(toTest any) gomerr.Gomerr {
		ttv, ok := flect.ReadableIndirectValue(toTest)
		if !ok {
			return nil
		}

		if ttv.Kind() != reflect.Map {
			return gomerr.Unprocessable("test value is not a map", toTest)
		}

		iter := ttv.MapRange()
		var eb gomerr.ErrorBatch
		for iter.Next() {
			ki := iter.Key().Interface()
			target := fmt.Sprintf("%v", ki)
			if keyConstraint != nil {
				eb.Capture(keyConstraint.Validate(target, ki))
			}
			if valueConstraint != nil {
				if UseBracketsForContainedTargets {
					target = "[" + target + "]"
				}
				eb.Capture(valueConstraint.Validate(target, iter.Value().Interface()))
			}
		}
		return eb.GomerrOrNil()
	}), keyConstraint, valueConstraint)
}

type Entry struct {
	Key   any
	Value any
}

func Entries(entryConstraint Constraint) Constraint {
	return dynamicIfNeeded(New("entries", entryConstraint, func(toTest any) gomerr.Gomerr {
		ttv, ok := flect.ReadableIndirectValue(toTest)
		if !ok {
			return nil
		}

		if ttv.Kind() != reflect.Map {
			return gomerr.Unprocessable("test value is not a map", toTest)
		}

		iter := ttv.MapRange()
		var eb gomerr.ErrorBatch
		for iter.Next() {
			var target string
			if UseBracketsForContainedTargets {
				target = fmt.Sprintf("[%v]", iter.Key().Interface())
			} else {
				target = fmt.Sprintf("%v", iter.Key().Interface())
			}
			eb.Capture(entryConstraint.Validate(target, Entry{iter.Key().Interface(), iter.Value().Interface()}))
		}
		return eb.GomerrOrNil()
	}), entryConstraint)
}

func Elements(elementsConstraint Constraint) Constraint {
	return dynamicIfNeeded(New("elements", elementsConstraint, func(toTest any) gomerr.Gomerr {
		ttv, ok := flect.ReadableIndirectValue(toTest)
		if !ok {
			return nil
		}

		if ttv.Kind() != reflect.Slice && ttv.Kind() != reflect.Array {
			return gomerr.Unprocessable("input is not a slice or array", toTest)
		}

		var eb gomerr.ErrorBatch
		for i := 0; i < ttv.Len(); i++ {
			var target string
			if UseBracketsForContainedTargets {
				target = fmt.Sprintf("[%d]", i)
			} else {
				target = fmt.Sprintf("%d", i)
			}
			eb.Capture(elementsConstraint.Validate(target, ttv.Index(i).Interface()))
		}
		return eb.GomerrOrNil()
	}), elementsConstraint)
}

func TypeOf(value any) Constraint {
	t, ok := value.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(value)
	}

	return New("typeOf", t.String(), func(toTest any) gomerr.Gomerr {
		if reflect.TypeOf(toTest) != t {
			return NotSatisfied(t.String())
		}
		return nil
	})
}
