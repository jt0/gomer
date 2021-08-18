package constraint

import (
	"fmt"
	"reflect"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

// UseBracketsForContainedTargets specifies whether the generated target value is in 'json' or 'Gomer' format. The
// former is consistent w/ the representation emitted by the JsonSchema validation. The latter conforms to the fields
// directives format. Examples of target values when UseBracketsForContainedTargets is true or false:
//
//  type SomeStruct struct {
//    Foo struct {
//      S []Bar          // true: Foo.S[3]; false: Foo.S.3
//      M map[string]Bar // true: Foo.M[cat]; false: Foo.M.cat
//    }
//  }
//
// Note that when UseBracketsForContainedTargets is false, both the key and value will have the same target (e.g.
// Foo.M.Cat). If the value is true, the key value will be Foo.M.cat and the latter will be Foo.M[cat].
var UseBracketsForContainedTargets = false

func Struct(validationToolName string) Constraint {
	return New("Struct", nil, func(toTest interface{}) gomerr.Gomerr {
		ttv, isNil := indirectValueOf(toTest)
		if isNil {
			return nil
		}

		if ttv.Kind() != reflect.Struct {
			return gomerr.Unprocessable("Test value is not a struct", toTest)
		}

		fs, ge := fields.Get(ttv.Type())
		if ge != nil {
			return gomerr.Unprocessable("Unable to get fields for test value", toTest).Wrap(ge)
		}

		if ge = fs.ApplyTools(ttv, fields.Application{ToolName: validationToolName}); ge != nil {
			return ge
		}

		return nil
	})
}

func MapKeys(keyConstraint Constraint) Constraint {
	return Map(keyConstraint, nil)
}

func MapValues(valueConstraint Constraint) Constraint {
	return Map(nil, valueConstraint)
}

func Map(keyConstraint Constraint, valueConstraint Constraint) Constraint {
	return dynamicIfNeeded(New("Map", nil, func(toTest interface{}) gomerr.Gomerr {
		ttv, isNil := indirectValueOf(toTest)
		if isNil {
			return nil
		}

		if ttv.Kind() != reflect.Map {
			return gomerr.Unprocessable("Test value is not a map", toTest)
		}

		iter := ttv.MapRange()
		var errors []gomerr.Gomerr
		for iter.Next() {
			ki := iter.Key().Interface()
			target := fmt.Sprintf("%v", ki)
			if keyConstraint != nil {
				if ge := keyConstraint.Validate(target, ki); ge != nil {
					errors = append(errors, ge)
				}
			}
			if valueConstraint != nil {
				if UseBracketsForContainedTargets {
					target = "[" + target + "]"
				}
				if ge := valueConstraint.Validate(target, iter.Value().Interface()); ge != nil {
					errors = append(errors, ge)
				}
			}
		}

		return gomerr.Batcher(errors)
	}), keyConstraint, valueConstraint)
}

type Entry struct {
	Key   interface{}
	Value interface{}
}

func Entries(entryConstraint Constraint) Constraint {
	return dynamicIfNeeded(New("Entries", nil, func(toTest interface{}) gomerr.Gomerr {
		ttv, isNil := indirectValueOf(toTest)
		if isNil {
			return nil
		}

		if ttv.Kind() != reflect.Map {
			return gomerr.Unprocessable("Test value is not a map", toTest)
		}

		iter := ttv.MapRange()
		var errors []gomerr.Gomerr
		for iter.Next() {
			var target string
			if UseBracketsForContainedTargets {
				target = fmt.Sprintf("[%v]", iter.Key().Interface())
			} else {
				target = fmt.Sprintf("%v", iter.Key().Interface())
			}
			if ge := entryConstraint.Validate(target, Entry{iter.Key().Interface(), iter.Value().Interface()}); ge != nil {
				errors = append(errors, ge)
			}
		}

		return gomerr.Batcher(errors)
	}), entryConstraint)
}

func Elements(constraint Constraint) Constraint {
	return dynamicIfNeeded(New("Elements", nil, func(toTest interface{}) gomerr.Gomerr {
		ttv, isNil := indirectValueOf(toTest)
		if isNil {
			return nil
		}

		if ttv.Kind() != reflect.Slice && ttv.Kind() != reflect.Array {
			return gomerr.Unprocessable("Input is not a slice or array", toTest)
		}

		var errors []gomerr.Gomerr
		for i := 0; i < ttv.Len(); i++ {
			var target string
			if UseBracketsForContainedTargets {
				target = fmt.Sprintf("[%d]", i)
			} else {
				target = fmt.Sprintf("%d", i)
			}
			if ge := constraint.Validate(target, ttv.Index(i).Interface()); ge != nil {
				errors = append(errors, ge)
			}
		}

		return gomerr.Batcher(errors)
	}), constraint)
}

func TypeOf(value interface{}) Constraint {
	t, ok := value.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(value)
	}

	return New("TypeOf", t.String(), func(toTest interface{}) gomerr.Gomerr {
		if reflect.TypeOf(toTest) != t {
			return NotSatisfied(toTest)
		}
		return nil
	})
}
