package dynamodb

import (
	"reflect"
	"slices"

	"github.com/jt0/gomer/gomerr"
)

// mergeFields applies field-level changes from the update value (uv) into the persisted value (pv). The merge
// semantics vary by kind:
//
//   - Struct (non-pointer): always recurse.
//   - Equal values: nothing to do, continue.
//   - Pointer: nil uv means no change. Non-nil uv into nil pv sets pv to uv. A zero *uv explicitly clears pv.
//     Otherwise, recurse into structs and maps or overwrite for scalars.
//   - Map: nil means "no change". Non-nil empty map clears pv's map. Per-key: zero value deletes,
//     equal values are skipped, otherwise upsert.
//   - Slice: nil means "no change". Non-nil replaces pv's slice entirely (no per-element merge).
//   - Scalars: zero value means "not provided" — cannot intentionally set a scalar to its zero value.
//     Use a pointer type if zero is a valid update value.
//
// Constraint tracking (pt) is only applied at the top level; recursive calls pass nil.
func (t *table) mergeFields(pv, uv reflect.Value, pt *persistableType) (bool, gomerr.Gomerr) {
	validateConstraints := false

	var keyFields []*keyField
	if pt != nil {
		keyFields = t.index.pk.keyFieldsByPersistable[pt.name]
		if t.index.sk != nil {
			keyFields = append(keyFields, t.index.sk.keyFieldsByPersistable[pt.name]...)
		}
	}

	for i := 0; i < uv.NumField(); i++ {
		pField := pv.Field(i)
		uField := uv.Field(i)

		if !pField.CanSet() {
			continue
		} else if uField.Kind() == reflect.Struct && !uField.Type().AssignableTo(timeType) {
			_, _ = t.mergeFields(pField, uField, nil)
			continue
		} else if reflect.DeepEqual(uField.Interface(), pField.Interface()) {
			continue
		}

		fieldName := uv.Type().Field(i).Name
		if pt != nil && slices.ContainsFunc(keyFields, func(kf *keyField) bool { return kf.name == fieldName }) {
			return false, gomerr.Unprocessable("partition or sort key fields cannot be modified", fieldName)
		}

		if uField.Kind() == reflect.Ptr {
			if uField.IsNil() {
				continue
			} else if pField.IsNil() {
				if uField.Elem().IsZero() {
					continue
				}
				pField.Set(uField)
			} else {
				switch uField.Elem().Kind() {
				case reflect.Struct:
					if uField.Elem().IsZero() {
						pField.Set(reflect.Zero(pField.Type()))
						continue
					} else if !uField.Elem().Type().AssignableTo(timeType) {
						_, _ = t.mergeFields(pField.Elem(), uField.Elem(), nil)
						continue
					}
					// time types are treated the same as scalar fields; continue to after the if-clause
				case reflect.Map:
					// Check here rather than in mergeMaps to nil the pointer, not just the inner map
					if uField.Elem().Len() == 0 {
						pField.Set(reflect.Zero(pField.Type()))
					} else {
						t.mergeMaps(uField.Elem(), pField.Elem())
					}
					continue
				default:
					pField.Set(uField)
				}
			}
			if pt != nil && pt.constraintFields[fieldName] {
				validateConstraints = true
			}
		} else if uField.Kind() == reflect.Map {
			if uField.IsNil() {
				continue
			}
			t.mergeMaps(uField, pField)
		} else if uField.Kind() == reflect.Slice {
			if uField.IsNil() {
				continue
			}
			pField.Set(uField)
		} else {
			if uField.IsZero() {
				continue
			}
			pField.Set(uField)
			if pt != nil && pt.constraintFields[fieldName] {
				validateConstraints = true
			}
		}
	}
	return validateConstraints, nil
}

// mergeMaps applies per-key merge semantics from uMap into pMap. An empty uMap clears pMap.
// Per-key: zero value deletes the key, struct values are recursively merged, otherwise upsert.
func (t *table) mergeMaps(uMap, pMap reflect.Value) {
	if uMap.Len() == 0 {
		pMap.Set(reflect.Zero(pMap.Type()))
		return
	}
	if pMap.IsNil() {
		pMap.Set(reflect.MakeMap(uMap.Type()))
	}
	for _, k := range uMap.MapKeys() {
		uVal := uMap.MapIndex(k)
		if uVal.IsZero() {
			pMap.SetMapIndex(k, reflect.Value{})
		} else if uVal.Kind() == reflect.Struct {
			pVal := pMap.MapIndex(k)
			if !pVal.IsValid() {
				pMap.SetMapIndex(k, uVal)
			} else {
				// Map values aren't addressable, so copy into temps to merge
				uTmp := reflect.New(uVal.Type()).Elem()
				uTmp.Set(uVal)
				pTmp := reflect.New(pVal.Type()).Elem()
				pTmp.Set(pVal)
				_, _ = t.mergeFields(pTmp, uTmp, nil)
				pMap.SetMapIndex(k, pTmp)
			}
		} else if uVal.Kind() == reflect.Ptr && uVal.Elem().Kind() == reflect.Struct {
			pVal := pMap.MapIndex(k)
			if !pVal.IsValid() || pVal.IsNil() {
				pMap.SetMapIndex(k, uVal)
			} else if uVal.Elem().IsZero() {
				pMap.SetMapIndex(k, reflect.Value{})
			} else {
				_, _ = t.mergeFields(pVal.Elem(), uVal.Elem(), nil)
			}
		} else {
			pMap.SetMapIndex(k, uVal)
		}
	}
}
