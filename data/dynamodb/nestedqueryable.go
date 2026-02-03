package dynamodb

import (
	"reflect"

	"github.com/jt0/gomer/data"
)

// nestedQueryableInfo holds information about a nested Queryable field within a struct.
// This is used to detect and process child Queryables in Single Table Design patterns.
type nestedQueryableInfo struct {
	fieldName  string         // Name of the field in parent struct
	fieldIndex int            // Index for efficient field access via reflection
	queryable  data.Queryable // The nested Queryable instance (always non-nil)
}

// detectNestedQueryables scans a struct for non-nil Queryable fields.
// Returns a slice of nestedQueryableInfo for fields that should be included in the query.
// Nil Queryable fields are skipped (the user can set a Queryable field to non-nil
// to opt-in to nested querying, typically in a PreRead hook).
func detectNestedQueryables(v reflect.Value) []nestedQueryableInfo {
	var result []nestedQueryableInfo

	// Get underlying struct (handle pointer)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil
	}

	t := v.Type()
	queryableType := reflect.TypeOf((*data.Queryable)(nil)).Elem()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Handle embedded anonymous structs recursively
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			embedded := detectNestedQueryables(fieldValue)
			result = append(result, embedded...)
			continue
		}

		// Handle embedded pointer to struct
		if field.Anonymous && field.Type.Kind() == reflect.Pointer && field.Type.Elem().Kind() == reflect.Struct {
			if !fieldValue.IsNil() {
				embedded := detectNestedQueryables(fieldValue)
				result = append(result, embedded...)
			}
			continue
		}

		// Check if field type implements Queryable
		fieldType := field.Type
		isPointer := fieldType.Kind() == reflect.Pointer
		if isPointer {
			fieldType = fieldType.Elem()
		}

		// Check both direct and pointer implementation
		implementsQueryable := fieldType.Implements(queryableType) ||
			reflect.PointerTo(fieldType).Implements(queryableType)
		if !implementsQueryable {
			continue
		}

		// Check if non-nil (should be included)
		if isPointer && fieldValue.IsNil() {
			continue // Skip nil Queryables - not opted-in
		}

		// Get the Queryable instance
		var q data.Queryable
		if isPointer {
			q = fieldValue.Interface().(data.Queryable)
		} else {
			// Value type - need to take address
			if fieldValue.CanAddr() {
				q = fieldValue.Addr().Interface().(data.Queryable)
			} else {
				// Cannot take address of field - skip
				continue
			}
		}

		result = append(result, nestedQueryableInfo{
			fieldName:  field.Name,
			fieldIndex: i,
			queryable:  q,
		})
	}

	return result
}
