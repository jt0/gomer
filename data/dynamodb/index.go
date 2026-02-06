package dynamodb

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/data/dataerr"
	"github.com/jt0/gomer/gomerr"
)

type index struct {
	name                *string
	pk                  *keyAttribute
	sk                  *keyAttribute
	canReadConsistently bool
	queryWildcardChar   byte
	// projects bool
}

type keyAttribute struct {
	name                   string
	attributeType          string
	keyFieldsByPersistable map[string][]*keyField // persistable type name -> key fields
}

type keyField struct {
	name      string
	preferred bool
	ascending bool
}

type candidate struct {
	index     *index
	preferred bool
	ascending bool
	skLength  int
	skMissing int
}

func (i *index) friendlyName() string {
	if i.name == nil {
		return "__table__"
	}
	return *i.name
}

func (i *index) processKeySchema(keySchemaElements []types.KeySchemaElement, attributeTypes map[string]string) (ge gomerr.Gomerr) {
	for _, keySchemaElement := range keySchemaElements {
		key := &keyAttribute{
			name:                   *keySchemaElement.AttributeName,
			keyFieldsByPersistable: make(map[string][]*keyField),
		}

		key.attributeType, ge = safeAttributeType(attributeTypes[key.name])
		if ge != nil {
			return ge.AddAttribute("key.name", key.name)
		}

		switch keySchemaElement.KeyType {
		case types.KeyTypeHash:
			i.pk = key
		case types.KeyTypeRange:
			i.sk = key
		}
	}

	return nil
}

var safeTypeConstraint = constraint.OneOf(string(types.ScalarAttributeTypeS), string(types.ScalarAttributeTypeN))

func safeAttributeType(attributeType string) (string, gomerr.Gomerr) {
	ge := safeTypeConstraint.Validate("AttributeType", attributeType)
	if ge != nil {
		return "", ge
	}

	return attributeType, nil
}

// indexFor attempts to find the best index match for the provided queryable. The definition of "best" is the index
// that has the greatest number of matching attributes present in the query.
//
// If the data.Queryable implements ConsistencyTyper and it states that the query must be strongly consistent, GSIs
// will be excluded from consideration. On success, the function returns the matching index (if one), and a boolean
// to include as the 'consistent' value for the ddb query. Possible errors:
//
//	gomerr.Missing:
//	    if there is no matching index for the query
func indexFor(t *table, q data.Queryable) (index *index, ascending bool, consistent *bool, ge gomerr.Gomerr) {
	var consistencyType ConsistencyType
	if c, ok := q.(ConsistencyTyper); ok {
		consistencyType = c.ConsistencyType()
	} else {
		consistencyType = t.defaultConsistencyType
	}

	candidates := make([]*candidate, 0, len(t.indexes))
	qv := reflect.ValueOf(q).Elem()

	for _, idx := range t.indexes {
		if consistencyType == Required && !idx.canReadConsistently {
			continue
		}

		// A viable candidate needs to use one index to collect each type that the queryable is interested in. To find
		// it, range through each index and if it doesn't work for a type, fail and move to the next one.
		//
		// TODO: revisit - should be the one that covers the least, right? Amongst the viable candidates, choose the
		//      best match under the (presumption) that fewer missing keys and longer key length are better
		if c := idx.candidate(qv, q.TypeName()); c != nil {
			candidates = append(candidates, c)
		}
	}

	switch len(candidates) {
	case 0:
		available := make(map[string]interface{}, 1)
		for _, idx := range t.indexes {
			available[idx.friendlyName()] = idx
		}
		return nil, false, nil, dataerr.NoIndexMatch(available, q)
	case 1:
		// do nothing. candidates[0] returned below
	default:
		sort.Slice(candidates, func(i, j int) bool {
			c1 := candidates[i]
			c2 := candidates[j]

			if c1.preferred != c2.preferred {
				return c1.preferred // sorts based on which of c1 or c2 is preferred over the other
			}

			// TODO: moving and reordering logic from compareCandidates - consider if it should be applied above, too
			// 4-2 vs 3-1  a_b_c_d  vs a_b_e_d
			if c1.skMissing != c2.skMissing {
				return c1.skMissing < c2.skMissing
			}

			if c1.skLength != c2.skLength {
				return c1.skLength > c2.skLength
			}

			if consistencyType == Preferred && c1.index.canReadConsistently != c2.index.canReadConsistently {
				return c1.index.canReadConsistently // sorts based on which of c1 or c2 can be read consistently
			}

			return c1.index.name == nil // favor the table's index over others
		})
	}

	return candidates[0].index, candidates[0].ascending, consistentRead(consistencyType, candidates[0].index.canReadConsistently), nil
}

func (i *index) candidate(qv reflect.Value, ptName string) *candidate {
	// TODO: validate index sufficiently projects over request. if not, return nil
	var keyFields []*keyField
	if keyFields = i.pk.keyFieldsByPersistable[ptName]; keyFields == nil {
		return nil
	}
	for _, kf := range keyFields {
		if kf.name[0] == '\'' {
			continue
		}

		fv := qv.FieldByName(kf.name)
		if !fv.IsValid() || fv.IsZero() {
			return nil
		}

		if i.queryWildcardChar != 0 {
			if s, ok := fv.Interface().(string); ok && s != "" && s[len(s)-1] == i.queryWildcardChar {
				return nil
			}
		}
	}

	c := &candidate{index: i}

	// Needs more work to handle multi-attribute cases such as "between"
	if i.sk != nil {
		if keyFields = i.sk.keyFieldsByPersistable[ptName]; keyFields == nil {
			return nil
		}
		applySort := true
		hasWildcard := false
		for _, kf := range keyFields {
			// Sort order comes from the first unprovided (or partially-provided) field since that
			// field contains the data to be ordered. We look one field ahead by continuing to
			// apply until we find a missing or wildcarded value.
			if applySort {
				c.ascending = kf.ascending
			}

			if kf.name[:1] == "'" {
				c.preferred = kf.preferred // static values always apply preferred
				continue
			}

			fv := qv.FieldByName(kf.name)
			if !fv.IsValid() || fv.IsZero() {
				applySort = false // sort indicator already captured from this field
				c.skMissing++
			} else if c.skMissing > 0 || hasWildcard { // Cannot have gaps in the middle of the sort key
				return nil
			} else if i.queryWildcardChar != 0 && endsWithWildcard(fv, i.queryWildcardChar) {
				// Wildcard means partial value - sort indicator captured, remaining fields are variable
				applySort = false
				hasWildcard = true
				c.preferred = kf.preferred
			} else {
				c.preferred = kf.preferred // only set preferred when field has a value
			}
		}
		c.skLength = len(keyFields)
	}

	return c
}

// endsWithWildcard checks if a reflect.Value (string or *string) ends with the wildcard character.
func endsWithWildcard(fv reflect.Value, wildcardChar byte) bool {
	if fv.Kind() == reflect.Ptr {
		fv = fv.Elem()
	}
	if s, ok := fv.Interface().(string); ok && s != "" {
		return s[len(s)-1] == wildcardChar
	}
	return false
}

func (i *index) populateKeyValues(avm map[string]types.AttributeValue, p data.Persistable, valueSeparator byte, mustBeSet bool) gomerr.Gomerr {
	var av types.AttributeValue

	// TODO: any reason Elem() would be incorrect?
	pElem := reflect.ValueOf(p).Elem()

	if _, present := avm[i.pk.name]; !present {
		if av = i.pk.attributeValue(pElem, p.TypeName(), valueSeparator, 0); av != nil {
			avm[i.pk.name] = av
		} else if mustBeSet {
			return dataerr.KeyValueNotFound(i.pk.name, keyFieldNames(i.pk.keyFieldsByPersistable[p.TypeName()]), p)
		}
	}

	if i.sk != nil {
		if _, present := avm[i.sk.name]; !present {
			if av = i.sk.attributeValue(pElem, p.TypeName(), valueSeparator, 0); av != nil {
				avm[i.sk.name] = av
			} else if mustBeSet {
				return dataerr.KeyValueNotFound(i.sk.name, keyFieldNames(i.sk.keyFieldsByPersistable[p.TypeName()]), p)
			}
		}
	}

	return nil
}

func keyFieldNames(keyFields []*keyField) []string {
	names := make([]string, len(keyFields))
	for i, kf := range keyFields {
		names[i] = kf.name
	}
	return names
}

func (i *index) keyAttributes() []*keyAttribute {
	if i.sk == nil {
		return []*keyAttribute{i.pk}
	} else {
		return []*keyAttribute{i.pk, i.sk}
	}
}

func (k *keyAttribute) attributeValue(elemValue reflect.Value, persistableTypeName string, valueSeparator, queryWildcardChar byte) types.AttributeValue {
	value := k.buildKeyValue(elemValue, persistableTypeName, valueSeparator, queryWildcardChar)
	if value == "" {
		return nil
	}

	switch k.attributeType {
	case string(types.ScalarAttributeTypeS):
		return &types.AttributeValueMemberS{Value: value}
	case string(types.ScalarAttributeTypeN):
		return &types.AttributeValueMemberN{Value: value} // TODO:p3 add better support for numeric values
	default:
		// Log that safeAttributeType() missed something. received type: k.AttributeType
	}

	return nil
}

func (k *keyAttribute) buildKeyValue(elemValue reflect.Value, persistableTypeName string, valueSeparator, queryWildcardChar byte) string {
	// sv := reflect.ValueOf(s).Elem()
	escapeChar := valueSeparator + 1
	keyFields := k.keyFieldsByPersistable[persistableTypeName]
	if keyFields == nil {
		return ""
	}
	keyValue := fieldValue(keyFields[0].name, elemValue, valueSeparator, escapeChar) // will always have at least one keyField
	if len(keyFields) > 1 {                                                          // 3
		separator := string(valueSeparator)
		lastFieldIndex := 0
		for i, separators := 1, separator; i < len(keyFields); i, separators = i+1, separators+separator {
			if nextField := fieldValue(keyFields[i].name, elemValue, valueSeparator, escapeChar); nextField != "" {
				keyValue += separators // add collected separators when a fieldValue is not ""
				keyValue += nextField
				lastFieldIndex, separators = i, ""
			}
		}

		// If no non-empty fields found (all empty), preserve structure with separators
		if lastFieldIndex == 0 && keyValue == "" {
			// All fields are empty - return separators to show structure
			// E.g., 2 fields: "#", 3 fields: "##"
			for i := 1; i < len(keyFields); i++ {
				keyValue += separator
			}
		} else if lastFieldIndex < len(keyFields)-1 && len(keyValue) > 0 && keyValue[len(keyValue)-1] != queryWildcardChar {
			// Add trailing separator for empty fields after last non-empty field
			keyValue += separator
		}
	}
	return keyValue
}

// unescapeAndSplit splits a composite key value by separator and unescapes each segment.
// Handles escaped separators and escape characters correctly.
func unescapeAndSplit(value string, separator, escape byte) []string {
	if len(value) == 0 {
		return []string{}
	}

	var segments []string
	var current []byte
	i := 0

	for i < len(value) {
		b := value[i]

		if b == escape && i+1 < len(value) {
			// Escape sequence - next character is literal
			next := value[i+1]
			if next == separator || next == escape {
				current = append(current, next)
				i += 2 // Skip both escape and escaped character
				continue
			}
		}

		if b == separator {
			// Unescaped separator - split point
			segments = append(segments, string(current))
			current = nil
		} else {
			current = append(current, b)
		}

		i++
	}

	// Add the last segment
	segments = append(segments, string(current))

	return segments
}

// escapeAndJoin joins segments with separator, escaping any separator or escape chars in values.
func escapeAndJoin(segments []string, separator, escape byte) string {
	escaped := make([]string, len(segments))
	for i, s := range segments {
		escaped[i] = escapeKeyValue(s, separator, escape)
	}

	result := ""
	for i, s := range escaped {
		if i > 0 {
			result += string(separator)
		}
		result += s
	}
	return result
}

// escapeKeyValue escapes separator and escape characters in field values to prevent ambiguity in composite keys.
// Uses the next ASCII character after separator as the escape character to preserve sort order.
func escapeKeyValue(value string, separator, escape byte) string {
	// Fast path: no escaping needed
	if len(value) == 0 {
		return value
	}

	needsEscape := false
	for i := 0; i < len(value); i++ {
		if value[i] == separator || value[i] == escape {
			needsEscape = true
			break
		}
	}

	if !needsEscape {
		return value
	}

	// Escape both separator and escape character
	result := make([]byte, 0, len(value)+4) // Pre-allocate some extra space
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b == separator || b == escape {
			result = append(result, escape) // Add escape character
		}
		result = append(result, b)
	}

	return string(result)
}

func fieldValue(fieldName string, sv reflect.Value, separator, escape byte) string {
	if fieldName[:1] == "'" {
		// Static value - don't escape (controlled by developer, not user data)
		return fieldName[1 : len(fieldName)-1]
	} else {
		v := sv.FieldByName(fieldName)
		// NB: if the type is a number w/ a value of 0, it will be discarded. To use an actual zero, one needs to
		//  specify the attribute as a pointer to the numeric type.
		if v.IsValid() && !v.IsZero() {
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()
			}

			// Special case for time.Time to match framework RFC3339 standard
			var value string
			if t, ok := v.Interface().(time.Time); ok {
				value = t.Format(time.RFC3339)
			} else {
				value = fmt.Sprint(v.Interface())
			}

			// Escape separator and escape characters to preserve sort order and avoid ambiguity
			return escapeKeyValue(value, separator, escape)
		} else {
			return ""
		}
	}
}

// indexForMultiple finds an index that supports querying multiple types together.
// This is used when a Queryable has nested Queryables that should be fetched in a single query.
//
// Returns:
//   - index: The best matching index, or nil if no single index can satisfy all types
//   - ascending: Sort direction based on the parent type's key definition
//   - consistent: Whether consistent reads should be used
//   - error: Only returned for internal errors; nil index means fallback to separate queries
func indexForMultiple(t *table, parentType string, childTypes []string, q data.Queryable) (*index, bool, *bool, gomerr.Gomerr) {
	allTypes := append([]string{parentType}, childTypes...)

	var consistencyType ConsistencyType
	if c, ok := q.(ConsistencyTyper); ok {
		consistencyType = c.ConsistencyType()
	} else {
		consistencyType = t.defaultConsistencyType
	}

	candidates := make([]*candidate, 0)
	qv := reflect.ValueOf(q).Elem()

	for _, idx := range t.indexes {
		if consistencyType == Required && !idx.canReadConsistently {
			continue
		}

		// Check if index supports ALL types
		if !indexSupportsAllTypes(idx, allTypes) {
			continue
		}

		// Verify PK compatibility (all types must have compatible PK structure)
		if !verifyPKCompatibility(idx, allTypes) {
			continue
		}

		// Use parent type for candidate evaluation
		if c := idx.candidate(qv, parentType); c != nil {
			candidates = append(candidates, c)
		}
	}

	if len(candidates) == 0 {
		return nil, false, nil, nil // No shared index; caller should use separate queries
	}

	// Sort candidates (same logic as indexFor)
	sort.Slice(candidates, func(i, j int) bool {
		c1 := candidates[i]
		c2 := candidates[j]

		if c1.preferred != c2.preferred {
			return c1.preferred
		}

		if c1.skMissing != c2.skMissing {
			return c1.skMissing < c2.skMissing
		}

		if c1.skLength != c2.skLength {
			return c1.skLength > c2.skLength
		}

		if consistencyType == Preferred && c1.index.canReadConsistently != c2.index.canReadConsistently {
			return c1.index.canReadConsistently
		}

		return c1.index.name == nil
	})

	return candidates[0].index, candidates[0].ascending, consistentRead(consistencyType, candidates[0].index.canReadConsistently), nil
}

// indexSupportsAllTypes checks if an index has key field mappings for all given types.
func indexSupportsAllTypes(idx *index, types []string) bool {
	for _, typeName := range types {
		// Must have PK mapping
		if idx.pk.keyFieldsByPersistable[typeName] == nil {
			return false
		}
		// Must have SK mapping if index has SK
		if idx.sk != nil && idx.sk.keyFieldsByPersistable[typeName] == nil {
			return false
		}
	}
	return true
}

// verifyPKCompatibility verifies that multiple types have compatible PK structures.
// For types to share a query, their PK fields must have the same structure so that
// a single PK value from the query can be used to fetch items of all types.
func verifyPKCompatibility(idx *index, types []string) bool {
	if len(types) < 2 {
		return true
	}

	// Get PK field names for first type
	baseFields := idx.pk.keyFieldsByPersistable[types[0]]
	if baseFields == nil {
		return false
	}

	// Verify all other types have same PK structure
	for _, typeName := range types[1:] {
		typeFields := idx.pk.keyFieldsByPersistable[typeName]
		if typeFields == nil || len(typeFields) != len(baseFields) {
			return false
		}
		for i, kf := range typeFields {
			// Static fields must match exactly
			if baseFields[i].name[0] == '\'' || kf.name[0] == '\'' {
				if baseFields[i].name != kf.name {
					return false
				}
			}
			// Dynamic field names must match so query values can be shared
			if baseFields[i].name != kf.name {
				return false
			}
		}
	}
	return true
}

// commonKeyPrefix computes the SK prefix that covers both parent and child types.
// This enables a single query using begins_with(SK, prefix) to return items of multiple types.
//
// For Extension (SK: E#<id>) and ExtensionVersion (SK: E#<id>#V#...),
// the common prefix is "E#<id>#" which matches both.
func commonKeyPrefix(idx *index, parentType string, qv reflect.Value, separator, escape byte) string {
	if idx.sk == nil {
		return ""
	}

	parentFields := idx.sk.keyFieldsByPersistable[parentType]
	if parentFields == nil {
		return ""
	}

	// Build segments from parent's key fields
	var segments []string

	for _, kf := range parentFields {
		if len(kf.name) > 0 && kf.name[0] == '\'' {
			// Static value - extract from quotes
			segments = append(segments, kf.name[1:len(kf.name)-1])
		} else {
			// Dynamic field - get value from query
			fv := qv.FieldByName(kf.name)
			if !fv.IsValid() || fv.IsZero() {
				// No more values - stop here
				break
			}
			segments = append(segments, fmt.Sprint(fv.Interface()))
		}
	}

	if len(segments) == 0 {
		return ""
	}

	// Join with escaping and add trailing separator for begins_with
	return escapeAndJoin(segments, separator, escape) + string(separator)
}
