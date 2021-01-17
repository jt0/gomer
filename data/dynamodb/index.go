package dynamodb

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/aws/aws-sdk-go/service/dynamodb"

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
	// projects bool
}

type keyAttribute struct {
	name                   string
	attributeType          string
	keyFieldsByPersistable map[string][]string // persistable type name -> key fields
}

type candidate struct {
	index     *index
	skLength  int
	skMissing int
}

func (i *index) friendlyName() string {
	if i.name == nil {
		return "__table__"
	}
	return *i.name
}

func (i *index) processKeySchema(keySchemaElements []*dynamodb.KeySchemaElement, attributeTypes map[string]string) (ge gomerr.Gomerr) {
	for _, keySchemaElement := range keySchemaElements {
		key := &keyAttribute{
			name:                   *keySchemaElement.AttributeName,
			keyFieldsByPersistable: make(map[string][]string),
		}

		key.attributeType, ge = safeAttributeType(attributeTypes[key.name])
		if ge != nil {
			return ge.AddAttribute("key.name", key.name)
		}

		switch *keySchemaElement.KeyType {
		case dynamodb.KeyTypeHash:
			i.pk = key
		case dynamodb.KeyTypeRange:
			i.sk = key
		}
	}

	return nil
}

var safeTypeConstraint = constraint.OneOf(dynamodb.ScalarAttributeTypeS, dynamodb.ScalarAttributeTypeN)

func safeAttributeType(attributeType string) (string, gomerr.Gomerr) {
	ge := safeTypeConstraint.Validate(attributeType)
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
//  gomerr.Missing:
//      if there is no matching index for the query
func indexFor(t *table, q data.Queryable) (*index, *bool, gomerr.Gomerr) {
	var consistencyType ConsistencyType
	if c, ok := q.(ConsistencyTyper); ok {
		consistencyType = c.ConsistencyType()
	} else {
		consistencyType = t.defaultConsistencyType
	}

	candidates := make([]*candidate, 0, 5)
	qv := reflect.ValueOf(q).Elem()

	for _, index := range t.indexes {
		if consistencyType == Required && !index.canReadConsistently {
			continue
		}

		// A viable candidate needs to use one index to collect each type that the queryable is interested in. To find
		// it, range through each index and if it doesn't work for a type, fail and move to the next one.
		//
		// XXX: revisit - should be the one that covers the least, right? Amongst the viable candidates, choose the
		//      best match under the (presumption) that fewer missing keys and longer key length are better
		var candidate *candidate
		for _, typeName := range q.TypeNames() {
			if c := index.candidate(qv, typeName); c == nil {
				candidate = nil
				break
			} else if candidate == nil {
				candidate = c
			} else if compareCandidates(candidate, c) {
				c = candidate
			}
		}
		if candidate != nil {
			candidates = append(candidates, candidate)
		}
	}

	switch len(candidates) {
	case 0:
		available := make(map[string]interface{}, 1)
		for _, index := range t.indexes {
			available[index.friendlyName()] = index
		}
		return nil, nil, dataerr.NoIndexMatch(available, q)
	case 1:
		return candidates[0].index, consistentRead(consistencyType, candidates[0].index.canReadConsistently), nil
	default:
		sort.Slice(candidates, func(i, j int) bool {
			c1 := candidates[i]
			c2 := candidates[j]

			if consistencyType == Preferred && c1.index.canReadConsistently != c2.index.canReadConsistently {
				return c1.index.canReadConsistently // sorts based on which of c1 or c2 can be read consistently
			}

			return compareCandidates(c1, c2)
		})

		return candidates[0].index, consistentRead(consistencyType, candidates[0].index.canReadConsistently), nil
	}
}

func compareCandidates(c1 *candidate, c2 *candidate) bool {
	// 4-2 vs 3-1  a_b_c_d  vs a_b_e_d
	if c1.skMissing != c2.skMissing {
		return c1.skMissing < c2.skMissing
	}

	return c1.skLength > c2.skLength
}

func (i *index) candidate(qv reflect.Value, ptName string) *candidate {
	// TODO: validate index sufficiently projects over request. if not, return nil

	for _, keyField := range i.pk.keyFieldsByPersistable[ptName] {
		if keyField[:1] == "'" {
			continue
		}

		if fv := qv.FieldByName(keyField); !fv.IsValid() || fv.IsZero() {
			return nil
		}
	}

	candidate := &candidate{index: i}

	// Needs more work to handle multi-attribute cases such as "between"
	if i.sk != nil {
		for kfi, keyField := range i.sk.keyFieldsByPersistable[ptName] {
			if keyField[:1] == "'" {
				continue
			}

			fv := qv.FieldByName(keyField)
			if !fv.IsValid() {
				// Fixed bug where index could be dropped as a candidate if
				// a trailing sort key member doesn't exist in the Queryable.
				if kfi == 0 { // XXX: Why is this additional check here? It may be valid to have no SK value present for a query
					return nil
				} else {
					candidate.skMissing++
				}
			} else if fv.IsZero() {
				candidate.skMissing++
			} else {
				if candidate.skMissing > 0 { // Cannot have gaps in the middle of the sort key
					return nil
				}
			}
		}

		candidate.skLength = len(i.sk.keyFieldsByPersistable[ptName])
	}

	return candidate
}

func (i *index) populateKeyValues(avm map[string]*dynamodb.AttributeValue, p data.Persistable, valueSeparator string, mustBeSet bool) gomerr.Gomerr {
	var av *dynamodb.AttributeValue

	// TODO: any reason Elem() would be incorrect?
	pElem := reflect.ValueOf(p).Elem()

	if _, present := avm[i.pk.name]; !present {
		if av = i.pk.attributeValue(pElem, p.TypeName(), valueSeparator); av != nil {
			avm[i.pk.name] = av
		} else if mustBeSet {
			return dataerr.KeyValueNotFound(i.pk.name, i.pk.keyFieldsByPersistable[p.TypeName()], p)
		}
	}

	if i.sk != nil {
		if _, present := avm[i.sk.name]; !present {
			if av = i.sk.attributeValue(pElem, p.TypeName(), valueSeparator); av != nil {
				avm[i.sk.name] = av
			} else if mustBeSet {
				return dataerr.KeyValueNotFound(i.sk.name, i.sk.keyFieldsByPersistable[p.TypeName()], p)
			}
		}
	}

	return nil
}

func (i *index) keyAttributes() []*keyAttribute {
	if i.sk == nil {
		return []*keyAttribute{i.pk}
	} else {
		return []*keyAttribute{i.pk, i.sk}
	}
}

func (k *keyAttribute) attributeValue(elemValue reflect.Value, persistableTypeName string, valueSeparator string) *dynamodb.AttributeValue {
	value := k.buildKeyValue(elemValue, persistableTypeName, valueSeparator)
	if value == "" {
		return nil
	}

	switch k.attributeType {
	case dynamodb.ScalarAttributeTypeS:
		s := fmt.Sprint(value) // TODO:p1 replace with a better conversion mechanism (e.g. handle times)
		return &dynamodb.AttributeValue{S: &s}
	case dynamodb.ScalarAttributeTypeN:
		n := fmt.Sprint(value)
		return &dynamodb.AttributeValue{N: &n}
	default:
		// Log that safeAttributeType() missed something. received type: k.AttributeType
	}

	return nil
}

func (k *keyAttribute) buildKeyValue(elemValue reflect.Value, persistableTypeName string, valueSeparator string) string {
	// sv := reflect.ValueOf(s).Elem()
	keyFields := k.keyFieldsByPersistable[persistableTypeName]
	keyValue := fieldValue(keyFields[0], elemValue) // will always have at least one keyField
	for i := 1; i < len(keyFields); i++ {
		keyValue += valueSeparator
		keyValue += fieldValue(keyFields[i], elemValue)
	}
	return keyValue
}

func fieldValue(fieldName string, sv reflect.Value) string {
	if fieldName[:1] == "'" {
		return fieldName[1 : len(fieldName)-1]
	} else {
		v := sv.FieldByName(fieldName)
		if v.IsValid() && !v.IsZero() {
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()
			}
			return fmt.Sprint(v.Interface())
		} else {
			return ""
		}
	}
}
