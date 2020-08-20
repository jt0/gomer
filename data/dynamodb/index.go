package dynamodb

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type index struct {
	name                *string
	pk                  *keyAttribute
	sk                  *keyAttribute
	canReadConsistently bool
	//projects bool
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

func (i *index) processKeySchema(keySchemaElements []*dynamodb.KeySchemaElement, attributeTypes map[string]string) (ge gomerr.Gomerr) {
	for _, keySchemaElement := range keySchemaElements {
		key := &keyAttribute{
			name:                   *keySchemaElement.AttributeName,
			keyFieldsByPersistable: make(map[string][]string),
		}

		key.attributeType, ge = safeAttributeType(attributeTypes[*keySchemaElement.AttributeName])
		if ge != nil {
			return
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

		if candidate := index.candidate(qv, q.PersistableTypeName()); candidate != nil {
			candidates = append(candidates, candidate)
		}
	}

	switch len(candidates) {
	case 0:
		return nil, nil, data.IndexNotFound(*t.tableName, q).AddCulprit(gomerr.Configuration)
	case 1:
		return candidates[0].index, consistentRead(consistencyType, candidates[0].index.canReadConsistently), nil
	default:
		sort.Slice(candidates, func(i, j int) bool {
			c1 := candidates[i]
			c2 := candidates[j]

			if consistencyType == Preferred && c1.index.canReadConsistently != c2.index.canReadConsistently {
				return c1.index.canReadConsistently
			}

			// 4-2 vs 3-1  a_b_c_d  vs a_b_e_d
			if c1.skMissing != c2.skMissing {
				return c1.skMissing < c2.skMissing
			}

			return c1.skLength > c2.skLength
		})

		return candidates[0].index, consistentRead(consistencyType, candidates[0].index.canReadConsistently), nil
	}
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
				if kfi == 0 {
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

func (i *index) populateKeyValues(av map[string]*dynamodb.AttributeValue, p data.Persistable, valueSeparator string, mustBeSet bool) (ge gomerr.Gomerr) {
	if _, present := av[i.pk.name]; !present {
		av[i.pk.name], ge = i.pk.attributeValue(p, valueSeparator, mustBeSet)
	}

	if ge == nil && i.sk != nil {
		if _, present := av[i.sk.name]; !present {
			av[i.sk.name], ge = i.sk.attributeValue(p, valueSeparator, mustBeSet)
		}
	}

	return
}

func (i *index) keyAttributes() []*keyAttribute {
	if i.sk == nil {
		return []*keyAttribute{i.pk}
	} else {
		return []*keyAttribute{i.pk, i.sk}
	}
}

func (k *keyAttribute) attributeValue(s data.Storable, valueSeparator string, mustBeSet bool) (*dynamodb.AttributeValue, gomerr.Gomerr) {
	value := k.buildKeyValue(s, valueSeparator)
	if value == "" {
		if mustBeSet {
			return nil, gomerr.BadValue("storable.keyAttribute", s, constraint.NonZero())
		}

		return nil, nil
	}

	switch k.attributeType {
	case dynamodb.ScalarAttributeTypeS:
		s := fmt.Sprint(value) // TODO:p1 replace with a better conversion mechanism (e.g. handle times)
		return &dynamodb.AttributeValue{S: &s}, nil
	case dynamodb.ScalarAttributeTypeN:
		n := fmt.Sprint(value)
		return &dynamodb.AttributeValue{N: &n}, nil
	default:
		// Can only be one of the two explicit types - protected by usage of safeAttributeType() function
		return nil, gomerr.Unsupported("unexpected key attributeType: " + k.attributeType).AddCulprit(gomerr.Internal)
	}
}

func safeAttributeType(attributeType string) (string, gomerr.Gomerr) {
	if attributeType == dynamodb.ScalarAttributeTypeS || attributeType == dynamodb.ScalarAttributeTypeN {
		return attributeType, nil
	}

	return "", gomerr.BadValue("attributeType", attributeType, constraint.Values("S", "N")).AddNotes("Only supporting string and number attribute types").AddCulprit(gomerr.Configuration)
}

func (k *keyAttribute) buildKeyValue(s data.Storable, valueSeparator string) string {
	sv := reflect.ValueOf(s).Elem()
	keyFields := k.keyFieldsByPersistable[s.PersistableTypeName()]
	keyValue := fieldValue(keyFields[0], sv) // will always have at least one keyField
	for i := 1; i < len(keyFields); i++ {
		keyValue += valueSeparator
		keyValue += fieldValue(keyFields[i], sv)
	}
	return keyValue
}

func fieldValue(fieldName string, sv reflect.Value) string {
	if fieldName[:1] == "'" {
		return fieldName[1 : len(fieldName)-1]
	} else {
		v := sv.FieldByName(fieldName)
		if v.IsValid() {
			return fmt.Sprint(v.Interface())
		} else {
			return ""
		}
	}
}
