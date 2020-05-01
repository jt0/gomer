package dynamodb

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type index struct {
	tableName           *string
	name                *string
	pk                  *keyAttribute
	sk                  *keyAttribute
	canReadConsistently bool
	//projects bool
}

type keyAttribute struct {
	name              string
	attributeType     string
	compositeKeyParts map[string][]string // persistable type name -> key parts
}

type candidate struct {
	index     *index
	skLength  int
	skMissing int
}

func (i *index) processKeySchema(keySchemaElements []*dynamodb.KeySchemaElement, attributeTypes map[string]string) {
	for _, keySchemaElement := range keySchemaElements {
		key := &keyAttribute{
			name:              *keySchemaElement.AttributeName,
			attributeType:     attributeTypes[*keySchemaElement.AttributeName],
			compositeKeyParts: make(map[string][]string),
		}
		switch *keySchemaElement.KeyType {
		case dynamodb.KeyTypeHash:
			i.pk = key
		case dynamodb.KeyTypeRange:
			i.sk = key
		}
	}
}

func indexFor(t *table, q data.Queryable) (*index, *bool, *gomerr.ApplicationError) {
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
		return nil, nil, gomerr.BadRequest("Unable to satisfy query")
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

	for _, keyField := range i.pk.compositeKeyParts[ptName] {
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
		for _, keyField := range i.sk.compositeKeyParts[ptName] {
			if keyField[:1] == "'" {
				continue
			}

			if fv := qv.FieldByName(keyField); !fv.IsValid() {
				return nil
			} else if fv.IsZero() {
				candidate.skMissing++
			} else {
				if candidate.skMissing > 0 { // Cannot have gaps in the middle of the sort key
					return nil
				}
			}
		}

		candidate.skLength = len(i.sk.compositeKeyParts[ptName])
	}

	return candidate
}

func (i *index) populateKeyValues(av map[string]*dynamodb.AttributeValue, p data.Persistable, valueSeparator string) {
	if _, present := av[i.pk.name]; !present {
		av[i.pk.name] = i.pk.attributeValue(p, valueSeparator)
	}

	if i.sk != nil {
		if _, present := av[i.sk.name]; !present {
			av[i.sk.name] = i.sk.attributeValue(p, valueSeparator)
		}
	}
}

func (i *index) keyAttributes() []*keyAttribute {
	if i.sk == nil {
		return []*keyAttribute{i.pk}
	} else {
		return []*keyAttribute{i.pk, i.sk}
	}
}

func (k *keyAttribute) attributeValue(s data.Storable, valueSeparator string) *dynamodb.AttributeValue {
	value := k.buildKeyValue(s, valueSeparator)
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
		panic("Unsupported scalar attribute type value: " + k.attributeType)
	}
}

func (k *keyAttribute) buildKeyValue(s data.Storable, valueSeparator string) string {
	sv := reflect.ValueOf(s).Elem()

	compositeParts := k.compositeKeyParts[s.PersistableTypeName()]
	compositeValues := make([]string, len(compositeParts))
	for i, compositePart := range compositeParts {
		if compositePart[:1] == "'" {
			compositeValues[i] = compositePart[1 : len(compositePart)-1]
		} else {
			compositeValues[i] = fmt.Sprint(sv.FieldByName(compositePart).Interface())
		}
	}

	return strings.Join(compositeValues, valueSeparator)
}
