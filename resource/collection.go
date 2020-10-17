package resource

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type CollectionQuery interface {
	resource
	data.Queryable

	PreQuery() gomerr.Gomerr
	PostQuery() gomerr.Gomerr
	Collection() Collection
}

type Collectible interface {
	OnCollect(CollectionQuery) gomerr.Gomerr
}

type Collection interface {
	SetItems([]interface{})
	SetNextPageToken(token *string)
	SetPrevPageToken(token *string)
}

func newCollectionQuery(resourceType string, subject auth.Subject) (CollectionQuery, gomerr.Gomerr) {
	metadata, ok := lowerCaseResourceTypeToMetadata[strings.ToLower(resourceType)]
	if !ok {
		return nil, unknownResourceType(resourceType)
	}

	collectionQuery := reflect.New(metadata.collectionQueryType.Elem()).Interface().(CollectionQuery)
	collectionQuery.setMetadata(metadata)
	collectionQuery.setSubject(subject)

	return collectionQuery, nil
}

func NewCollectionQuery(resourceType string, subject auth.Subject) (CollectionQuery, gomerr.Gomerr) {
	collectionQuery, ge := newCollectionQuery(resourceType, subject)
	if ge != nil {
		return nil, ge
	}

	collectionQuery.OnSubject()

	return collectionQuery, nil
}

func UnmarshalCollectionQuery(resourceType string, subject auth.Subject, bytes []byte) (CollectionQuery, gomerr.Gomerr) {
	collectionQuery, ge := newCollectionQuery(resourceType, subject)
	if ge != nil {
		return nil, ge
	}

	if len(bytes) != 0 {
		if err := json.Unmarshal(bytes, collectionQuery); err != nil {
			return nil, gomerr.Unmarshal("CollectionQuery", bytes, collectionQuery).Wrap(err)
		}
	}

	collectionQuery.OnSubject()

	return collectionQuery, nil
}

func DoQuery(c CollectionQuery) (Collection, gomerr.Gomerr) {
	ge := c.PreQuery()
	if ge != nil {
		return nil, ge
	}

	items, nextToken, ge := c.metadata().dataStore.Query(c)
	if ge != nil {
		return nil, ge
	}

	lenItems := len(items)
	resultsArray := make([]interface{}, 0, lenItems)
	for i := 0; i < lenItems; i++ {
		instance := items[i].(Instance)
		instance.setMetadata(c.metadata())
		instance.setSubject(c.Subject())
		instance.OnSubject()

		if collectible, ok := instance.(Collectible); ok {
			ge := collectible.OnCollect(c)
			if ge != nil {
				return nil, ge
			}
		}

		rendered, nfe := renderInstance(instance)
		if nfe != nil {
			// Ignore because the item's rendered view contains no visible data so is just excluded from the results
			continue
		}

		resultsArray = append(resultsArray, rendered)
	}

	collection := c.Collection()
	collection.SetItems(resultsArray)
	collection.SetNextPageToken(nextToken)

	return collection, nil
}

type BaseCollectionQuery struct {
	BaseResource
}

func (b *BaseCollectionQuery) PersistableTypeName() string {
	return b.metadata().instanceName
}

func (*BaseCollectionQuery) NextPageToken() string {
	return ""
}

func (*BaseCollectionQuery) PrevPageToken() string {
	return ""
}

func (*BaseCollectionQuery) MaximumPageSize() int {
	return 0
}

func (*BaseCollectionQuery) PreQuery() gomerr.Gomerr {
	return nil
}

func (*BaseCollectionQuery) PostQuery() gomerr.Gomerr {
	return nil
}
