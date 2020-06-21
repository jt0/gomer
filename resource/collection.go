package resource

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/util"
)

type CollectionQuery interface {
	resource
	data.Queryable

	PreQuery() gomerr.Gomerr
	Collection() Collection
}

type Collection interface {
	SetItems([]interface{})
	SetNextPageToken(token *string)
	SetPrevPageToken(token *string)
}

func newCollectionQuery(resourceType string, subject auth.Subject) (CollectionQuery, gomerr.Gomerr) {
	metadata, ok := resourceMetadata[strings.ToLower(resourceType)]
	if !ok {
		return nil, gomerr.NotFound("resource type", resourceType).AddCulprit(gomerr.Client)
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
			return nil, gomerr.Unmarshal(err, bytes, collectionQuery)
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

	items := util.EmptySliceForType(c.metadata().instanceType)
	nextToken, ge := c.metadata().dataStore.Query(c, items)
	if ge != nil {
		return nil, ge
	}

	iv := reflect.ValueOf(items).Elem()
	resultsArray := make([]interface{}, 0, iv.Len())
	for i := 0; i < iv.Len(); i++ {
		instance := iv.Index(i).Interface().(Instance)
		instance.setMetadata(c.metadata())
		instance.setSubject(c.Subject())
		instance.OnSubject()

		ge := instance.PostQuery()
		if ge != nil {
			return nil, ge
		}

		scoped, ge := scopedResult(instance)
		if ge != nil {
			var nfe *gomerr.NotFoundError
			if errors.As(ge, &nfe) {
				continue
			} else {
				return nil, ge
			}
		}

		resultsArray = append(resultsArray, scoped)
	}

	collection := c.Collection()
	collection.SetItems(resultsArray)
	collection.SetNextPageToken(nextToken)

	return collection, nil
}

type BaseCollectionQuery struct {
	BaseResource
}

func (b *BaseCollectionQuery) OnSubject() {
	// ignore
}

func (b *BaseCollectionQuery) PersistableTypeName() string {
	return b.md.instanceName
}

func (b *BaseCollectionQuery) NextPageToken() *string {
	return nil
}

func (b *BaseCollectionQuery) PrevPageToken() *string {
	return nil
}

func (b *BaseCollectionQuery) MaximumPageSize() *int {
	return nil
}

func (b *BaseCollectionQuery) ResponseFields() []string {
	return nil
}

func (b *BaseCollectionQuery) PreQuery() gomerr.Gomerr {
	return nil
}
