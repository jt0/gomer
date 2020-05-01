package resource

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/logs"
)

type CollectionQuery interface {
	resource
	data.Queryable

	PreQuery() *gomerr.ApplicationError
	Collection(items []interface{}, nextToken *string) (Collection, *gomerr.ApplicationError)
}

type Collection struct {
	Items     []interface{}
	NextToken *string
}

func UnmarshalCollectionQuery(resourceType string, subject auth.Subject, bytes []byte) (CollectionQuery, *gomerr.ApplicationError) {
	metadata, ok := resourceMetadata[strings.ToLower(resourceType)]
	if !ok {
		return nil, gomerr.BadRequest("Unknown type: " + resourceType)
	}

	collectionQuery := reflect.New(metadata.collectionQueryType.Elem()).Interface().(CollectionQuery)

	if len(bytes) != 0 {
		if err := json.Unmarshal(bytes, collectionQuery); err != nil {
			logs.Error.Printf("Unmarshal error while parsing '%s': %s\n", resourceType, err.Error())
			return nil, gomerr.BadRequest("Unable to parse request data", fmt.Sprintf("Data does not appear to correlate to a '%s' resource", collectionQuery.metadata().collectionQueryName))
		}
	}

	collectionQuery.setMetadata(metadata)
	collectionQuery.setSubject(subject)
	collectionQuery.OnSubject()

	return collectionQuery, nil
}

func DoQuery(c CollectionQuery) (interface{}, *gomerr.ApplicationError) {
	ae := c.PreQuery()
	if ae != nil {
		return nil, ae
	}

	items := c.metadata().emptyItems()
	nextToken, ae := c.metadata().dataStore.Query(c, items)
	if ae != nil {
		return nil, ae
	}

	iv := reflect.ValueOf(items).Elem()
	resultsArray := make([]interface{}, 0, iv.Len())
	for i := 0; i < iv.Len(); i++ {
		instance := iv.Index(i).Interface().(Instance)
		instance.setMetadata(c.metadata())
		instance.setSubject(c.Subject())
		instance.OnSubject()

		instance.PostQuery()

		scoped, ae := scopedResult(instance)
		if ae != nil {
			if ae.ErrorType == gomerr.ResourceNotFoundType {
				continue
			} else {
				return nil, ae
			}
		}

		resultsArray = append(resultsArray, scoped)
	}

	return c.Collection(resultsArray, nextToken)
}

type BaseCollectionQuery struct {
	BaseResource
}

func (b *BaseCollectionQuery) PersistableTypeName() string {
	return b.md.instanceName
}

func (b *BaseCollectionQuery) NextToken() *string {
	return nil
}

func (b *BaseCollectionQuery) MaxResults() *int64 {
	return nil
}

func (b *BaseCollectionQuery) ResponseFields() []string {
	return nil
}

func (b *BaseCollectionQuery) PreQuery() *gomerr.ApplicationError {
	return nil
}

func (b *BaseCollectionQuery) Collection(items []interface{}, nextToken *string) (Collection, *gomerr.ApplicationError) {
	return Collection{
		Items:     items,
		NextToken: nextToken,
	}, nil
}
