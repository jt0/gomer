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

type Collection interface {
	resource
	data.Queryable

	PreQuery() *gomerr.ApplicationError
	PostQuery(results interface{}) *gomerr.ApplicationError
}

func NewCollection(resourceType string, subject auth.Subject) (Collection, *gomerr.ApplicationError) {
	metadata, ok := resourceMetadata[strings.ToLower(resourceType)]
	if !ok {
		return nil, gomerr.BadRequest("Unknown type: " + resourceType)
	}

	// Future: could support non-Elem() types, but not sure if it's worth it
	collection := reflect.New(metadata.collectionType.Elem()).Interface().(Collection)
	collection.setMetadata(metadata)
	collection.setSubject(subject)

	return collection, nil
}

func UnmarshallCollection(resourceType string, subject auth.Subject, bytes []byte) (Collection, *gomerr.ApplicationError) {
	collection, ae := NewCollection(resourceType, subject)
	if ae != nil {
		return nil, ae
	}

	if len(bytes) != 0 {
		if err := json.Unmarshal(bytes, collection); err != nil {
			logs.Error.Printf("Unmarshal error while parsing '%s': %s\n", collection.metadata().collectionName, err.Error())
			return nil, gomerr.BadRequest("Unable to parse request data", fmt.Sprintf("Data does not appear to correlate to a '%s' resource", collection.metadata().collectionName))
		}
	}

	return collection, nil
}

type QueryResult struct {
	Items     []interface{}
	NextToken *string
}

func DoQuery(c Collection) (*QueryResult, *gomerr.ApplicationError) {
	ae := c.PreQuery()
	if ae != nil {
		return nil, ae
	}

	items := c.metadata().emptyItems()
	nextToken, ae := c.metadata().dataStore.Query(c, items)
	if ae != nil {
		return nil, ae
	}

	ae = c.PostQuery(items)
	if ae != nil {
		return nil, ae
	}

	var resultsArray []interface{}
	iv := reflect.ValueOf(items).Elem()
	for i := 0; i < iv.Len(); i++ {
		instance := iv.Index(i).Interface().(Instance)
		instance.setSubject(c.Subject())
		instance.setMetadata(c.metadata())
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

	return &QueryResult{resultsArray, nextToken}, nil
}

type BaseCollection struct {
	Next     *string `json:"NextToken"`
	MaxItems *int64  `json:"MaxResults,omitempty"`
}

func (b *BaseCollection) NextToken() *string {
	return b.Next
}

func (b *BaseCollection) MaxResults() *int64 {
	return b.MaxItems
}

func (b *BaseCollection) PreQuery() *gomerr.ApplicationError {
	return nil
}

func (b *BaseCollection) PostQuery(_ interface{}) *gomerr.ApplicationError {
	return nil
}
