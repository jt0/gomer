package data

import (
	"github.com/jt0/gomer/gomerr"
)

type Store interface {
	Create(p Persistable) *gomerr.ApplicationError
	Read(p Persistable) *gomerr.ApplicationError
	Update(p Persistable) *gomerr.ApplicationError
	Delete(p Persistable) *gomerr.ApplicationError
	Query(q Queryable, arrayOfPersistable interface{}) (nextToken *string, ae *gomerr.ApplicationError)
}

type Persistable interface {
	Id() string
	KeyValues() []interface{}
}

type Queryable interface {
	QueryInfo() (queryKeys []QueryKey, attributes []string)
	NextToken() *string
	MaxResults() *int64
}

type QueryKey struct {
	Name       string
	Value      interface{}
	Descending bool
}
