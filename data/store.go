package data

import (
	"github.com/jt0/gomer/gomerr"
)

type Store interface {
	Create(p Persistable) *gomerr.ApplicationError
	Read(p Persistable) *gomerr.ApplicationError
	Update(p Persistable, update Persistable) *gomerr.ApplicationError
	Delete(p Persistable) *gomerr.ApplicationError
	Query(q Queryable, arrayOfPersistable interface{}) (nextToken *string, ae *gomerr.ApplicationError)
}

type Storable interface {
	PersistableTypeName() string // TODO: change this to support an array of types
}

type Persistable interface {
	Storable

	Id() string
	NewQueryable() Queryable
}

type Queryable interface {
	Storable
	Paginatable

	ResponseFields() []string
}

type Paginatable interface {
	NextPageToken() *string
	PrevPageToken() *string
	MaximumPageSize() *int // TODO: should these pointers? How about nextToken response from query()?
}

type QueryTypes uint16

const (
	EQ QueryTypes = iota + 1
	//NEQ
	//GTE
	//GT
	//LTE
	//LT
	//BETWEEN
	//CONTAINS
)
