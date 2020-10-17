package data

import (
	"github.com/jt0/gomer/gomerr"
)

type Store interface {
	Create(p Persistable) gomerr.Gomerr
	Read(p Persistable) gomerr.Gomerr
	Update(p Persistable, update Persistable) gomerr.Gomerr
	Delete(p Persistable) gomerr.Gomerr
	Query(q Queryable) (items []interface{}, nextToken *string, ge gomerr.Gomerr)
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
}

type Paginatable interface {
	NextPageToken() string
	PrevPageToken() string
	MaximumPageSize() int
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
