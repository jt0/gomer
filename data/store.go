package data

import (
	"github.com/jt0/gomer/gomerr"
)

type Store interface {
	Create(p Persistable) gomerr.Gomerr
	Read(p Persistable) gomerr.Gomerr
	Update(p Persistable, update Persistable) gomerr.Gomerr
	Delete(p Persistable) gomerr.Gomerr
	Query(q Queryable) gomerr.Gomerr
}

type Persistable interface {
	TypeName() string
	NewQueryable() Queryable
}

type Queryable interface {
	TypeNames() []string
	TypeOf(interface{}) string
	Items() []interface{}
	SetItems([]interface{})
	NextPageToken() *string
	SetNextPageToken(*string)
	MaximumPageSize() int
}

type QueryTypes uint16

const (
	EQ QueryTypes = iota + 1
	// NEQ
	// GTE
	// GT
	// LTE
	// LT
	// BETWEEN
	// CONTAINS
)
