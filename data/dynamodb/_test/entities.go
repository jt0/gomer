package _test

import (
	"time"

	"github.com/jt0/gomer/data"
)

// CompositeKeyEntity - demonstrates partition + sort key pattern (non-multi-tenant)
type CompositeKeyEntity struct {
	PartitionKey string `db.keys:"pk"`
	SortKey      string `db.keys:"sk"`
	Data         string
	Status       string
	Active       bool
}

func (c *CompositeKeyEntity) TypeName() string             { return "CompositeKeyEntity" }
func (c *CompositeKeyEntity) NewQueryable() data.Queryable { return &CompositeKeyEntities{} }

type CompositeKeyEntities struct {
	data.BaseQueryable
	PartitionKey string // Can list all entities within a partition
	SortKey      string
	Data         string
	Status       string
	// Active       bool
}

func (q *CompositeKeyEntities) TypeName() string { return "CompositeKeyEntity" }

// MultiPartKeyEntity - demonstrates composite keys with multiple parts
type MultiPartKeyEntity struct {
	TenantId   string `db.keys:"pk.0"`
	EntityType string `db.keys:"pk.1"`
	Id         string `db.keys:"sk"`
	Payload    string
}

func (m *MultiPartKeyEntity) TypeName() string             { return "MultiPartKeyEntity" }
func (m *MultiPartKeyEntity) NewQueryable() data.Queryable { return &MultiPartKeyEntities{} }

type MultiPartKeyEntities struct {
	data.BaseQueryable
	TenantId   string // List by tenant
	EntityType string // List by tenant + entity type
	Id         string
}

func (q *MultiPartKeyEntities) TypeName() string { return "MultiPartKeyEntity" }

// StaticKeyEntity - demonstrates static key prefixes for single table design
type StaticKeyEntity struct {
	Id     string `db.keys:"pk.0='ITEM',pk.1"`
	Status string `db.keys:"sk.0='STATUS',sk.1"`
	Detail string
}

func (s *StaticKeyEntity) TypeName() string             { return "StaticKeyEntity" }
func (s *StaticKeyEntity) NewQueryable() data.Queryable { return &StaticKeyEntities{} }

type StaticKeyEntities struct {
	data.BaseQueryable
	Id     string // List by id (all status values for an item)
	Status string // List by id + status
}

func (q *StaticKeyEntities) TypeName() string { return "StaticKeyEntity" }

// User - concrete domain entity for multi-tenant service
// Use cases: read by id, list by tenant, lookup by email
type User struct {
	TenantId string `db.keys:"pk.0,gsi_1:sk"`
	Id       string `db.keys:"pk.1='USER',sk"`
	Email    string `db.keys:"gsi_1:pk.0='USER',gsi_1:pk.1" db.constraints:"unique(TenantId)"`
	Name     string
	Status   string
}

func (u *User) TypeName() string             { return "User" }
func (u *User) NewQueryable() data.Queryable { return &Users{} }

type Users struct {
	data.BaseQueryable
	TenantId string // List users by tenant (uses base table)
	Email    string // Lookup user by email (uses gsi_1)
}

func (q *Users) TypeName() string { return "User" }

// Product - concrete domain entity for multi-tenant e-commerce
// Use cases: read by id, read by sku, list by tenant, browse by category
type Product struct {
	TenantId    string `db.keys:"pk.0,gsi_1:pk.0"`
	Id          string `db.keys:"pk.1='PRODUCT',sk"`
	Sku         string `db.keys:"lsi_1:sk" db.constraints:"unique(TenantId)"`
	Category    string `db.keys:"gsi_1:pk.1='CATEGORY',gsi_1:sk.0"`
	Name        string `db.keys:"gsi_1:sk.1='PRODUCT',gsi_1:sk.2"`
	Price       float64
	Description string
}

func (p *Product) TypeName() string             { return "Product" }
func (p *Product) NewQueryable() data.Queryable { return &Products{} }

type Products struct {
	data.BaseQueryable
	TenantId string // List products by tenant (uses base table)
	Category string // Browse products by category within tenant (uses gsi_1)
	Sku      string
	Name     string
}

func (q *Products) TypeName() string { return "Product" }

// Order - concrete domain entity for multi-tenant e-commerce
// Use cases: read by id, list by user, list by status
type Order struct {
	TenantId  string    `db.keys:"pk.0,pk.1='ORDER'"`
	OrderId   string    `db.keys:"sk.0='ID',sk.1"`
	UserId    string    `db.keys:"lsi_1:sk.0='USER',lsi_1:sk.1"`
	OrderDate time.Time `db.keys:"-lsi_1:sk.2,-lsi_2:sk"`
	Status    string    // `db.keys:""`
	Total     float64
}

func (o *Order) TypeName() string             { return "Order" }
func (o *Order) NewQueryable() data.Queryable { return &Orders{} }

type Orders struct {
	data.BaseQueryable
	TenantId string
	UserId   string // List orders by user (uses gsi_1, sorted by date desc)
	Status   string // List orders by status within tenant (uses gsi_2, sorted by date desc)
}

func (q *Orders) TypeName() string { return "Order" }

// EmptyValueEntity - demonstrates empty and zero value handling
// Note: Zero int values (0) are treated as "not set" and become empty segments
type EmptyValueEntity struct {
	Id          string `db.keys:"pk"`
	EmptyString string `db.keys:"sk.0"`
	ZeroInt     int    `db.keys:"sk.1"` // Zero (0) treated as "not set" -> empty segment
	OptionalPtr *string
	RequiredStr string
}

func (e *EmptyValueEntity) TypeName() string             { return "EmptyValueEntity" }
func (e *EmptyValueEntity) NewQueryable() data.Queryable { return &EmptyValueEntities{} }

type EmptyValueEntities struct {
	data.BaseQueryable
	Id string
}

func (q *EmptyValueEntities) TypeName() string { return "EmptyValueEntity" }

// NumericKeyEntity - demonstrates numeric key handling
// WARNING: Zero values (0) treated as "not set", sort order is lexicographic not numeric
type NumericKeyEntity struct {
	Id      int `db.keys:"pk"` // Numeric PK - converted to string
	Version int `db.keys:"sk"` // Numeric SK - converted to string
	Data    string
}

func (n *NumericKeyEntity) TypeName() string             { return "NumericKeyEntity" }
func (n *NumericKeyEntity) NewQueryable() data.Queryable { return &NumericKeyEntities{} }

type NumericKeyEntities struct {
	data.BaseQueryable
	Id int
}

func (q *NumericKeyEntities) TypeName() string { return "NumericKeyEntity" }

// EscapedValueEntity - demonstrates separator escaping in keys
// Separator '#' becomes '$#', escape char '$' becomes '$$'
type EscapedValueEntity struct {
	Id              string `db.keys:"pk"`
	FieldWithHash   string `db.keys:"sk.0"` // Value may contain '#'
	FieldWithDollar string `db.keys:"sk.1"` // Value may contain '$'
	NormalField     string
}

func (e *EscapedValueEntity) TypeName() string             { return "EscapedValueEntity" }
func (e *EscapedValueEntity) NewQueryable() data.Queryable { return &EscapedValueEntities{} }

type EscapedValueEntities struct {
	data.BaseQueryable
	Id string
}

func (q *EscapedValueEntities) TypeName() string { return "EscapedValueEntity" }

// PointerKeyEntity - demonstrates pointer fields in keys
type PointerKeyEntity struct {
	Id      *string `db.keys:"pk"`
	SortVal *int    `db.keys:"sk"`
	Data    string
}

func (p *PointerKeyEntity) TypeName() string             { return "PointerKeyEntity" }
func (p *PointerKeyEntity) NewQueryable() data.Queryable { return &PointerKeyEntities{} }

type PointerKeyEntities struct {
	data.BaseQueryable
}

func (q *PointerKeyEntities) TypeName() string { return "PointerKeyEntity" }

// MappedFieldEntity - demonstrates db.name tag mapping
type MappedFieldEntity struct {
	Id       string `db.keys:"pk"`
	Username string `db.name:"user_name"`
	Email    string `db.name:"email_addr"`
	FullName string
}

func (m *MappedFieldEntity) TypeName() string             { return "MappedFieldEntity" }
func (m *MappedFieldEntity) NewQueryable() data.Queryable { return &MappedFieldEntities{} }

type MappedFieldEntities struct {
	data.BaseQueryable
}

func (q *MappedFieldEntities) TypeName() string { return "MappedFieldEntity" }

// EntityWithExclusions - demonstrates db.name:"-" exclusion
type EntityWithExclusions struct {
	Id          string `db.keys:"pk"`
	Name        string
	Password    string `db.name:"-"`
	CachedValue int    `db.name:"-"`
}

func (e *EntityWithExclusions) TypeName() string             { return "EntityWithExclusions" }
func (e *EntityWithExclusions) NewQueryable() data.Queryable { return &EntityWithExclusionsQuery{} }

type EntityWithExclusionsQuery struct {
	data.BaseQueryable
}

func (q *EntityWithExclusionsQuery) TypeName() string { return "EntityWithExclusions" }

// TimeSeriesEvent - demonstrates composite sort key with ascending/descending at different levels
// Used to test wildcard sort order behavior
type TimeSeriesEvent struct {
	DeviceId  string `db.keys:"pk"`
	Year      string `db.keys:"+sk.0"` // Ascending
	Month     string `db.keys:"+sk.1"` // Ascending
	DayDetail string `db.keys:"-sk.2"` // Descending - controls sort when Year+Month provided
	Value     string
}

func (e *TimeSeriesEvent) TypeName() string             { return "TimeSeriesEvent" }
func (e *TimeSeriesEvent) NewQueryable() data.Queryable { return &TimeSeriesEvents{} }

type TimeSeriesEvents struct {
	data.BaseQueryable
	DeviceId  string
	Year      string
	Month     string
	DayDetail string
}

func (q *TimeSeriesEvents) TypeName() string { return "TimeSeriesEvent" }
