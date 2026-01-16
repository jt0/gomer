package data

import (
	"context"

	"github.com/jt0/gomer/gomerr"
)

type Store interface {
	Create(ctx context.Context, p Persistable) gomerr.Gomerr
	Read(ctx context.Context, p Persistable) gomerr.Gomerr
	Update(ctx context.Context, p Persistable, update Persistable) gomerr.Gomerr
	Delete(ctx context.Context, p Persistable) gomerr.Gomerr
	Query(ctx context.Context, q Queryable) gomerr.Gomerr
}

type Persistable interface {
	TypeName() string
	NewQueryable() Queryable
}
