package resource

import (
	"context"
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

// NewInstance creates a new instance of type I.
// Retrieves the Registry from context automatically.
func NewInstance[I Instance[I]](ctx context.Context, sub auth.Subject) (I, gomerr.Gomerr) {
	var zero I
	if r, _ := ctx.Value(RegistryCtxKey).(*Registry); r == nil {
		return zero, gomerr.Configuration("no registry in context")
	} else if rt := r.registeredTypes[reflect.TypeFor[I]()]; rt == nil {
		return zero, gomerr.Unprocessable("unknown instance type", reflect.TypeFor[I]())
	} else {
		return rt.newInstance(sub).(I), nil
	}
}

// Instance extends Resource for individual entities. Instances have an identity (Id) and support CRUD operations.
type Instance[I Resource[I]] interface {
	Resource[I]
	data.Persistable // TypeName() string, NewQueryable() data.Queryable
	Id() string
	PreCreate(context.Context) gomerr.Gomerr
	PostCreate(context.Context) gomerr.Gomerr
	PreRead(context.Context) gomerr.Gomerr
	PostRead(context.Context) gomerr.Gomerr
	PreUpdate(context.Context, I) gomerr.Gomerr
	PostUpdate(context.Context, I) gomerr.Gomerr
	PreDelete(context.Context) gomerr.Gomerr
	PostDelete(context.Context) gomerr.Gomerr
}

// BaseInstance provides the default implementation for Instance[I]. Embed this in concrete instance types.
type BaseInstance[I Instance[I]] struct {
	BaseResource[I]
}

func (b *BaseInstance[I]) TypeName() string {
	return b.rt.instanceName
}

func (b *BaseInstance[I]) Id() string {
	id, ge := Id(reflect.ValueOf(b.self).Elem())
	if ge != nil {
		return ""
	}
	return id
}

// NewQueryable creates a Collection for querying instances of this type.
// Implements data.Persistable.
func (b *BaseInstance[I]) NewQueryable() data.Queryable {
	return b.rt.newCollection(b.rt.newInstance(b.sub)).(data.Queryable)
}

// Action lifecycle hooks - override these in concrete types as needed.

func (*BaseInstance[I]) PreCreate(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) PostCreate(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) PreRead(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) PostRead(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) PreUpdate(context.Context, I) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) PostUpdate(context.Context, I) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) PreDelete(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) PostDelete(context.Context) gomerr.Gomerr {
	return nil
}

// CRUD convenience methods

func (b *BaseInstance[I]) Create(ctx context.Context) (I, gomerr.Gomerr) {
	return b.DoAction(ctx, CreateAction[I]())
}

func (b *BaseInstance[I]) Read(ctx context.Context) (I, gomerr.Gomerr) {
	return b.DoAction(ctx, ReadAction[I]())
}

func (b *BaseInstance[I]) Update(ctx context.Context) (I, gomerr.Gomerr) {
	return b.DoAction(ctx, UpdateAction[I]())
}

func (b *BaseInstance[I]) Delete(ctx context.Context) (I, gomerr.Gomerr) {
	return b.DoAction(ctx, DeleteAction[I]())
}
