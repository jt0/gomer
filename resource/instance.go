package resource

import (
	"context"
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/log"
)

// NewInstance creates a new instance of type I.
// Retrieves the Registry from context automatically.
func NewInstance[I Instance[I]](ctx context.Context, sub auth.Subject) I {
	if r, _ := ctx.Value(RegistryCtxKey).(*Registry); r == nil {
		panic("no registry in context")
	} else if rt := r.registeredTypes[reflect.TypeFor[I]()]; rt == nil {
		panic("type not registered: " + reflect.TypeFor[I]().String())
	} else {
		return rt.newInstance(sub).(I)
	}
}

// Instance extends Resource for individual entities. Instances have an identity (Id) and support CRUD operations.
type Instance[I Resource[I]] interface {
	Resource[I]
	data.Persistable // TypeName() string, NewQueryable() data.Queryable
	Id() string
	PreCreate(context.Context) gomerr.Gomerr
	RetryCreate(context.Context, gomerr.Gomerr) gomerr.Gomerr
	PostCreate(context.Context) gomerr.Gomerr
	CreateError(context.Context, gomerr.Gomerr) gomerr.Gomerr
	PreRead(context.Context) gomerr.Gomerr
	RetryRead(context.Context, gomerr.Gomerr) gomerr.Gomerr
	PostRead(context.Context) gomerr.Gomerr
	ReadError(context.Context, gomerr.Gomerr) gomerr.Gomerr
	PreUpdate(context.Context, I) gomerr.Gomerr
	RetryUpdate(context.Context, I, gomerr.Gomerr) gomerr.Gomerr
	PostUpdate(context.Context, I) gomerr.Gomerr
	UpdateError(context.Context, gomerr.Gomerr) gomerr.Gomerr
	PreDelete(context.Context) gomerr.Gomerr
	RetryDelete(context.Context, gomerr.Gomerr) gomerr.Gomerr
	PostDelete(context.Context) gomerr.Gomerr
	DeleteError(context.Context, gomerr.Gomerr) gomerr.Gomerr
	PreList(context.Context) gomerr.Gomerr
	ListError(context.Context, gomerr.Gomerr) gomerr.Gomerr
}

// BaseInstance provides the default implementation for Instance[I]. Embed this in concrete instance types.
type BaseInstance[I Instance[I]] struct {
	BaseResource[I]
}

func (b *BaseInstance[I]) TypeName() string {
	return b.rt.instanceName
}

func (b *BaseInstance[I]) Id() string {
	instanceId, ge := id(reflect.ValueOf(b.self).Elem())
	if ge != nil {
		log.Logger().Warn("unable to resolve id", "type", b.TypeName(), "error", ge)
		return ""
	}
	return instanceId
}

func (b *BaseInstance[I]) Ids() []string {
	instanceIdChain, ge := ids(reflect.ValueOf(b.self).Elem())
	if ge != nil {
		log.Logger().Warn("unable to resolve ids", "type", b.TypeName(), "error", ge)
		return nil
	}
	return instanceIdChain
}

// NewQueryable creates a Collection for querying instances of this type.
// Implements data.Persistable.
func (b *BaseInstance[I]) NewQueryable() data.Queryable {
	return b.rt.newCollection(b.rt.newInstance(b.sub)).(data.Queryable)
}

// Instance action hooks - override these in concrete types as needed.

func (*BaseInstance[I]) PreCreate(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) RetryCreate(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (*BaseInstance[I]) PostCreate(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) CreateError(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (*BaseInstance[I]) PreRead(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) RetryRead(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (*BaseInstance[I]) PostRead(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) ReadError(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (*BaseInstance[I]) PreUpdate(context.Context, I) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) RetryUpdate(_ context.Context, _ I, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (*BaseInstance[I]) PostUpdate(context.Context, I) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) UpdateError(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (*BaseInstance[I]) PreDelete(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) RetryDelete(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (*BaseInstance[I]) PostDelete(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) DeleteError(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (*BaseInstance[I]) PreList(context.Context) gomerr.Gomerr {
	return nil
}

func (*BaseInstance[I]) ListError(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

// CRUD convenience methods

func (b *BaseInstance[I]) Create(ctx context.Context) gomerr.Gomerr {
	_, ge := b.DoAction(ctx, CreateAction[I]())
	return ge
}

func (b *BaseInstance[I]) Read(ctx context.Context) gomerr.Gomerr {
	_, ge := b.DoAction(ctx, ReadAction[I]())
	return ge
}

func (b *BaseInstance[I]) Update(ctx context.Context) gomerr.Gomerr {
	_, ge := b.DoAction(ctx, UpdateAction[I](nil))
	return ge
}

func (b *BaseInstance[I]) Delete(ctx context.Context) gomerr.Gomerr {
	_, ge := b.DoAction(ctx, DeleteAction[I]())
	return ge
}
