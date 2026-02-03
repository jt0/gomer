package resource

import (
	"context"
	"errors"
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data/dataerr"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/limit"
	"github.com/jt0/gomer/structs"
)

// IdTool contains the configured tool to copy ids. It's initialized to id.DefaultIdFieldTool, but can be replaced
// if preferred.
var IdTool = NewIdTool(structs.StructTagDirectiveProvider{"id"})

func init() {
	// This sets up default aliases for each of the Actions defined here. An application can add other alias values or
	// can clear any out by calling ScopeAlias with the undesired alias and an empty string scope value.
	structs.ScopeAlias("create", CreateAction().Name())
	structs.ScopeAlias("read", ReadAction().Name())
	structs.ScopeAlias("update", UpdateAction().Name())
	structs.ScopeAlias("delete", DeleteAction().Name())
	structs.ScopeAlias("list", ListAction().Name())
}

type Creatable interface {
	Instance
	PreCreate(context.Context) gomerr.Gomerr
	PostCreate(context.Context) gomerr.Gomerr
}

type OnCreateFailer interface {
	OnCreateFailure(context.Context, gomerr.Gomerr) gomerr.Gomerr
}

func CreateAction() Action {
	return &createAction{}
}

type createAction struct {
	limiter limit.Limiter
}

func (*createAction) Name() string {
	return "resource.CreateAction"
}

func (*createAction) AppliesToCategory() Category {
	return InstanceCategory
}

func (*createAction) FieldAccessPermissions() auth.AccessPermissions {
	return auth.CreatePermission
}

func (*createAction) Pre(ctx context.Context, r Resource) gomerr.Gomerr {
	creatable, ok := r.(Creatable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Creatable", r)
	}

	return creatable.PreCreate(ctx)
}

func (a *createAction) Do(ctx context.Context, r Resource) (ge gomerr.Gomerr) {
	a.limiter, ge = applyLimitAction(ctx, checkAndIncrement, r)
	if ge != nil {
		return ge
	}

	return r.metadata().dataStore.Create(ctx, r.(Creatable))
}

func (a *createAction) OnDoSuccess(ctx context.Context, r Resource) (Resource, gomerr.Gomerr) {
	defer saveLimiterIfDirty(ctx, a.limiter)

	return r, r.(Creatable).PostCreate(ctx)
}

func (*createAction) OnDoFailure(ctx context.Context, r Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := r.(OnCreateFailer); ok {
		return failer.OnCreateFailure(ctx, ge)
	}

	return ge
}

type Readable interface {
	Instance
	PreRead(ctx context.Context) gomerr.Gomerr
	PostRead(ctx context.Context) gomerr.Gomerr
}

type OnReadFailer interface {
	OnReadFailure(ctx context.Context, ge gomerr.Gomerr) gomerr.Gomerr
}

func ReadAction() Action {
	return readAction{}
}

type readAction struct{}

func (readAction) Name() string {
	return "resource.ReadAction"
}

func (readAction) AppliesToCategory() Category {
	return InstanceCategory
}

func (readAction) FieldAccessPermissions() auth.AccessPermissions {
	return auth.ReadPermission
}

func (readAction) Pre(ctx context.Context, r Resource) gomerr.Gomerr {
	readable, ok := r.(Readable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Readable", r)
	}

	return readable.PreRead(ctx)
}

func (readAction) Do(ctx context.Context, r Resource) (ge gomerr.Gomerr) {
	return r.metadata().dataStore.Read(ctx, r.(Readable))
}

func (readAction) OnDoSuccess(ctx context.Context, r Resource) (Resource, gomerr.Gomerr) {
	return r, r.(Readable).PostRead(ctx)
}

func (readAction) OnDoFailure(ctx context.Context, r Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := r.(OnReadFailer); ok {
		return failer.OnReadFailure(ctx, ge)
	}

	return convertPersistableNotFoundIfApplicable(r.(Readable), ge)
}

type Updatable interface {
	Instance
	PreUpdate(ctx context.Context, update Resource) gomerr.Gomerr
	PostUpdate(ctx context.Context, update Resource) gomerr.Gomerr
}

type OnUpdateFailer interface {
	OnUpdateFailure(ctx context.Context, ge gomerr.Gomerr) gomerr.Gomerr
}

func UpdateAction() Action {
	return &updateAction{}
}

type updateAction struct {
	actual Updatable
}

func (*updateAction) Name() string {
	return "resource.UpdateAction"
}

func (*updateAction) AppliesToCategory() Category {
	return InstanceCategory
}

func (*updateAction) FieldAccessPermissions() auth.AccessPermissions {
	return auth.UpdatePermission
}

func (a *updateAction) Pre(ctx context.Context, update Resource) gomerr.Gomerr {
	d, _ := ctx.Value(DomainCtxKey).(*Domain)
	r, ge := d.NewResource(reflect.TypeOf(update), update.Subject(ctx))
	if ge != nil {
		return ge
	}
	current, ok := r.(Updatable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Updatable", update)
	}

	// Get the id fields from the update
	tc := structs.EnsureContext().With(SourceValue, reflect.ValueOf(update).Elem())
	if ge = structs.ApplyTools(current, tc, IdTool); ge != nil {
		return ge
	}

	// Populate other fields with data from the underlying store
	if ge = current.metadata().dataStore.Read(ctx, current); ge != nil {
		return ge
	}

	a.actual = current

	return current.PreUpdate(ctx, update)
}

func (a *updateAction) Do(ctx context.Context, update Resource) (ge gomerr.Gomerr) {
	return update.metadata().dataStore.Update(ctx, a.actual, update.(Updatable))
}

func (a *updateAction) OnDoSuccess(ctx context.Context, update Resource) (Resource, gomerr.Gomerr) {
	return a.actual, a.actual.PostUpdate(ctx, update)
}

func (a *updateAction) OnDoFailure(ctx context.Context, update Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := a.actual.(OnUpdateFailer); ok {
		return failer.OnUpdateFailure(ctx, ge)
	}

	return convertPersistableNotFoundIfApplicable(update.(Updatable), ge)
}

type Deletable interface {
	Instance
	PreDelete(ctx context.Context) gomerr.Gomerr
	PostDelete(ctx context.Context) gomerr.Gomerr
}

type OnDeleteFailer interface {
	OnDeleteFailure(ctx context.Context, ge gomerr.Gomerr) gomerr.Gomerr
}

func DeleteAction() Action {
	return &deleteAction{}
}

type deleteAction struct {
	limiter limit.Limiter
}

func (*deleteAction) Name() string {
	return "resource.DeleteAction"
}

func (*deleteAction) AppliesToCategory() Category {
	return InstanceCategory
}

func (*deleteAction) FieldAccessPermissions() auth.AccessPermissions {
	return auth.NoPermissions
}

func (*deleteAction) Pre(ctx context.Context, r Resource) gomerr.Gomerr {
	deletable, ok := r.(Deletable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Deletable", r)
	}

	return deletable.PreDelete(ctx)
}

func (a *deleteAction) Do(ctx context.Context, r Resource) (ge gomerr.Gomerr) {
	a.limiter, ge = applyLimitAction(ctx, decrement, r)
	if ge != nil {
		return ge
	}

	return r.metadata().dataStore.Delete(ctx, r.(Deletable))
}

func (a *deleteAction) OnDoSuccess(ctx context.Context, r Resource) (Resource, gomerr.Gomerr) {
	defer saveLimiterIfDirty(ctx, a.limiter)

	// If we made it this far, we know r is a Deletable
	return r, r.(Deletable).PostDelete(ctx)
}

func (*deleteAction) OnDoFailure(ctx context.Context, r Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := r.(OnDeleteFailer); ok {
		return failer.OnDeleteFailure(ctx, ge)
	}

	return convertPersistableNotFoundIfApplicable(r.(Deletable), ge)
}

type Listable interface {
	Collection
	PreList(ctx context.Context) gomerr.Gomerr
	PostList(ctx context.Context) gomerr.Gomerr
}

type Collectible interface {
	OnCollect(ctx context.Context, r Resource) gomerr.Gomerr
}

type OnListFailer interface {
	OnListFailure(ctx context.Context, ge gomerr.Gomerr) gomerr.Gomerr
}

func ListAction() Action {
	return listAction{}
}

type listAction struct{}

func (listAction) Name() string {
	return "resource.ListAction"
}

func (listAction) AppliesToCategory() Category {
	return CollectionCategory
}

func (listAction) FieldAccessPermissions() auth.AccessPermissions {
	return auth.WritePermissions // 'Write' because we're creating a query, not creating a resource per se
}

func (listAction) Pre(ctx context.Context, r Resource) gomerr.Gomerr {
	listable, ok := r.(Listable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Listable", r)
	}

	return listable.PreList(ctx)
}

func (listAction) Do(ctx context.Context, r Resource) gomerr.Gomerr {
	if ge := r.metadata().dataStore.Query(ctx, r.(Listable)); ge != nil {
		return ge
	}

	for _, elem := range r.(Listable).Items() {
		item := elem.(Resource)
		item.setSelf(item)
		item.setMetadata(r.metadata())
		item.setSubject(r.Subject(ctx))

		if collectible, ok := item.(Collectible); ok {
			if ge := collectible.OnCollect(ctx, r); ge != nil {
				return ge
			}
		}
	}

	return nil
}

func (listAction) OnDoSuccess(ctx context.Context, r Resource) (Resource, gomerr.Gomerr) {
	return r, r.(Listable).PostList(ctx)
}

func (listAction) OnDoFailure(ctx context.Context, r Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := r.(OnListFailer); ok {
		return failer.OnListFailure(ctx, ge)
	}

	return ge
}

type NoOpAction struct{}

func (NoOpAction) Name() string {
	return "resource.NoOpAction"
}

func (NoOpAction) FieldAccessPermissions() auth.AccessPermissions {
	return auth.NoPermissions
}

func (NoOpAction) Pre(_ context.Context, _ Resource) gomerr.Gomerr {
	return nil
}

func (NoOpAction) Do(_ context.Context, _ Resource) gomerr.Gomerr {
	return nil
}

func (NoOpAction) OnDoSuccess(_ context.Context, r Resource) (Resource, gomerr.Gomerr) {
	return r, nil
}

func (NoOpAction) OnDoFailure(_ context.Context, _ Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

var persistableNotFound = &dataerr.PersistableNotFoundError{}

func convertPersistableNotFoundIfApplicable(i Instance, ge gomerr.Gomerr) gomerr.Gomerr {
	if !errors.Is(ge, persistableNotFound) {
		return ge
	}

	return gomerr.NotFound(i.metadata().instanceName, i.Id()).Wrap(ge)
}
