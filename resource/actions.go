package resource

import (
	"errors"
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data/dataerr"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/limit"
)

type Creatable interface {
	Instance
	PreCreate() gomerr.Gomerr
	PostCreate() gomerr.Gomerr
}

type OnCreateFailer interface {
	OnCreateFailure(gomerr.Gomerr) gomerr.Gomerr
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

func (*createAction) ResourceType() Type {
	return InstanceType
}

func (*createAction) Pre(r Resource) gomerr.Gomerr {
	creatable, ok := r.(Creatable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Creatable", r)
	}

	return creatable.PreCreate()
}

func (a *createAction) Do(r Resource) (ge gomerr.Gomerr) {
	a.limiter, ge = applyLimitAction(checkAndIncrement, r)
	if ge != nil {
		return ge
	}

	return r.metadata().dataStore.Create(r.(Creatable))
}

func (a *createAction) OnDoSuccess(r Resource) (Resource, gomerr.Gomerr) {
	defer saveLimiterIfDirty(a.limiter)

	return r, r.(Creatable).PostCreate()
}

func (*createAction) OnDoFailure(r Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := r.(OnCreateFailer); ok {
		return failer.OnCreateFailure(ge)
	}

	return ge
}

type Readable interface {
	Instance
	PreRead() gomerr.Gomerr
	PostRead() gomerr.Gomerr
}

type OnReadFailer interface {
	OnReadFailure(gomerr.Gomerr) gomerr.Gomerr
}

func ReadAction() Action {
	return readAction{}
}

type readAction struct{}

func (readAction) Name() string {
	return "resource.ReadAction"
}

func (readAction) ResourceType() Type {
	return InstanceType
}

func (readAction) Pre(r Resource) gomerr.Gomerr {
	readable, ok := r.(Readable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Readable", r)
	}

	return readable.PreRead()
}

func (readAction) Do(r Resource) (ge gomerr.Gomerr) {
	return r.metadata().dataStore.Read(r.(Readable))
}

func (readAction) OnDoSuccess(r Resource) (Resource, gomerr.Gomerr) {
	return r, r.(Readable).PostRead()
}

func (readAction) OnDoFailure(r Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := r.(OnReadFailer); ok {
		return failer.OnReadFailure(ge)
	}

	return convertPersistableNotFoundIfApplicable(r.(Readable), ge)
}

type Updatable interface {
	Instance
	PreUpdate(update Resource) gomerr.Gomerr
	PostUpdate(update Resource) gomerr.Gomerr
}

type OnUpdateFailer interface {
	OnUpdateFailure(gomerr.Gomerr) gomerr.Gomerr
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

func (*updateAction) ResourceType() Type {
	return InstanceType
}

func (a *updateAction) Pre(update Resource) gomerr.Gomerr {
	r, ge := New(reflect.TypeOf(update), update.Subject())
	if ge != nil {
		return ge
	}
	current, ok := r.(Updatable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Updatable", update)
	}

	tool := fields.ToolWithContext{auth.FieldAccessTool, auth.AddCopyProvidedToContext(reflect.ValueOf(update).Elem())}
	if ge := current.ApplyTools(tool); ge != nil {
		return ge
	}

	// Populate other fields with data uv the underlying store
	if ge := current.metadata().dataStore.Read(current); ge != nil {
		return ge
	}

	a.actual = current

	return current.PreUpdate(update)
}

func (a *updateAction) Do(update Resource) (ge gomerr.Gomerr) {
	return update.metadata().dataStore.Update(a.actual, update.(Updatable))
}

func (a *updateAction) OnDoSuccess(update Resource) (Resource, gomerr.Gomerr) {
	return a.actual, a.actual.PostUpdate(update)
}

func (a *updateAction) OnDoFailure(update Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := a.actual.(OnUpdateFailer); ok {
		return failer.OnUpdateFailure(ge)
	}

	return convertPersistableNotFoundIfApplicable(update.(Updatable), ge)
}

type Deletable interface {
	Instance
	PreDelete() gomerr.Gomerr
	PostDelete() gomerr.Gomerr
}

type OnDeleteFailer interface {
	OnDeleteFailure(gomerr.Gomerr) gomerr.Gomerr
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

func (*deleteAction) ResourceType() Type {
	return InstanceType
}

func (*deleteAction) Pre(r Resource) gomerr.Gomerr {
	deletable, ok := r.(Deletable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Deletable", r)
	}

	return deletable.PreDelete()
}

func (a *deleteAction) Do(r Resource) (ge gomerr.Gomerr) {
	a.limiter, ge = applyLimitAction(decrement, r)
	if ge != nil {
		return ge
	}

	return r.metadata().dataStore.Delete(r.(Deletable))
}

func (a *deleteAction) OnDoSuccess(r Resource) (Resource, gomerr.Gomerr) {
	defer saveLimiterIfDirty(a.limiter)

	// If we made it this far, we know r is a Deletable
	return r, r.(Deletable).PostDelete()
}

func (*deleteAction) OnDoFailure(r Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := r.(OnDeleteFailer); ok {
		return failer.OnDeleteFailure(ge)
	}

	return convertPersistableNotFoundIfApplicable(r.(Deletable), ge)
}

type Queryable interface {
	Collection
	PreQuery() gomerr.Gomerr
	PostQuery() gomerr.Gomerr
}

type Collectible interface {
	OnCollect(Resource) gomerr.Gomerr
}

type OnQueryFailer interface {
	OnQueryFailure(gomerr.Gomerr) gomerr.Gomerr
}

func QueryAction() Action {
	return queryAction{}
}

type queryAction struct{}

func (queryAction) Name() string {
	return "resource.QueryAction"
}

func (queryAction) ResourceType() Type {
	return CollectionType
}

func (queryAction) Pre(r Resource) gomerr.Gomerr {
	queryable, ok := r.(Queryable)
	if !ok {
		return gomerr.Unprocessable("Type does not implement resource.Queryable", r)
	}

	return queryable.PreQuery()
}

func (queryAction) Do(r Resource) (ge gomerr.Gomerr) {
	if ge := r.metadata().dataStore.Query(r.(Queryable)); ge != nil {
		return ge
	}

	tc := fields.EnsureContext()
	for _, elem := range r.(Queryable).Items() {
		item := elem.(Resource)
		item.setSelf(item)
		item.setMetadata(r.metadata())
		item.setSubject(r.Subject())

		tool := fields.ToolWithContext{auth.FieldAccessTool, auth.AddCopyProvidedToContext(reflect.ValueOf(r).Elem(), tc)}
		if ge := item.ApplyTools(tool); ge != nil {
			return ge
		}

		if collectible, ok := item.(Collectible); ok {
			ge := collectible.OnCollect(r)
			if ge != nil {
				return ge
			}
		}
	}

	return nil
}

func (queryAction) OnDoSuccess(r Resource) (Resource, gomerr.Gomerr) {
	return r, r.(Queryable).PostQuery()
}

func (queryAction) OnDoFailure(r Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := r.(OnQueryFailer); ok {
		return failer.OnQueryFailure(ge)
	}

	return ge
}

type NoOpAction struct{}

func (NoOpAction) Name() string {
	return "resource.NoOpAction"
}

func (NoOpAction) Pre(Resource) gomerr.Gomerr {
	return nil
}

func (NoOpAction) Do(Resource) gomerr.Gomerr {
	return nil
}

func (NoOpAction) OnDoSuccess(r Resource) (Resource, gomerr.Gomerr) {
	return r, nil
}

func (NoOpAction) OnDoFailure(_ Resource, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

var persistableNotFound = &dataerr.PersistableNotFoundError{}

func convertPersistableNotFoundIfApplicable(i Instance, ge gomerr.Gomerr) gomerr.Gomerr {
	if !errors.Is(ge, persistableNotFound) {
		return ge
	}

	return gomerr.NotFound(i.metadata().instanceName, i.Id()).Wrap(ge)
}
