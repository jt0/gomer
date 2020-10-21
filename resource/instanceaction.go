package resource

import (
	"reflect"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
	"github.com/jt0/gomer/limit"
	"github.com/jt0/gomer/util"
)

type InstanceAction interface {
	Pre(Instance) gomerr.Gomerr
	Do(Instance) gomerr.Gomerr
	OnDoSuccess(Instance) (interface{}, gomerr.Gomerr)
	OnDoFailure(Instance, gomerr.Gomerr) gomerr.Gomerr
}

func DoInstanceAction(i Instance, instanceAction InstanceAction) (interface{}, gomerr.Gomerr) {
	if ge := instanceAction.Pre(i); ge != nil {
		return nil, ge
	}

	if ge := instanceAction.Do(i); ge != nil {
		return nil, instanceAction.OnDoFailure(i, ge)
	}

	output, ge := instanceAction.OnDoSuccess(i)
	if ge != nil {
		return nil, ge
	}

	return render(output)
}

type Creatable interface {
	PreCreate() gomerr.Gomerr
	PostCreate() gomerr.Gomerr
}

type OnCreateFailer interface {
	OnCreateFailure(gomerr.Gomerr) gomerr.Gomerr
}

func CreateInstance() InstanceAction {
	return &createAction{}
}

type createAction struct {
	limiter limit.Limiter
}

func (*createAction) Pre(i Instance) gomerr.Gomerr {
	iv := reflect.ValueOf(i).Elem() // Support non-pointer types?

	if ge := i.metadata().fields.RemoveNonWritable(iv, fields.CreateAccess, i.Subject().Principal(fields.FieldAccess).(fields.AccessPrincipal)); ge != nil {
		return ge
	}

	if ge := i.metadata().fields.ApplyDefaults(iv); ge != nil {
		return ge
	}

	if ge := i.metadata().fields.Validate(iv, "create"); ge != nil {
		return ge
	}

	creatable, ok := i.(Creatable)
	if !ok {
		return gomerr.Unprocessable("Instance", i, constraint.TypeOf(creatable))
	}

	return creatable.PreCreate()
}

func (a *createAction) Do(i Instance) (ge gomerr.Gomerr) {
	a.limiter, ge = applyLimitAction(checkAndIncrement, i)
	if ge != nil {
		return ge
	}

	return i.metadata().dataStore.Create(i)
}

func (a *createAction) OnDoSuccess(i Instance) (interface{}, gomerr.Gomerr) {
	defer saveLimiterIfDirty(a.limiter)

	// If we made it this far, we know i is a Creatable
	return i, i.(Creatable).PostCreate()
}

func (*createAction) OnDoFailure(i Instance, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := i.(OnCreateFailer); ok {
		return failer.OnCreateFailure(ge)
	}

	return ge
}

type Readable interface {
	PreRead() gomerr.Gomerr
	PostRead() gomerr.Gomerr
}

type OnReadFailer interface {
	OnReadFailure(gomerr2 gomerr.Gomerr) gomerr.Gomerr
}

func ReadInstance() InstanceAction {
	return readActionSingleton
}

var readActionSingleton = &readAction{}

type readAction struct{}

func (a readAction) Pre(i Instance) gomerr.Gomerr {
	if ge := i.metadata().fields.Validate(reflect.ValueOf(i).Elem(), "read"); ge != nil {
		return ge
	}

	readable, ok := i.(Readable)
	if !ok {
		return gomerr.Unprocessable("Instance", i, constraint.TypeOf(readable))
	}

	return readable.PreRead()
}

func (a readAction) Do(i Instance) (ge gomerr.Gomerr) {
	return i.metadata().dataStore.Read(i)
}

func (a readAction) OnDoSuccess(i Instance) (interface{}, gomerr.Gomerr) {
	// If we made it this far, we know i is a Readable
	return i, i.(Readable).PostRead()
}

func (a readAction) OnDoFailure(i Instance, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := i.(OnReadFailer); ok {
		return failer.OnReadFailure(ge)
	}

	return ge
}

type Updatable interface {
	Instance
	PreUpdate(updateInstance Instance) gomerr.Gomerr
	PostUpdate(updateInstance Instance) gomerr.Gomerr
}

type OnUpdateFailer interface {
	OnUpdateFailure(gomerr2 gomerr.Gomerr) gomerr.Gomerr
}

func UpdateInstance() InstanceAction {
	return &updateAction{}
}

type updateAction struct {
	actual Updatable
}

func (a *updateAction) Pre(ui Instance) gomerr.Gomerr {
	uv := reflect.ValueOf(ui).Elem()

	// Clear out the update's non-writable fields (will keep 'provided' fields)
	if ge := ui.metadata().fields.RemoveNonWritable(uv, fields.UpdateAccess, ui.Subject().Principal(fields.FieldAccess).(fields.AccessPrincipal)); ge != nil {
		return ge
	}

	if ge := ui.metadata().fields.Validate(uv, "update"); ge != nil {
		return ge
	}

	i, ge := NewInstance(util.UnqualifiedTypeName(ui), ui.Subject())
	if ge != nil {
		return ge
	}

	if ge := i.metadata().fields.CopyProvided(uv, reflect.ValueOf(i).Elem()); ge != nil {
		return ge
	}

	// Populate other fields with data uv the underlying store
	if ge := i.metadata().dataStore.Read(i); ge != nil {
		return ge
	}

	actual, ok := i.(Updatable)
	if !ok {
		return gomerr.Unprocessable("Instance", ui, constraint.TypeOf(actual))
	}

	a.actual = actual

	return actual.PreUpdate(ui)
}

func (a *updateAction) Do(ui Instance) (ge gomerr.Gomerr) {
	return ui.metadata().dataStore.Update(a.actual, ui)
}

func (a *updateAction) OnDoSuccess(ui Instance) (interface{}, gomerr.Gomerr) {
	return a.actual, a.actual.PostUpdate(ui)
}

func (a *updateAction) OnDoFailure(_ Instance, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := a.actual.(OnUpdateFailer); ok {
		return failer.OnUpdateFailure(ge)
	}

	return ge
}

type Deletable interface {
	PreDelete() gomerr.Gomerr
	PostDelete() gomerr.Gomerr
}

type OnDeleteFailer interface {
	OnDeleteFailure(gomerr2 gomerr.Gomerr) gomerr.Gomerr
}

func DeleteInstance() InstanceAction {
	return &deleteAction{}
}

type deleteAction struct {
	limiter limit.Limiter
}

func (*deleteAction) Pre(i Instance) gomerr.Gomerr {
	if ge := i.metadata().fields.Validate(reflect.ValueOf(i).Elem(), "delete"); ge != nil {
		return ge
	}

	deletable, ok := i.(Deletable)
	if !ok {
		return gomerr.Unprocessable("Instance", i, constraint.TypeOf(deletable))
	}

	return deletable.PreDelete()
}

func (a *deleteAction) Do(i Instance) (ge gomerr.Gomerr) {
	a.limiter, ge = applyLimitAction(decrement, i)
	if ge != nil {
		return ge
	}

	return i.metadata().dataStore.Delete(i)
}

func (a *deleteAction) OnDoSuccess(i Instance) (interface{}, gomerr.Gomerr) {
	defer saveLimiterIfDirty(a.limiter)

	// If we made it this far, we know i is a Deletable
	return i, i.(Deletable).PostDelete()
}

func (*deleteAction) OnDoFailure(i Instance, ge gomerr.Gomerr) gomerr.Gomerr {
	if failer, ok := i.(OnDeleteFailer); ok {
		return failer.OnDeleteFailure(ge)
	}

	return ge
}

type Renderer interface {
	Render() (interface{}, gomerr.Gomerr)
}

type NoContentRenderer struct{}

func (NoContentRenderer) Render() (interface{}, gomerr.Gomerr) {
	return nil, nil
}

func render(i interface{}) (interface{}, gomerr.Gomerr) {
	switch t := i.(type) {
	case Renderer:
		return t.Render()
	case Instance:
		if result := t.metadata().fields.RemoveNonReadable(reflect.ValueOf(t).Elem(), t.Subject().Principal(fields.FieldAccess).(fields.AccessPrincipal)); result == nil || len(result) == 0 {
			return nil, gomerr.NotFound(t.metadata().instanceName, t.Id())
		} else {
			return result, nil
		}
	default:
		return i, nil
	}
}

type NoOpInstanceAction struct{}

func (NoOpInstanceAction) Pre(Instance) gomerr.Gomerr {
	return nil
}

func (NoOpInstanceAction) Do(Instance) gomerr.Gomerr {
	return nil
}

func (NoOpInstanceAction) OnDoSuccess(Instance) (interface{}, gomerr.Gomerr) {
	return nil, nil
}

func (NoOpInstanceAction) OnDoFailure(_ Instance, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}
