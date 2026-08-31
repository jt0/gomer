package resource

import (
	"context"
	"errors"
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data/dataerr"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

// IdTool is an alias for DefaultIdFieldTool for backward compatibility.
var IdTool = DefaultIdFieldTool

func init() {
	// This sets up default aliases for each of the Actions defined here. An application can add other alias values or
	// can clear any out by calling ScopeAlias with the undesired alias and an empty string scope value.
	structs.ScopeAlias("create", "resource.CreateAction")
	structs.ScopeAlias("read", "resource.ReadAction")
	structs.ScopeAlias("update", "resource.UpdateAction")
	structs.ScopeAlias("delete", "resource.DeleteAction")
	structs.ScopeAlias("list", "resource.ListAction")
}

type AnyAction interface {
	Name() string
	AppliesToCategory() Category
	FieldAccessPermissions() auth.AccessPermissions
	ExecuteOn(ctx context.Context, resource any) (any, gomerr.Gomerr)
}

// Action defines an operation that can be performed on a resource.
type Action[T any] interface {
	AnyAction
	Pre(context.Context, T) gomerr.Gomerr
	Retry(context.Context, T, gomerr.Gomerr) gomerr.Gomerr
	Do(context.Context, T) gomerr.Gomerr
	OnDoSuccess(context.Context, T) (T, gomerr.Gomerr)
	OnDoFailure(context.Context, T, gomerr.Gomerr) gomerr.Gomerr
}

// CreateAction returns an action for creating instances.
func CreateAction[I Instance[I]]() Action[I] {
	return &createAction[I]{}
}

type createAction[I Instance[I]] struct{}

func (*createAction[I]) Name() string {
	return "resource.CreateAction"
}

func (*createAction[I]) AppliesToCategory() Category {
	return InstanceCategory
}

func (*createAction[I]) FieldAccessPermissions() auth.AccessPermissions {
	return auth.CreatePermission
}

func (*createAction[I]) Pre(ctx context.Context, i I) gomerr.Gomerr {
	return i.PreCreate(ctx)
}

func (*createAction[I]) Do(ctx context.Context, i I) gomerr.Gomerr {
	return i.registeredType().store.Create(ctx, i)
}

func (*createAction[I]) Retry(ctx context.Context, i I, ge gomerr.Gomerr) gomerr.Gomerr {
	return i.RetryCreate(ctx, ge)
}

func (*createAction[I]) OnDoSuccess(ctx context.Context, i I) (I, gomerr.Gomerr) {
	return i, i.PostCreate(ctx)
}

func (*createAction[I]) OnDoFailure(ctx context.Context, i I, ge gomerr.Gomerr) gomerr.Gomerr {
	return i.CreateError(ctx, ge)
}

func (a *createAction[T]) ExecuteOn(ctx context.Context, resource any) (any, gomerr.Gomerr) {
	return resource.(Resource[T]).DoAction(ctx, a)
}

// ReadAction returns an action for reading instances.
func ReadAction[I Instance[I]]() Action[I] {
	return &readAction[I]{}
}

type readAction[I Instance[I]] struct{}

func (*readAction[I]) Name() string {
	return "resource.ReadAction"
}

func (*readAction[I]) AppliesToCategory() Category {
	return InstanceCategory
}

func (*readAction[I]) FieldAccessPermissions() auth.AccessPermissions {
	return auth.ReadPermission
}

func (*readAction[I]) Pre(ctx context.Context, i I) gomerr.Gomerr {
	if ge := i.PreRead(ctx); ge != nil {
		return ge
	}

	// Prepare any Collection fields for nested queries
	registry, _ := ctx.Value(RegistryCtxKey).(*Registry)
	if registry == nil {
		return nil // No registry in context, skip auto-population
	}

	iv := reflect.ValueOf(i)
	if iv.Kind() == reflect.Pointer {
		iv = iv.Elem()
	}
	if iv.Kind() != reflect.Struct {
		return nil
	}

	it := iv.Type()
	for fieldNum := range it.NumField() {
		field := it.Field(fieldNum)
		if !field.IsExported() {
			continue
		}

		// Looking for *Collection[T] fields that are nil
		if field.Type.Kind() != reflect.Pointer {
			continue
		}

		fv := iv.Field(fieldNum)
		if !fv.IsNil() {
			continue // Already set (possibly by PreRead)
		}

		// Check if it's a Collection by looking for "proto" field in the pointed-to type
		elemType := field.Type.Elem()
		if elemType.Kind() != reflect.Struct {
			continue
		}
		protoField, hasProto := elemType.FieldByName("proto")
		if !hasProto {
			continue
		}

		// Get the element type from the proto field (e.g., *ProfileExtension)
		protoType := protoField.Type
		if protoType.Kind() != reflect.Pointer {
			continue
		}

		// Look up registeredType for the element type
		rt := registry.registeredTypes[protoType]
		if rt == nil {
			continue
		}

		// Create proto instance and collection
		proto := rt.newInstance(i.Subject())
		c := rt.newCollection(proto)

		// Set the collection on the field
		fv.Set(reflect.ValueOf(c))
	}

	return nil
}

func (*readAction[I]) Do(ctx context.Context, i I) gomerr.Gomerr {
	return i.registeredType().store.Read(ctx, i)
}

func (*readAction[I]) Retry(ctx context.Context, i I, ge gomerr.Gomerr) gomerr.Gomerr {
	return i.RetryRead(ctx, ge)
}

func (*readAction[I]) OnDoSuccess(ctx context.Context, i I) (I, gomerr.Gomerr) {
	return i, i.PostRead(ctx)
}

func (*readAction[I]) OnDoFailure(ctx context.Context, i I, ge gomerr.Gomerr) gomerr.Gomerr {
	return i.ReadError(ctx, ge)
}

func (a *readAction[T]) ExecuteOn(ctx context.Context, resource any) (any, gomerr.Gomerr) {
	return resource.(Resource[T]).DoAction(ctx, a)
}

// UpdateAction returns an action for updating instances.
func UpdateAction[I Instance[I]](readAction Action[I]) Action[I] {
	return &updateAction[I]{readAction: readAction}
}

type updateAction[I Instance[I]] struct {
	readAction Action[I]
	current    I // The current state, read from store
}

func (*updateAction[I]) Name() string {
	return "resource.UpdateAction"
}

func (*updateAction[I]) AppliesToCategory() Category {
	return InstanceCategory
}

func (*updateAction[I]) FieldAccessPermissions() auth.AccessPermissions {
	return auth.UpdatePermission
}

func (a *updateAction[I]) Pre(ctx context.Context, update I) gomerr.Gomerr {
	if a.readAction == nil {
		a.current = update
		return update.PreUpdate(ctx, update)
	}

	rt := update.registeredType()

	// Create a new instance to hold current state
	current := rt.newInstance(update.Subject()).(I)

	// Copy ID fields from update to current
	tc := structs.EnsureContext().With(SourceValue, reflect.ValueOf(update).Elem())
	ge := structs.ApplyTools(current, tc, IdTool)
	if ge != nil {
		return ge
	}

	// Read current state
	a.current, ge = current.DoAction(ctx, a.readAction)
	if ge != nil {
		return ge
	}

	// Call PreUpdate hook
	return current.PreUpdate(ctx, update)
}

func (a *updateAction[I]) Do(ctx context.Context, update I) gomerr.Gomerr {
	return update.registeredType().store.Update(ctx, a.current, update)
}

func (a *updateAction[I]) Retry(ctx context.Context, update I, ge gomerr.Gomerr) gomerr.Gomerr {
	return a.current.RetryUpdate(ctx, update, ge)
}

func (a *updateAction[I]) OnDoSuccess(ctx context.Context, update I) (I, gomerr.Gomerr) {
	return a.current, a.current.PostUpdate(ctx, update)
}

func (a *updateAction[I]) OnDoFailure(ctx context.Context, i I, ge gomerr.Gomerr) gomerr.Gomerr {
	return i.UpdateError(ctx, ge)
}

func (a *updateAction[T]) ExecuteOn(ctx context.Context, resource any) (any, gomerr.Gomerr) {
	return resource.(Resource[T]).DoAction(ctx, a)
}

// DeleteAction returns an action for deleting instances.
func DeleteAction[I Instance[I]]() Action[I] {
	return &deleteAction[I]{}
}

type deleteAction[I Instance[I]] struct{}

func (*deleteAction[I]) Name() string {
	return "resource.DeleteAction"
}

func (*deleteAction[I]) AppliesToCategory() Category {
	return InstanceCategory
}

func (*deleteAction[I]) FieldAccessPermissions() auth.AccessPermissions {
	return auth.NoPermissions
}

func (*deleteAction[I]) Pre(ctx context.Context, i I) gomerr.Gomerr {
	return i.PreDelete(ctx)
}

func (*deleteAction[I]) Do(ctx context.Context, i I) gomerr.Gomerr {
	return i.registeredType().store.Delete(ctx, i)
}

func (*deleteAction[I]) Retry(ctx context.Context, i I, ge gomerr.Gomerr) gomerr.Gomerr {
	return i.RetryDelete(ctx, ge)
}

func (*deleteAction[I]) OnDoSuccess(ctx context.Context, i I) (I, gomerr.Gomerr) {
	return i, i.PostDelete(ctx)
}

func (*deleteAction[I]) OnDoFailure(ctx context.Context, i I, ge gomerr.Gomerr) gomerr.Gomerr {
	return i.DeleteError(ctx, ge)
}

func (a *deleteAction[T]) ExecuteOn(ctx context.Context, resource any) (any, gomerr.Gomerr) {
	return resource.(Resource[T]).DoAction(ctx, a)
}

// ListAction returns an action for listing instances via a collection.
func ListAction[I Instance[I]]() Action[*Collection[I]] {
	return &listAction[I]{}
}

type listAction[I Instance[I]] struct{}

func (*listAction[I]) Name() string {
	return "resource.ListAction"
}

func (*listAction[I]) AppliesToCategory() Category {
	return CollectionCategory
}

func (*listAction[I]) FieldAccessPermissions() auth.AccessPermissions {
	return auth.WritePermissions
}

func (*listAction[I]) Pre(ctx context.Context, c *Collection[I]) gomerr.Gomerr {
	if ge := c.proto.PreList(ctx); ge != nil {
		return ge
	}
	return c.PreList(ctx)
}

func (*listAction[I]) Do(ctx context.Context, c *Collection[I]) gomerr.Gomerr {
	if ge := c.Query(ctx); ge != nil {
		return ge
	}
	if len(c.Items) == 0 {
		// If no results and the proto implements RetryList, return a sentinel to
		// trigger a retry
		if _, ok := any(c.proto).(RetryLister[I]); ok {
			return gomerr.NotAnError
		}
	}
	return nil
}

type RetryLister[I Instance[I]] interface {
	RetryList(context.Context, *Collection[I], gomerr.Gomerr) gomerr.Gomerr
}

func (*listAction[I]) Retry(ctx context.Context, c *Collection[I], ge gomerr.Gomerr) gomerr.Gomerr {
	// If not the sentinel error, return the error directly
	if !errors.Is(ge, gomerr.NotAnError) {
		return ge
	}
	if rl, ok := any(c.proto).(RetryLister[I]); ok {
		if ge = rl.RetryList(ctx, c, ge); ge != nil {
			return ge
		}
	}
	return c.RetryList(ctx, ge)
}

type postLister[I Instance[I]] interface {
	PostList(context.Context, *Collection[I]) gomerr.Gomerr
}

func (*listAction[I]) OnDoSuccess(ctx context.Context, c *Collection[I]) (*Collection[I], gomerr.Gomerr) {
	if ge := c.PostList(ctx); ge != nil {
		return nil, ge
	}
	if pl, ok := any(c.proto).(postLister[I]); ok {
		return c, pl.PostList(ctx, c)
	}
	return c, nil
}

func (*listAction[I]) OnDoFailure(ctx context.Context, c *Collection[I], ge gomerr.Gomerr) gomerr.Gomerr {
	return c.proto.ListError(ctx, ge)
}

func (a *listAction[T]) ExecuteOn(ctx context.Context, resource any) (any, gomerr.Gomerr) {
	return resource.(Resource[*Collection[T]]).DoAction(ctx, a)
}

// Collectible is implemented by instances that want to be notified when collected.
type Collectible interface {
	OnCollect(ctx context.Context, r any) gomerr.Gomerr
}

// NoOpAction is an action that does nothing.
type NoOpAction[T any] struct{}

func (NoOpAction[T]) Name() string {
	return "resource.NoOpAction"
}

func (NoOpAction[T]) AppliesToCategory() Category {
	return InstanceCategory
}

func (NoOpAction[T]) FieldAccessPermissions() auth.AccessPermissions {
	return auth.NoPermissions
}

func (NoOpAction[T]) Pre(_ context.Context, _ T) gomerr.Gomerr {
	return nil
}

func (NoOpAction[T]) Do(_ context.Context, _ T) gomerr.Gomerr {
	return nil
}

func (NoOpAction[T]) Retry(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (NoOpAction[T]) OnDoSuccess(_ context.Context, r T) (T, gomerr.Gomerr) {
	return r, nil
}

func (NoOpAction[T]) OnDoFailure(_ context.Context, _ T, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func convertPersistableNotFoundIfApplicable[I Instance[I]](i I, ge gomerr.Gomerr) gomerr.Gomerr {
	if _, ok := errors.AsType[*dataerr.PersistableNotFoundError](ge); !ok {
		return ge
	}
	return gomerr.NotFound(i.registeredType().instanceName, i.Id()).Wrap(ge)
}
