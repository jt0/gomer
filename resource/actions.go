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

func (*createAction[I]) Pre(ctx context.Context, instance I) gomerr.Gomerr {
	return instance.PreCreate(ctx)
}

func (*createAction[I]) Do(ctx context.Context, instance I) gomerr.Gomerr {
	return instance.registeredType().store.Create(ctx, instance)
}

func (*createAction[I]) OnDoSuccess(ctx context.Context, instance I) (I, gomerr.Gomerr) {
	return instance, instance.PostCreate(ctx)
}

func (*createAction[I]) OnDoFailure(_ context.Context, _ I, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
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

func (*readAction[I]) Pre(ctx context.Context, instance I) gomerr.Gomerr {
	if ge := instance.PreRead(ctx); ge != nil {
		return ge
	}

	// Prepare any Collection fields for nested queries
	registry, _ := ctx.Value(RegistryCtxKey).(*Registry)
	if registry == nil {
		return nil // No registry in context, skip auto-population
	}

	iv := reflect.ValueOf(instance)
	if iv.Kind() == reflect.Pointer {
		iv = iv.Elem()
	}
	if iv.Kind() != reflect.Struct {
		return nil
	}

	it := iv.Type()
	for i := range it.NumField() {
		field := it.Field(i)
		if !field.IsExported() {
			continue
		}

		// Looking for *Collection[T] fields that are nil
		if field.Type.Kind() != reflect.Pointer {
			continue
		}

		fv := iv.Field(i)
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
		proto := rt.newInstance(instance.Subject())
		collection := rt.newCollection(proto)

		// Set the collection on the field
		fv.Set(reflect.ValueOf(collection))
	}

	return nil
}

func (*readAction[I]) Do(ctx context.Context, instance I) gomerr.Gomerr {
	return instance.registeredType().store.Read(ctx, instance)
}

func (*readAction[I]) OnDoSuccess(ctx context.Context, instance I) (I, gomerr.Gomerr) {
	return instance, instance.PostRead(ctx)
}

func (*readAction[I]) OnDoFailure(_ context.Context, _ I, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (a *readAction[T]) ExecuteOn(ctx context.Context, resource any) (any, gomerr.Gomerr) {
	return resource.(Resource[T]).DoAction(ctx, a)
}

// UpdateAction returns an action for updating instances.
func UpdateAction[I Instance[I]]() Action[I] {
	return &updateAction[I]{}
}

type updateAction[I Instance[I]] struct {
	current I // The current state, read from store
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
	rt := update.registeredType()

	// Create a new instance to hold current state
	current := rt.newInstance(update.Subject()).(I)

	// Copy ID fields from update to current
	tc := structs.EnsureContext().With(SourceValue, reflect.ValueOf(update).Elem())
	if ge := structs.ApplyTools(current, tc, IdTool); ge != nil {
		return ge
	}

	// Read current state from store
	if ge := rt.store.Read(ctx, current); ge != nil {
		return ge
	}

	a.current = current

	// Call PreUpdate hook
	return current.PreUpdate(ctx, update)
}

func (a *updateAction[I]) Do(ctx context.Context, update I) gomerr.Gomerr {
	return update.registeredType().store.Update(ctx, a.current, update)
}

func (a *updateAction[I]) OnDoSuccess(ctx context.Context, update I) (I, gomerr.Gomerr) {
	return a.current, a.current.PostUpdate(ctx, update)
}

func (a *updateAction[I]) OnDoFailure(_ context.Context, _ I, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
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

func (*deleteAction[I]) Pre(ctx context.Context, instance I) gomerr.Gomerr {
	return instance.PreDelete(ctx)
}

func (*deleteAction[I]) Do(ctx context.Context, instance I) gomerr.Gomerr {
	return instance.registeredType().store.Delete(ctx, instance)
}

func (*deleteAction[I]) OnDoSuccess(ctx context.Context, instance I) (I, gomerr.Gomerr) {
	return instance, instance.PostDelete(ctx)
}

func (*deleteAction[I]) OnDoFailure(_ context.Context, _ I, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
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

func (*listAction[I]) Pre(ctx context.Context, collection *Collection[I]) gomerr.Gomerr {
	return collection.PreList(ctx)
}

func (*listAction[I]) Do(ctx context.Context, collection *Collection[I]) gomerr.Gomerr {
	return collection.Query(ctx)
}

func (*listAction[I]) OnDoSuccess(ctx context.Context, collection *Collection[I]) (*Collection[I], gomerr.Gomerr) {
	return collection, collection.PostList(ctx)
}

func (*listAction[I]) OnDoFailure(_ context.Context, _ *Collection[I], ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (a *listAction[T]) ExecuteOn(ctx context.Context, resource any) (any, gomerr.Gomerr) {
	return resource.(Resource[*Collection[T]]).DoAction(ctx, a)
}

// OnCreateFailer is implemented by instances that want custom failure handling for create.
type OnCreateFailer[I Instance[I]] interface {
	OnCreateFailure(context.Context, gomerr.Gomerr) gomerr.Gomerr
}

// OnReadFailer is implemented by instances that want custom failure handling for read.
type OnReadFailer[I Instance[I]] interface {
	OnReadFailure(context.Context, gomerr.Gomerr) gomerr.Gomerr
}

// OnUpdateFailer is implemented by instances that want custom failure handling for update.
type OnUpdateFailer[I Instance[I]] interface {
	OnUpdateFailure(context.Context, gomerr.Gomerr) gomerr.Gomerr
}

// OnDeleteFailer is implemented by instances that want custom failure handling for delete.
type OnDeleteFailer[I Instance[I]] interface {
	OnDeleteFailure(context.Context, gomerr.Gomerr) gomerr.Gomerr
}

// OnListFailer is implemented by collections that want custom failure handling for list.
type OnListFailer[I Instance[I]] interface {
	OnListFailure(context.Context, gomerr.Gomerr) gomerr.Gomerr
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

func (NoOpAction[T]) OnDoSuccess(_ context.Context, r T) (T, gomerr.Gomerr) {
	return r, nil
}

func (NoOpAction[T]) OnDoFailure(_ context.Context, _ T, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

var persistableNotFound = &dataerr.PersistableNotFoundError{}

func convertPersistableNotFoundIfApplicable[I Instance[I]](i I, ge gomerr.Gomerr) gomerr.Gomerr {
	if !errors.Is(ge, persistableNotFound) {
		return ge
	}

	return gomerr.NotFound(i.registeredType().instanceName, i.Id()).Wrap(ge)
}
