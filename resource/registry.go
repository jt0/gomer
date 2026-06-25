package resource

import (
	"context"
	"reflect"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type registryCtxKey struct{}

// RegistryCtxKey is the context key for storing/retrieving a Registry.
var RegistryCtxKey = registryCtxKey{}

// Registry holds all registered resource types.
type Registry struct {
	registeredTypes map[reflect.Type]*registeredType
	rootTypes       []RegisteredType
}

// NewRegistry creates a new registry for resource registration.
func NewRegistry() (*Registry, context.Context) {
	r := &Registry{registeredTypes: make(map[reflect.Type]*registeredType)}
	ctx := context.WithValue(context.Background(), RegistryCtxKey, r)
	return r, ctx
}

// RootTypes returns all root-level registered resources.
func (r *Registry) RootTypes() []RegisteredType {
	return r.rootTypes
}

type RegisteredType interface {
	InstanceName() string
	CollectionName() string
	Actions() map[any]func() AnyAction
	Parent() RegisteredType
	Children() []RegisteredType
	NewInstance(auth.Subject) any
	NewCollection(proto any) any
	Store() data.Store
}

// Register registers an instance type with the registry.
func Register[I Instance[I]](ctx context.Context, opts ...Option) {
	a := ctx.Value(RegistryCtxKey)
	r, ok := a.(*Registry)
	if !ok {
		panic("context does not contain a Registry")
	}

	rt := &registeredType{
		instanceType: reflect.TypeFor[I](),
		baseOffset:   findBaseResourceOffset[I](),
	}

	for _, opt := range opts {
		opt(rt)
	}

	if rt.instanceName == "" {
		s := rt.instanceType.Elem().Name()
		dotIndex := strings.Index(s, ".")
		rt.instanceName = s[dotIndex+1:]
	}

	if rt.collectionName == "" {
		rt.collectionName = rt.instanceName + "s" // default to most common pluralization form
	}

	// Create closures while we know the type of I.
	rt.newInstance = func(sub auth.Subject) any {
		i := reflect.New(reflect.TypeFor[I]().Elem()).Interface().(I)
		i.initialize(rt, sub)
		return i
	}

	rt.newCollection = func(proto any) any {
		i, ok := proto.(I)
		if !ok || i.registeredType() != rt {
			panic(gomerr.Configuration("collection must be created with its own instance type").String())
		}
		return NewCollection(i)
	}

	// Resolve parent if specified
	if rt.parentType != nil {
		parent := r.registeredTypes[rt.parentType]
		if parent == nil {
			panic(gomerr.Configuration("unregistered parent type: " + rt.parentType.String()).String())
		}
		parent.children = append(parent.children, rt)
		rt.parent = parent
	} else {
		r.rootTypes = append(r.rootTypes, rt)
	}

	r.registeredTypes[rt.instanceType] = rt

	return
}

type Option func(*registeredType)

// WithParent establishes a parent-child relationship with the specified resource type. Parents must be
// registered before their children.
func WithParent[P Instance[P]]() Option {
	return func(rt *registeredType) {
		rt.parentType = reflect.TypeFor[P]()
	}
}

// WithActions specifies the actions available for this resource.
func WithActions(actions map[any]func() AnyAction) Option {
	return func(rt *registeredType) {
		rt.actions = actions
	}
}

// WithStore specifies the data store for this resource. If not provided, inherits from parent (if any).
func WithStore(store data.Store) Option {
	return func(rt *registeredType) {
		rt.store = store
	}
}

// WithInstanceName overrides the singular path name derived from the type name.
func WithInstanceName(name string) Option {
	return func(rt *registeredType) {
		rt.instanceName = name
	}
}

// WithCollectionName overrides the plural/collection path name.
func WithCollectionName(name string) Option {
	return func(rt *registeredType) {
		rt.collectionName = name
	}
}

// findBaseResourceOffset finds the offset of BaseResource[I] within the instance struct.
// It searches recursively through embedded structs.
func findBaseResourceOffset[I Instance[I]]() uintptr {
	instanceType := reflect.TypeFor[I]().Elem() // *Person -> Person
	baseResourceType := reflect.TypeOf(BaseResource[I]{})
	return findFieldOffset(instanceType, baseResourceType)
}

// findFieldOffset recursively searches for a field of the target type and returns its offset.
func findFieldOffset(structType, targetType reflect.Type) uintptr {
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if field.Type == targetType {
			return field.Offset
		}
		// Recurse into embedded structs
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			if innerOffset := findFieldOffset(field.Type, targetType); innerOffset != ^uintptr(0) {
				return field.Offset + innerOffset
			}
		}
	}
	return ^uintptr(0) // not found
}

// registeredType holds registration information for a resource type. It is not generic because it needs to be stored
// in collections indexed by reflect.Type.
type registeredType struct {
	instanceType   reflect.Type
	baseOffset     uintptr // offset of BaseResource within the instance struct
	instanceName   string
	collectionName string
	actions        map[any]func() AnyAction // Returns Action[I] for the registered type
	parentType     reflect.Type
	parent         *registeredType
	children       []RegisteredType
	store          data.Store

	newInstance   func(sub auth.Subject) any
	newCollection func(proto any) any
}

func (m *registeredType) InstanceName() string {
	return m.instanceName
}

func (m *registeredType) CollectionName() string {
	return m.collectionName
}

func (m *registeredType) Actions() map[any]func() AnyAction {
	return m.actions
}

func (m *registeredType) Parent() RegisteredType {
	if m.parent == nil {
		return nil // to avoid type-mismatch
	}
	return m.parent
}

func (m *registeredType) Children() []RegisteredType {
	return m.children
}

func (m *registeredType) NewInstance(subject auth.Subject) any {
	return m.newInstance(subject)
}

func (m *registeredType) NewCollection(proto any) any {
	return m.newCollection(proto)
}

func (m *registeredType) Store() data.Store {
	return m.store
}
