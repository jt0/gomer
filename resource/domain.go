package resource

import (
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

// Register registers an instance type with the domain.
// If parent is nil, the resource is registered as a root resource.
// If parent is provided, the resource is registered as a child of that parent.
func Register[I Instance[I]](d *Domain, parent *Metadata, actions map[any]func() AnyAction, store data.Store) (*Metadata, gomerr.Gomerr) {
	instanceType := reflect.TypeFor[I]()

	// Compute offset of BaseResource within the instance struct
	baseOffset := findBaseResourceOffset[I]()

	md := &Metadata{
		instanceType: instanceType,
		instanceName: instanceType.Elem().Name(),
		anyActions:   actions,
		dataStore:    store,
		parent:       parent,
		children:     make([]*Metadata, 0),
		baseOffset:   baseOffset,
	}

	// Create closures while we know the type of I.
	md.NewInstance = func(sub auth.Subject) any {
		i := reflect.New(reflect.TypeFor[I]().Elem()).Interface().(I)
		i.initialize(md, sub)
		return i
	}
	md.NewCollection = func(proto any) any {
		i, ok := proto.(I)
		if !ok || i.Metadata() != md {
			panic(gomerr.Configuration("collection must be created with its own instance type").String())
		}
		return NewCollection(i)
	}

	d.metadata[instanceType] = md

	if parent == nil {
		d.roots = append(d.roots, md)
	} else {
		parent.children = append(parent.children, md)
	}

	return md, nil
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

type domainCtxKey struct{}

// DomainCtxKey is the context key for storing/retrieving a Domain.
var DomainCtxKey = domainCtxKey{}

// Domain holds all registered resource types.
type Domain struct {
	metadata map[reflect.Type]*Metadata
	roots    []*Metadata
}

// NewDomain creates a new domain for resource registration.
func NewDomain() *Domain {
	return &Domain{
		metadata: make(map[reflect.Type]*Metadata),
	}
}

// RootResources returns all root-level registered resources.
func (d *Domain) RootResources() []*Metadata {
	return d.roots
}

// Metadata holds registration information for a resource type. It is not generic because it needs to be stored
// in collections indexed by reflect.Type.
type Metadata struct {
	instanceType reflect.Type
	instanceName string
	anyActions   map[any]func() AnyAction // Returns Action[I] for the registered type
	dataStore    data.Store
	parent       *Metadata
	children     []*Metadata
	baseOffset   uintptr // offset of BaseResource within the instance struct

	NewInstance   func(sub auth.Subject) any
	NewCollection func(proto any) any
}

func (m *Metadata) InstanceType() reflect.Type {
	return m.instanceType
}

func (m *Metadata) InstanceName() string {
	return m.instanceName
}

func (m *Metadata) ActionFuncs() map[any]func() AnyAction {
	return m.anyActions
}

func (m *Metadata) Children() []*Metadata {
	return m.children
}

func (m *Metadata) DataStore() data.Store {
	return m.dataStore
}
