package resource

import (
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type Domain struct {
	metadata map[reflect.Type]*Metadata
	roots    []*Metadata
}

var defaultDomain atomic.Pointer[Domain]

type domainCtxKey struct{}

var DomainCtxKey = domainCtxKey{}

func NewDomain() *Domain {
	domain := &Domain{metadata: make(map[reflect.Type]*Metadata)}
	defaultDomain.CompareAndSwap(nil, domain) // Set the first domain as the default in case its needed
	return domain
}

func (d *Domain) Register(instance Instance, collection Collection, actions map[any]func() Action, dataStore data.Store) (*Metadata, gomerr.Gomerr) {
	md, ge := d.register(instance, collection, actions, dataStore)
	if ge != nil {
		return nil, ge
	}
	d.roots = append(d.roots, md)
	return md, ge
}

func (d *Domain) RootResources() []*Metadata {
	return d.roots
}

// NewResource return a new instance of the (registered) resourceType. If
func (d *Domain) NewResource(resourceType reflect.Type, subject auth.Subject) (Resource, gomerr.Gomerr) {
	if d == nil {
		if d = defaultDomain.Load(); d == nil {
			return nil, gomerr.Configuration("no resource domain to instantiate from")
		}
	}

	md, ok := d.metadata[resourceType]
	if !ok {
		return nil, gomerr.Unprocessable("Unknown Resource type. Was resource.Register() called for it?", resourceType)
	}

	resource := reflect.New(resourceType.Elem()).Interface().(Resource)
	resource.setSelf(resource)
	resource.setMetadata(md)
	resource.setSubject(subject)

	return resource, nil
}

func (d *Domain) register(instance Instance, collection Collection, actions map[any]func() Action, dataStore data.Store) (*Metadata, gomerr.Gomerr) {
	if instance == nil {
		return nil, gomerr.Configuration("non-nil instance required")
	}

	it := reflect.TypeOf(instance)
	md, _ := d.metadata[it]
	if md != nil {
		return md, nil
	}

	if actions == nil {
		return nil, gomerr.Configuration("non-nil actions required")
	}

	unqualifiedInstanceName := it.String()
	unqualifiedInstanceName = unqualifiedInstanceName[strings.Index(unqualifiedInstanceName, ".")+1:]

	var ct reflect.Type
	var unqualifiedCollectionName string
	if collection != nil {
		ct = reflect.TypeOf(collection)
		unqualifiedCollectionName = it.String()
		unqualifiedCollectionName = unqualifiedCollectionName[strings.Index(unqualifiedCollectionName, ".")+1:]
	}

	md = &Metadata{
		domain:         d,
		instanceType:   it,
		instanceName:   unqualifiedInstanceName,
		collectionType: ct,
		collectionName: unqualifiedCollectionName,
		actions:        actions,
		dataStore:      dataStore,
		children:       make([]*Metadata, 0),
	}

	d.metadata[it] = md
	if ct != nil {
		d.metadata[ct] = md
	}

	return md, nil
}

// type Metadata interface {
// 	RegisterChild(Instance, Collection, map[any]func() Action, data.Store) (*Metadata, gomerr.Gomerr)
// 	ResourceType(Category) reflect.Type
// 	Actions() map[any]func() Action
// 	Parent() *Metadata
// 	Children() []*Metadata
// }

type Metadata struct {
	domain         *Domain
	instanceType   reflect.Type
	instanceName   string
	collectionType reflect.Type
	collectionName string
	actions        map[any]func() Action
	dataStore      data.Store
	parent         *Metadata
	children       []*Metadata // Using interface type since we aren't currently using child attributes

	// idFields       []field
}

func (m *Metadata) RegisterChild(instance Instance, collection Collection, actions map[any]func() Action, dataStore data.Store) (*Metadata, gomerr.Gomerr) {
	if m == nil {
		return nil, gomerr.Configuration("cannot create child of nil parent")
	}

	md, ge := m.domain.register(instance, collection, actions, dataStore)
	if ge != nil {
		return nil, ge
	}

	md.parent = m
	m.children = append(m.children, md)

	return md, ge
}

func (m *Metadata) ResourceType(category Category) reflect.Type {
	switch category {
	case InstanceCategory:
		return m.instanceType
	case CollectionCategory:
		return m.collectionType
	default:
		return nil
	}
}

func (m *Metadata) Actions() map[any]func() Action {
	return m.actions
}

func (m *Metadata) Parent() *Metadata {
	return m.parent
}

func (m *Metadata) Children() []*Metadata {
	return m.children
}
