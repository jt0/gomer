package resource

import (
	"reflect"
	"strings"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type Metadata interface {
	ResourceType(Category) reflect.Type
	Actions() map[interface{}]func() Action
	// Parent() Metadata
	Children() []Metadata
}

func Register(instance Instance, collection Collection, actions map[interface{}]func() Action, dataStore data.Store, parentMetadata Metadata) (md *metadata, ge gomerr.Gomerr) {
	if instance == nil {
		return nil, gomerr.Configuration("Must register with an Instance type")
	}

	it := reflect.TypeOf(instance)
	md, _ = resourceTypeToMetadata[it]
	if md != nil {
		return md, nil
	}

	if actions == nil {
		return nil, gomerr.Configuration("Must register with a non-nil Actions")
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

	nilSafeParentMetadata, _ := parentMetadata.(*metadata)

	md = &metadata{
		instanceType:   it,
		instanceName:   unqualifiedInstanceName,
		collectionType: ct,
		collectionName: unqualifiedCollectionName,
		actions:        actions,
		dataStore:      dataStore,
		parent:         nilSafeParentMetadata,
		children:       make([]Metadata, 0),
	}

	if nilSafeParentMetadata != nil {
		nilSafeParentMetadata.children = append(nilSafeParentMetadata.children, md)
	}

	resourceTypeToMetadata[it] = md
	if ct != nil {
		resourceTypeToMetadata[ct] = md
	}

	return md, nil
}

var resourceTypeToMetadata = make(map[reflect.Type]*metadata)

type metadata struct {
	instanceType   reflect.Type
	instanceName   string
	collectionType reflect.Type
	collectionName string
	actions        map[interface{}]func() Action
	dataStore      data.Store
	parent         *metadata
	children       []Metadata // Using interface type since we aren't currently using child attributes

	// idFields       []field
}

func (m *metadata) ResourceType(category Category) reflect.Type {
	switch category {
	case InstanceCategory:
		return m.instanceType
	case CollectionCategory:
		return m.collectionType
	default:
		return nil
	}
}

func (m *metadata) Actions() map[interface{}]func() Action {
	return m.actions
}

// func (m *metadata) Parent() Metadata {
// 	if m.parent == nil {
// 		return nil
// 	}
//
// 	return m.parent
// }

func (m *metadata) Children() []Metadata {
	return m.children
}
