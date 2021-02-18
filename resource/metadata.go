package resource

import (
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/id"
	"github.com/jt0/gomer/util"
)

func init() {
	// These are the default tag keys for these tools, but an application can set different key values if they'd like or
	// add new entries to the map so long as they do it before invoking Register().
	fields.TagToFieldToolAssociations(map[string]fields.FieldTool{
		"access":   auth.FieldAccessTool,
		"default":  fields.FieldDefaultTool,
		"id":       id.IdFieldTool,
		"validate": constraint.FieldValidationTool,
	})

	// This defines a subset of default values that map to the provided Action types assoc. An application can add
	// alternative keys that map to these same action names if they prefer something different. To clear out these
	// values in lieu of alternatives (or to not have any aliases), call fields.ResetScopeAliases().
	fields.AddScopeAliases(map[string][]string{
		"create": {CreateAction().Name()},
		"read":   {ReadAction().Name()},
		"update": {UpdateAction().Name()},
		"delete": {DeleteAction().Name()},
		"query":  {QueryAction().Name()},
	})
}

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
	defer func() {
		if ge == nil {
			resourceTypeToMetadata[it] = md
		}
	}()

	if actions == nil {
		return nil, gomerr.Configuration("Must register with a non-nil Actions")
	}

	var ct reflect.Type
	if collection != nil {
		ct = reflect.TypeOf(collection)
		defer func() {
			if ge == nil {
				resourceTypeToMetadata[ct] = md
			}
		}()
	}

	nilSafeParentMetadata, _ := parentMetadata.(*metadata)

	md = &metadata{
		instanceType:   it,
		instanceName:   util.UnqualifiedTypeName(it),
		collectionType: ct,
		collectionName: util.UnqualifiedTypeName(ct),
		actions:        actions,
		dataStore:      dataStore,
		parent:         nilSafeParentMetadata,
		children:       make([]Metadata, 0),
	}

	if nilSafeParentMetadata != nil {
		nilSafeParentMetadata.children = append(nilSafeParentMetadata.children, md)
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
