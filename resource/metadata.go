package resource

import (
	"reflect"
	"strings"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/util"
)

type Metadata interface {
	InstanceName() string
	InstanceActions() InstanceActions
	CollectionQueryName() string
	//Parent() Metadata
	Children() []Metadata
	//IdExternalName() string
	ExternalNameToFieldName(externalName string) (string, bool)
}

type InstanceActions map[string]func() InstanceAction

var emptyActions = InstanceActions{}

func Register(
	instance Instance,
	instanceActions InstanceActions,
	collectionQuery CollectionQuery,
	// collectionActions map[string]func() CollectionAction,
	dataStore data.Store,
	parentMetadata Metadata,
) (md *metadata, ge gomerr.Gomerr) {
	if instance == nil {
		return nil, gomerr.Configuration("Must register with an Instance type")
	}

	it := reflect.TypeOf(instance)
	instanceName := util.UnqualifiedTypeName(it)
	lcInstanceName := strings.ToLower(instanceName)

	md = lowerCaseResourceTypeToMetadata[lcInstanceName]
	if md != nil {
		return
	}

	var structType reflect.Type
	switch it.Kind() {
	case reflect.Struct:
		structType = it
	case reflect.Ptr:
		structType = it.Elem()
	}

	cqt := reflect.TypeOf(collectionQuery)
	nilSafeParentMetadata, _ := parentMetadata.(*metadata)

	if instanceActions == nil {
		instanceActions = emptyActions
	}

	md = &metadata{
		instanceType:        it,
		instanceName:        instanceName,
		instanceActions:     instanceActions,
		collectionQueryType: cqt,
		collectionQueryName: util.UnqualifiedTypeName(cqt),
		dataStore:           dataStore,
		parent:              nilSafeParentMetadata,
		children:            make([]Metadata, 0),
	}

	md.fields, ge = newFields(structType)
	if ge != nil {
		return nil, ge // don't want to return metadata value
	}

	lowerCaseResourceTypeToMetadata[lcInstanceName] = md
	if nilSafeParentMetadata != nil {
		nilSafeParentMetadata.children = append(nilSafeParentMetadata.children, md)
	}

	return
}

var lowerCaseResourceTypeToMetadata = make(map[string]*metadata)

type metadata struct {
	instanceType        reflect.Type
	instanceName        string
	instanceActions     InstanceActions
	collectionQueryType reflect.Type
	collectionQueryName string
	fields              *fields
	dataStore           data.Store
	parent              *metadata
	children            []Metadata // Using interface type since we aren't currently using child attributes

	//idFields       []field
}

func (m *metadata) InstanceName() string {
	return m.instanceName
}

func (m *metadata) InstanceActions() InstanceActions {
	return m.instanceActions
}

func (m *metadata) CollectionQueryName() string {
	return m.collectionQueryName
}

//func (m *metadata) Parent() Metadata {
//	if m.parent == nil {
//		return nil
//	}
//
//	return m.parent
//}

func (m *metadata) Children() []Metadata {
	return m.children
}

//func (m *metadata) IdExternalName() string {
//	return m.fields.idExternalName()
//}

func (m *metadata) ExternalNameToFieldName(externalName string) (string, bool) {
	return m.fields.externalNameToFieldName(externalName)
}

func (m *metadata) emptyItems() interface{} {
	slice := reflect.MakeSlice(reflect.SliceOf(m.instanceType), 0, 0)

	// Create a pointer to a slice value and set it to the slice
	slicePtr := reflect.New(slice.Type())
	slicePtr.Elem().Set(slice)

	return slicePtr.Interface()
}
