package resource

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/util"
)

type Metadata interface {
	InstanceName() string
	CollectionQueryName() string
	Parent() Metadata
	Children() []Metadata
}

func Register(instance Instance, collectionQuery CollectionQuery, dataStore data.Store, parentMetadata Metadata) Metadata {
	if instance == nil {
		panic("A resource requires an Instance type")
	}

	it := reflect.TypeOf(instance)
	instanceName := strings.ToLower(util.UnqualifiedTypeName(it))

	if metadata, ok := resourceMetadata[instanceName]; ok {
		return metadata
	}

	var structType reflect.Type
	switch it.Kind() {
	case reflect.Struct:
		structType = it
	case reflect.Ptr:
		structType = it.Elem()
	default:
		panic(fmt.Sprintf("Type other than pointer or struct as instance: %T", instance))
	}

	cqt := reflect.TypeOf(collectionQuery)
	nilSafeParentMetadata, _ := parentMetadata.(*metadata)

	metadata := &metadata{
		instanceType:        it,
		instanceName:        instanceName,
		collectionQueryType: cqt,
		collectionQueryName: strings.ToLower(util.UnqualifiedTypeName(cqt)),
		fields:              fields(structType, ""),
		dataStore:           dataStore,
		parent:              nilSafeParentMetadata,
		children:            make([]Metadata, 0),
	}

	resourceMetadata[instanceName] = metadata
	if nilSafeParentMetadata != nil {
		nilSafeParentMetadata.children = append(nilSafeParentMetadata.children, metadata)
	}

	return metadata
}

var resourceMetadata = make(map[string]*metadata)

type metadata struct {
	instanceType        reflect.Type
	instanceName        string
	collectionQueryType reflect.Type
	collectionQueryName string
	fields              map[string]*field
	dataStore           data.Store
	parent              *metadata
	children            []Metadata // Using interface type since we aren't currently using child attributes

	//idFields       []field
}

func (m *metadata) InstanceName() string {
	return m.instanceName
}

func (m *metadata) CollectionQueryName() string {
	return m.collectionQueryName
}

func (m *metadata) Parent() Metadata {
	if m.parent == nil {
		return nil
	}

	return m.parent
}

func (m *metadata) Children() []Metadata {
	return m.children
}

func (m *metadata) emptyItems() interface{} {
	slice := reflect.MakeSlice(reflect.SliceOf(m.instanceType), 0, 0)

	// Create a pointer to a slice value and set it to the slice
	slicePtr := reflect.New(slice.Type())
	slicePtr.Elem().Set(slice)

	return slicePtr.Interface()
}
