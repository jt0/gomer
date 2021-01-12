package resource

import (
	"encoding/json"
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
)

type Resource interface {
	Metadata() Metadata
	Subject() auth.Subject

	setSubject(auth.Subject)
	metadata() *metadata
	setMetadata(*metadata)
}

type Type string

const (
	InstanceType   Type = "Instance"
	CollectionType Type = "Collection"
)

func New(resourceType reflect.Type, subject auth.Subject) (Resource, gomerr.Gomerr) {
	metadata, ok := resourceTypeToMetadata[resourceType]
	if !ok {
		return nil, gomerr.Unprocessable("Unknown Resource type. Was resource.Register() called for it?", resourceType)
	}

	resource := reflect.New(resourceType.Elem()).Interface().(Resource)
	resource.setMetadata(metadata)
	resource.setSubject(subject)

	return resource, nil
}

func Unmarshal(resourceType reflect.Type, subject auth.Subject, bytes []byte) (Resource, gomerr.Gomerr) {
	resource, ge := New(resourceType, subject)
	if ge != nil {
		return nil, ge
	}

	if len(bytes) != 0 {
		if err := json.Unmarshal(bytes, &resource); err != nil {
			return nil, gomerr.Unmarshal("Resource", bytes, resourceType).Wrap(err)
		}
	}

	return resource, nil
}

func ReadableData(r Resource) gomerr.Gomerr {
	if c, ok := r.(Collection); ok {
		return readableCollectionData(c)
	} else if i, ok := r.(Instance); ok {
		return readableInstanceData(i)
	} else {
		return gomerr.Unprocessable("Resource is neither a Collection nor Instance", reflect.TypeOf(r))
	}
}

type BaseResource struct {
	md  *metadata
	sub auth.Subject
}

func (b *BaseResource) Metadata() Metadata {
	return b.md
}

func (b *BaseResource) Subject() auth.Subject {
	return b.sub
}

func (b *BaseResource) setSubject(subject auth.Subject) {
	b.sub = subject
}

func (b *BaseResource) metadata() *metadata {
	return b.md
}

func (b *BaseResource) setMetadata(metadata *metadata) {
	b.md = metadata
}
