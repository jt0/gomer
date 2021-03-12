package resource

import (
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

type Resource interface {
	Metadata() Metadata
	Subject() auth.Subject
	DoAction(Action) (Resource, gomerr.Gomerr)
	ApplyTools(tools ...fields.Application) gomerr.Gomerr

	setSelf(Resource)
	metadata() *metadata
	setMetadata(*metadata)
	setSubject(auth.Subject)
}

type Action interface {
	Name() string
	AppliesToCategory() Category
	FieldAccessPermissions() auth.AccessPermissions
	Pre(Resource) gomerr.Gomerr
	Do(Resource) gomerr.Gomerr
	OnDoSuccess(Resource) (Resource, gomerr.Gomerr)
	OnDoFailure(Resource, gomerr.Gomerr) gomerr.Gomerr
}

type Category string

const (
	InstanceCategory   Category = "Instance"
	CollectionCategory Category = "Collection"
)

func New(resourceType reflect.Type, subject auth.Subject) (Resource, gomerr.Gomerr) {
	md, ok := resourceTypeToMetadata[resourceType]
	if !ok {
		return nil, gomerr.Unprocessable("Unknown Resource type. Was resource.Register() called for it?", resourceType)
	}

	resource := reflect.New(resourceType.Elem()).Interface().(Resource)
	resource.setSelf(resource)
	resource.setMetadata(md)
	resource.setSubject(subject)

	return resource, nil
}

type BaseResource struct {
	self Resource
	md   *metadata
	sub  auth.Subject
}

func (b *BaseResource) Metadata() Metadata {
	return b.md
}

func (b *BaseResource) Subject() auth.Subject {
	return b.sub
}

func (b *BaseResource) DoAction(action Action) (Resource, gomerr.Gomerr) {
	if ge := action.Pre(b.self); ge != nil {
		return nil, ge
	}

	if ge := action.Do(b.self); ge != nil {
		return nil, action.OnDoFailure(b.self, ge)
	}

	return action.OnDoSuccess(b.self)
}

func (b *BaseResource) ApplyTools(tools ...fields.Application) gomerr.Gomerr {
	selfValue := reflect.ValueOf(b.self).Elem()
	fs, ge := fields.Get(selfValue.Type())
	if ge != nil {
		return ge
	}

	return fs.ApplyTools(selfValue, tools...)
}

func (b *BaseResource) metadata() *metadata {
	return b.md
}

func (b *BaseResource) setSelf(self Resource) {
	b.self = self
}

func (b *BaseResource) setMetadata(metadata *metadata) {
	b.md = metadata
}

func (b *BaseResource) setSubject(subject auth.Subject) {
	b.sub = subject
}
