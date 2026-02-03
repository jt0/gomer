package resource

import (
	"context"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
)

type Resource interface {
	Metadata() *Metadata
	Subject(context.Context) auth.Subject
	DoAction(context.Context, Action) (Resource, gomerr.Gomerr)

	setSelf(Resource)
	metadata() *Metadata
	setMetadata(*Metadata)
	setSubject(auth.Subject)
}

type Action interface {
	Name() string
	AppliesToCategory() Category
	FieldAccessPermissions() auth.AccessPermissions
	Pre(context.Context, Resource) gomerr.Gomerr
	Do(context.Context, Resource) gomerr.Gomerr
	OnDoSuccess(context.Context, Resource) (Resource, gomerr.Gomerr)
	OnDoFailure(context.Context, Resource, gomerr.Gomerr) gomerr.Gomerr
}

type Category string

const (
	InstanceCategory   Category = "Instance"
	CollectionCategory Category = "Collection"
)

type BaseResource struct {
	self Resource
	md   *Metadata
	sub  auth.Subject
}

func (b *BaseResource) Metadata() *Metadata {
	return b.md
}

func (b *BaseResource) Subject(context.Context) auth.Subject {
	return b.sub
}

func (b *BaseResource) DoAction(ctx context.Context, action Action) (Resource, gomerr.Gomerr) {
	if ge := action.Pre(ctx, b.self); ge != nil {
		return nil, ge
	}

	if ge := action.Do(ctx, b.self); ge != nil {
		return nil, action.OnDoFailure(ctx, b.self, ge)
	}

	return action.OnDoSuccess(ctx, b.self)
}

func (b *BaseResource) metadata() *Metadata {
	return b.md
}

func (b *BaseResource) setSelf(self Resource) {
	b.self = self
}

func (b *BaseResource) setMetadata(metadata *Metadata) {
	b.md = metadata
}

func (b *BaseResource) setSubject(subject auth.Subject) {
	b.sub = subject
}
