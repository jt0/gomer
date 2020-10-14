package resource

import (
	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
)

type resource interface {
	Metadata() Metadata
	Subject() auth.Subject
	OnSubject()

	setSubject(auth.Subject)
	metadata() *metadata
	setMetadata(*metadata)
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

func (b *BaseInstance) OnSubject() {
	// No-op by default
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

type UnknownResourceTypeError struct {
	gomerr.Gomerr
	Type string
}

func unknownResourceType(type_ string) gomerr.Gomerr {
	return gomerr.Build(new(UnknownResourceTypeError), type_)
}
