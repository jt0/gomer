package resource

import (
	"github.com/jt0/gomer/auth"
)

type resource interface {
	Subject() auth.Subject

	metadata() *metadata
	setMetadata(*metadata)
	setSubject(auth.Subject)
}

type BaseResource struct {
	md  *metadata
	sub auth.Subject
}

func (b *BaseResource) Subject() auth.Subject {
	return b.sub
}

func (b *BaseResource) metadata() *metadata {
	return b.md
}

func (b *BaseResource) setMetadata(metadata *metadata) {
	b.md = metadata
}

func (b *BaseResource) setSubject(subject auth.Subject) {
	b.sub = subject
}

