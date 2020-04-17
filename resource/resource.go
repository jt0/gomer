package resource

import (
	"github.com/jt0/gomer/auth"
)

type resource interface {
	Subject() auth.Subject
	SetSubject(auth.Subject)

	metadata() *metadata
	setMetadata(*metadata)
}

type BaseResource struct {
	md  *metadata
	sub auth.Subject
}

func (b *BaseResource) Subject() auth.Subject {
	return b.sub
}

func (b *BaseResource) SetSubject(subject auth.Subject) {
	b.sub = subject
}

func (b *BaseResource) metadata() *metadata {
	return b.md
}

func (b *BaseResource) setMetadata(metadata *metadata) {
	b.md = metadata
}
