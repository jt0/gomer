package resource

import (
	"github.com/jt0/gomer/auth"
)

type resource interface {
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
