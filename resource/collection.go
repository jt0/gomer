package resource

import (
	"context"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type Collection interface {
	Resource
	data.Queryable
}

type BaseCollection struct {
	BaseResource
}

func (c BaseCollection) TypeName() string {
	return c.md.instanceName
}

func (BaseCollection) MaximumPageSize() int {
	return 0
}

func (BaseCollection) PreList(context.Context) gomerr.Gomerr {
	return nil
}

func (BaseCollection) PostList(context.Context) gomerr.Gomerr {
	return nil
}
