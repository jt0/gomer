package resource

import (
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type Collection interface {
	Resource
	data.Listable
}

type BaseCollection struct {
	BaseResource
}

func (c BaseCollection) TypeNames() []string {
	return []string{c.md.instanceName}
}

func (c BaseCollection) TypeOf(interface{}) string {
	return c.md.instanceName
}

func (BaseCollection) MaximumPageSize() int {
	return 0
}

func (BaseCollection) PreList() gomerr.Gomerr {
	return nil
}

func (BaseCollection) PostList() gomerr.Gomerr {
	return nil
}
