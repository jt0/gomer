package resource

import (
	"reflect"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

type Collection interface {
	Resource
	data.Queryable
}

type BaseCollection struct {
	BaseResource
}

func (c *BaseCollection) TypeNames() []string {
	return []string{c.md.instanceName}
}

func (c *BaseCollection) TypeOf(interface{}) string {
	return c.md.instanceName
}

func (*BaseCollection) MaximumPageSize() int {
	return 0
}

func (*BaseCollection) PreQuery() gomerr.Gomerr {
	return nil
}

func (*BaseCollection) PostQuery() gomerr.Gomerr {
	return nil
}

func (c *BaseCollection) ApplyTools(tools ...fields.ToolWithContext) gomerr.Gomerr {
	fs, ge := fields.Get(c.md.collectionType.Elem())
	if ge != nil {
		return ge
	}

	return fs.ApplyTools(reflect.ValueOf(c.self).Elem(), tools...)
}
