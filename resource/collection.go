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

func readableCollectionData(c Collection) gomerr.Gomerr {
	for _, item := range c.Items() {
		if r, ok := item.(Resource); ok {
			r.metadata().fields.RemoveNonReadable(reflect.ValueOf(r).Elem(), r.Subject().Principal(fields.FieldAccess).(fields.AccessPrincipal))
		}
	}

	c.metadata().fields.RemoveNonReadable(reflect.ValueOf(c).Elem(), c.Subject().Principal(fields.FieldAccess).(fields.AccessPrincipal))

	return nil
}

type BaseCollection struct {
	BaseResource
}

func (b *BaseCollection) TypeNames() []string {
	return []string{b.metadata().instanceName}
}

func (b *BaseCollection) TypeOf(interface{}) string {
	return b.metadata().instanceName
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
