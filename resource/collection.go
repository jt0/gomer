package resource

import (
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

type Collection interface {
	Resource
	data.Queryable
}

func readableCollectionData(c Collection) gomerr.Gomerr {
	ta := fields.ToolWithContext{auth.FieldAccessTool, auth.AddClearIfDeniedAction(c.Subject(), auth.ReadPermission)}

	for _, item := range c.Items() {
		if r, ok := item.(Resource); ok {
			ge := c.metadata().fields.ApplyTools(reflect.ValueOf(r).Elem(), ta)
			if ge != nil {
				return ge
			}
		}
	}

	ge := c.metadata().fields.ApplyTools(reflect.ValueOf(c).Elem(), ta)
	if ge != nil {
		return ge
	}

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
