package resource

import (
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/data/dataerr"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/id"
)

type Instance interface {
	Resource
	data.Persistable
}

func SaveInstance(i Instance) gomerr.Gomerr {
	// TODO: Consider alt form w/ Updatable.Update() that separates resource from data
	// if ge := u.Update(u); ge != nil {
	// 	return ge
	// }

	if ge := i.metadata().dataStore.Update(i, nil); ge != nil {
		return ge
	}

	return nil
}

func Id(i Instance) (string, gomerr.Gomerr) {
	return id.Id(reflect.ValueOf(i).Elem())
}

func readableInstanceData(i Instance) gomerr.Gomerr {
	id, _ := Id(i)

	tool := fields.ToolWithContext{auth.FieldAccessTool, auth.AddClearIfDeniedAction(i.Subject(), auth.CreatePermission)}
	ge := i.metadata().fields.ApplyTools(reflect.ValueOf(i).Elem(), tool)
	if ge != nil {
		return ge
	}

	if tool.Context[auth.NotClearedCount] == 0 {
		return dataerr.PersistableNotFound(i.TypeName(), id).Wrap(ge)
	}

	return nil
}

type BaseInstance struct {
	BaseResource

	// persistedValues map[string]interface{}
}

func (b *BaseInstance) TypeName() string {
	return b.metadata().instanceName
}

func (b *BaseInstance) NewQueryable() data.Queryable {
	ct := b.metadata().collectionType
	if ct == nil {
		return nil
	}

	collection := reflect.New(ct.Elem()).Interface().(Collection)
	collection.setMetadata(b.metadata())
	collection.setSubject(b.Subject())

	return collection
}

func (*BaseInstance) PreCreate() gomerr.Gomerr {
	return nil
}

func (*BaseInstance) PostCreate() gomerr.Gomerr {
	return nil
}

func (*BaseInstance) PreRead() gomerr.Gomerr {
	return nil
}

func (*BaseInstance) PostRead() gomerr.Gomerr {
	return nil
}

func (*BaseInstance) PreUpdate(Resource) gomerr.Gomerr {
	return nil
}

func (*BaseInstance) PostUpdate(Resource) gomerr.Gomerr {
	return nil
}

func (*BaseInstance) PreDelete() gomerr.Gomerr {
	return nil
}

func (*BaseInstance) PostDelete() gomerr.Gomerr {
	return nil
}

func (*BaseInstance) PostQuery() gomerr.Gomerr {
	return nil
}
