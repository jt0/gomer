package resource

import (
	"reflect"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/id"
)

type Instance interface {
	Resource
	data.Persistable
	Id() string
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

type BaseInstance struct {
	BaseResource

	// persistedValues map[string]interface{}
}

func (i *BaseInstance) TypeName() string {
	return i.md.instanceName
}

func (i *BaseInstance) NewQueryable() data.Queryable {
	ct := i.metadata().collectionType
	if ct == nil {
		return nil
	}

	collection := reflect.New(ct.Elem()).Interface().(Collection)
	collection.setSelf(collection)
	collection.setMetadata(i.md)
	collection.setSubject(i.Subject())

	return collection
}

func (i *BaseInstance) Id() string {
	instanceId, ge := id.Id(reflect.ValueOf(i.self).Elem())
	if ge != nil {
		println("Unable to get id value for instance:\n", ge.Error())
	}

	return instanceId
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
