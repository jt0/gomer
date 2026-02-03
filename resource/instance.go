package resource

import (
	"context"
	"reflect"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type Instance interface {
	Resource
	data.Persistable
	Id() string
}

func SaveInstance(ctx context.Context, i Instance) gomerr.Gomerr {
	// TODO: Consider alt form w/ Updatable.Update() that separates resource from data
	// if ge := u.Update(u); ge != nil {
	// 	return ge
	// }

	if ge := i.metadata().dataStore.Update(ctx, i, nil); ge != nil {
		return ge
	}

	return nil
}

type BaseInstance struct {
	BaseResource

	// persistedValues map[string]interface{}
}

func (i BaseInstance) TypeName() string {
	return i.md.instanceName
}

func (i BaseInstance) NewQueryable() data.Queryable {
	ct := i.metadata().collectionType
	if ct == nil {
		return nil
	}

	collection := reflect.New(ct.Elem()).Interface().(Collection)
	collection.setSelf(collection)
	collection.setMetadata(i.md)
	collection.setSubject(i.Subject(context.TODO()))

	return collection
}

func (i BaseInstance) Id() string {
	instanceId, ge := Id(reflect.ValueOf(i.self).Elem())
	if ge != nil {
		println("Unable to get id value for instance:\n", ge.Error())
	}

	return instanceId
}

func (BaseInstance) PreCreate(context.Context) gomerr.Gomerr {
	return nil
}

func (BaseInstance) PostCreate(context.Context) gomerr.Gomerr {
	return nil
}

func (BaseInstance) PreRead(context.Context) gomerr.Gomerr {
	return nil
}

func (BaseInstance) PostRead(context.Context) gomerr.Gomerr {
	return nil
}

func (BaseInstance) PreUpdate(context.Context, Resource) gomerr.Gomerr {
	return nil
}

func (BaseInstance) PostUpdate(context.Context, Resource) gomerr.Gomerr {
	return nil
}

func (BaseInstance) PreDelete(context.Context) gomerr.Gomerr {
	return nil
}

func (BaseInstance) PostDelete(context.Context) gomerr.Gomerr {
	return nil
}
