package resource

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

type Instance interface {
	resource
	data.Persistable
}

func newInstance(resourceType string, subject auth.Subject) (Instance, gomerr.Gomerr) {
	metadata, ok := lowerCaseResourceTypeToMetadata[strings.ToLower(resourceType)]
	if !ok {
		return nil, unknownResourceType(resourceType)
	}

	instance := reflect.New(metadata.instanceType.Elem()).Interface().(Instance)
	instance.setMetadata(metadata)
	instance.setSubject(subject)

	return instance, nil
}

func NewInstance(resourceType string, subject auth.Subject) (Instance, gomerr.Gomerr) {
	instance, ge := newInstance(resourceType, subject)
	if ge != nil {
		return nil, ge
	}

	instance.OnSubject()

	return instance, nil
}

func UnmarshalInstance(resourceType string, subject auth.Subject, bytes []byte) (Instance, gomerr.Gomerr) {
	instance, ge := newInstance(resourceType, subject)
	if ge != nil {
		return nil, ge
	}

	if len(bytes) != 0 {
		if err := json.Unmarshal(bytes, &instance); err != nil {
			return nil, gomerr.Unmarshal("Instance", bytes, instance).Wrap(err)
		}
	}

	instance.OnSubject()

	return instance, nil
}

func SaveInstance(i Instance) gomerr.Gomerr {
	// Consider alt form w/ Updatable.Update() that separates resource from data
	//if ge := u.Update(u); ge != nil {
	//	return ge
	//}

	if ge := i.metadata().dataStore.Update(i, nil); ge != nil {
		return ge
	}

	return nil
}

type BaseInstance struct {
	BaseResource

	//persistedValues map[string]interface{}
}

func (*BaseInstance) Id() string {
	panic(gomerr.Configuration("Id() function must be shadowed in Instance types"))
}

func (b *BaseInstance) PersistableTypeName() string {
	return b.metadata().InstanceName()
}

func (b *BaseInstance) NewQueryable() data.Queryable {
	cqt := b.metadata().collectionQueryType
	if cqt == nil {
		return nil
	}

	collectionQuery := reflect.New(cqt.Elem()).Interface().(CollectionQuery)
	collectionQuery.setMetadata(b.metadata())
	collectionQuery.setSubject(b.Subject())
	collectionQuery.OnSubject()

	return collectionQuery
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

func (*BaseInstance) PreUpdate(Instance) gomerr.Gomerr {
	return nil
}

func (*BaseInstance) PostUpdate(Instance) gomerr.Gomerr {
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
