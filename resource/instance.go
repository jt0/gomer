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

	PreCreate() gomerr.Gomerr
	PostCreate() gomerr.Gomerr
	PreGet() gomerr.Gomerr
	PostGet() gomerr.Gomerr
	PreUpdate(updateInstance Instance) gomerr.Gomerr
	PostUpdate(updateInstance Instance) gomerr.Gomerr
	PreDelete() gomerr.Gomerr
	PostDelete() gomerr.Gomerr
	PostQuery() gomerr.Gomerr
}

func newInstance(resourceType string, subject auth.Subject) (Instance, gomerr.Gomerr) {
	metadata, ok := resourceMetadata[strings.ToLower(resourceType)]
	if !ok {
		return nil, gomerr.NotFound("resource type", resourceType).AddCulprit(gomerr.Client)
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
			return nil, gomerr.Unmarshal(err, bytes, instance).AddCulprit(gomerr.Client)
		}
	}

	instance.OnSubject()

	return instance, nil
}

func SaveInstance(i Instance) gomerr.Gomerr {
	if ge := i.metadata().dataStore.Update(i, nil); ge != nil {
		return ge
	}

	return nil
}

func DoCreate(i Instance) (result interface{}, ge gomerr.Gomerr) {
	if ge = i.metadata().fields.removeNonWritable(i, createAccess); ge != nil {
		return nil, ge.AddNotes("unable to clean instance before processing").AddCulprit(gomerr.Configuration)
	}

	if ge = i.metadata().fields.applyDefaults(i); ge != nil {
		return nil, ge.AddNotes("could not apply defaults during create")
	}

	if ge = i.PreCreate(); ge != nil {
		return nil, ge
	}

	//if err := validate.Struct(i); err != nil {
	//	return nil, gomerr.ValidationFailure(err)
	//}

	if limiter, lge := limit(checkAndIncrement, i); lge != nil {
		return nil, lge
	} else if limiter != nil {
		defer saveLimiter(limiter, ge)
	}

	if ge = i.metadata().dataStore.Create(i); ge != nil {
		return nil, ge
	}

	if ge = i.PostCreate(); ge != nil {
		return nil, ge
	}

	return scopedResult(i)
}

func DoGet(i Instance) (interface{}, gomerr.Gomerr) {
	if ge := i.PreGet(); ge != nil {
		return nil, ge
	}

	if ge := i.metadata().dataStore.Read(i); ge != nil {
		return nil, ge
	}

	if ge := i.PostGet(); ge != nil {
		return nil, ge
	}

	return scopedResult(i)
}

func DoUpdate(updateInstance Instance) (interface{}, gomerr.Gomerr) {
	// copy update to a new instance and read data into it
	i := shallowCopy(updateInstance)
	if ge := i.metadata().dataStore.Read(i); ge != nil {
		return nil, ge.AddNotes("unable to retrieve persisted instance for update")
	}

	if ge := i.metadata().fields.removeNonWritable(updateInstance, updateAccess); ge != nil {
		return nil, ge.AddNotes("unable to clean updateInstance before processing").AddCulprit(gomerr.Configuration)
	}

	if ge := i.PreUpdate(updateInstance); ge != nil {
		return nil, ge
	}

	if ge := i.metadata().dataStore.Update(i, updateInstance); ge != nil {
		return nil, ge
	}

	if ge := i.PostUpdate(updateInstance); ge != nil {
		return nil, ge
	}

	return scopedResult(i)
}

func shallowCopy(update Instance) Instance {
	updateCopy := reflect.ValueOf(update).Elem().Interface()
	persistedPtr := reflect.New(reflect.TypeOf(updateCopy))
	persistedPtr.Elem().Set(reflect.ValueOf(updateCopy))
	persisted := persistedPtr.Interface().(Instance)

	return persisted
}

func scopedResult(i Instance) (interface{}, gomerr.Gomerr) {
	if result := i.metadata().fields.removeNonReadable(i); result == nil || len(result) == 0 {
		return nil, gomerr.NotFound(i.PersistableTypeName(), i.Id()).AddCulprit(gomerr.Client)
	} else {
		return result, nil
	}
}

func DoDelete(i Instance) (result interface{}, ge gomerr.Gomerr) {
	if ge = i.PreDelete(); ge != nil {
		return nil, ge
	}

	if limiter, lge := limit(decrement, i); lge != nil {
		return nil, lge
	} else if limiter != nil {
		defer saveLimiter(limiter, ge)
	}

	if ge = i.metadata().dataStore.Delete(i); ge != nil {
		return nil, ge
	}

	if ge = i.PostDelete(); ge != nil {
		return nil, ge
	}

	return scopedResult(i)
}

type BaseInstance struct {
	BaseResource

	//persistedValues map[string]interface{}
}

func (b *BaseInstance) Id() string {
	panic(gomerr.Unsupported("shadow Id() from the embedding struct"))
}

func (b *BaseInstance) PersistableTypeName() string {
	return b.md.instanceName
}

func (b *BaseInstance) NewQueryable() data.Queryable {
	cqt := b.md.collectionQueryType
	if cqt == nil {
		return nil
	}

	collectionQuery := reflect.New(cqt.Elem()).Interface().(CollectionQuery)
	collectionQuery.setMetadata(b.md)
	collectionQuery.setSubject(b.sub)
	collectionQuery.OnSubject()

	return collectionQuery
}

func (b *BaseInstance) PreCreate() gomerr.Gomerr {
	return nil
}

func (b *BaseInstance) PostCreate() gomerr.Gomerr {
	return nil
}

func (b *BaseInstance) PreGet() gomerr.Gomerr {
	return nil
}

func (b *BaseInstance) PostGet() gomerr.Gomerr {
	return nil
}

func (b *BaseInstance) PreUpdate(_ Instance) gomerr.Gomerr {
	return nil
}

func (b *BaseInstance) PostUpdate(_ Instance) gomerr.Gomerr {
	return nil
}

func (b *BaseInstance) PreDelete() gomerr.Gomerr {
	return nil
}

func (b *BaseInstance) PostDelete() gomerr.Gomerr {
	return nil
}

func (b *BaseInstance) PostQuery() gomerr.Gomerr {
	return nil
}
