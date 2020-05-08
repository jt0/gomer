package resource

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/logs"
)

type Instance interface {
	resource
	data.Persistable

	PreCreate() *gomerr.ApplicationError
	PostCreate() *gomerr.ApplicationError
	PreGet() *gomerr.ApplicationError
	PostGet() *gomerr.ApplicationError
	PreUpdate(updateInstance Instance) *gomerr.ApplicationError
	PostUpdate(updateInstance Instance) *gomerr.ApplicationError
	PreDelete() *gomerr.ApplicationError
	PostDelete() *gomerr.ApplicationError
	PostQuery() *gomerr.ApplicationError
}

func newInstance(resourceType string, subject auth.Subject) (Instance, *gomerr.ApplicationError) {
	metadata, ok := resourceMetadata[strings.ToLower(resourceType)]
	if !ok {
		return nil, gomerr.BadRequest("Unknown type: " + resourceType)
	}

	instance := reflect.New(metadata.instanceType.Elem()).Interface().(Instance)
	instance.setMetadata(metadata)
	instance.setSubject(subject)

	return instance, nil
}

func NewInstance(resourceType string, subject auth.Subject) (Instance, *gomerr.ApplicationError) {
	instance, ae := newInstance(resourceType, subject)
	if ae != nil {
		return nil, ae
	}

	instance.OnSubject()

	return instance, nil
}

func UnmarshalInstance(resourceType string, subject auth.Subject, bytes []byte) (Instance, *gomerr.ApplicationError) {
	instance, ae := newInstance(resourceType, subject)
	if ae != nil {
		return nil, ae
	}

	if len(bytes) != 0 {
		if err := json.Unmarshal(bytes, &instance); err != nil {
			logs.Error.Printf("Unmarshal error while parsing '%s': %s\n", resourceType, err.Error())
			return nil, gomerr.BadRequest("Unable to parse request data", fmt.Sprintf("Data does not appear to correlate to a '%s' resource", instance.metadata().instanceName))
		}
	}

	instance.OnSubject()

	return instance, nil
}

func SaveInstance(i Instance) *gomerr.ApplicationError {
	if ae := i.metadata().dataStore.Update(i, nil); ae != nil {
		return ae
	}

	return nil
}

func DoCreate(i Instance) (result interface{}, ae *gomerr.ApplicationError) {
	if limiterInstance, ae := limit(checkAndIncrement, i); ae != nil {
		return nil, ae
	} else {
		defer saveLimiter(limiterInstance)
	}

	i.metadata().fields.applyDefaults(i)

	if ae := i.PreCreate(); ae != nil {
		return nil, ae
	}

	//if err := validate.Struct(i); err != nil {
	//	return nil, gomerr.ValidationFailure(err)
	//}

	if ae := i.metadata().dataStore.Create(i); ae != nil {
		return nil, ae
	}

	if ae := i.PostCreate(); ae != nil {
		return nil, ae
	}

	return scopedResult(i)
}

func DoGet(i Instance) (interface{}, *gomerr.ApplicationError) {
	if ae := i.metadata().dataStore.Read(i); ae != nil {
		return nil, ae
	}

	return scopedResult(i)
}

func DoUpdate(updateInstance Instance) (interface{}, *gomerr.ApplicationError) {
	// copy update to a new instance and read data into it
	i := shallowCopy(updateInstance)
	if ae := i.metadata().dataStore.Read(i); ae != nil {
		return nil, ae
	}

	if ae := i.metadata().fields.removeNonWritable(updateInstance); ae != nil {
		return nil, ae
	}

	if ae := i.PreUpdate(updateInstance); ae != nil {
		return nil, ae
	}

	if ae := i.metadata().dataStore.Update(i, updateInstance); ae != nil {
		return nil, ae
	}

	if ae := i.PostUpdate(updateInstance); ae != nil {
		return nil, ae
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

func scopedResult(i Instance) (interface{}, *gomerr.ApplicationError) {
	if result := i.metadata().fields.removeNonReadable(i); result == nil || len(result) == 0 {
		return nil, gomerr.ResourceNotFound(i)
	} else {
		return result, nil
	}
}

func DoDelete(i Instance) (interface{}, *gomerr.ApplicationError) {
	if limiter, ae := limit(decrement, i); ae != nil {
		return nil, ae
	} else {
		defer saveLimiter(limiter)
	}

	if ae := i.PreDelete(); ae != nil {
		return nil, ae
	}

	if ae := i.metadata().dataStore.Delete(i); ae != nil {
		return nil, ae
	}

	if ae := i.PostDelete(); ae != nil {
		return nil, ae
	}

	return scopedResult(i)
}

type BaseInstance struct {
	BaseResource

	//persistedValues map[string]interface{}
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

func (b *BaseInstance) PreCreate() *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PostCreate() *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PreGet() *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PostGet() *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PreUpdate(_ Instance) *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PostUpdate(_ Instance) *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PreDelete() *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PostDelete() *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PostQuery() *gomerr.ApplicationError {
	return nil
}
