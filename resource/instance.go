package resource

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/toolkits/slice"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/logs"
)

//var validate = validator.New()

type Instance interface {
	resource
	data.Persistable

	PreCreate() *gomerr.ApplicationError
	PostCreate() *gomerr.ApplicationError
	PrePatch(patch jsonpatch.Patch) *gomerr.ApplicationError
	PostPatch(patch jsonpatch.Patch) *gomerr.ApplicationError
	PreDelete() *gomerr.ApplicationError
	PostDelete() *gomerr.ApplicationError
}

func NewInstance(resourceType string, subject auth.Subject) (Instance, *gomerr.ApplicationError) {
	metadata, ok := resourceMetadata[strings.ToLower(resourceType)]
	if !ok {
		return nil, gomerr.BadRequest("Unknown type: " + resourceType)
	}

	instance := reflect.New(metadata.instanceType.Elem()).Interface().(Instance)
	instance.setMetadata(metadata)
	instance.setSubject(subject)

	return instance, nil
}

func UnmarshallInstance(resourceType string, subject auth.Subject, bytes []byte) (Instance, *gomerr.ApplicationError) {
	instance, ae := NewInstance(resourceType, subject)
	if ae != nil {
		return nil, ae
	}

	if len(bytes) != 0 {
		if err := json.Unmarshal(bytes, &instance); err != nil {
			logs.Error.Printf("Unmarshal error while parsing '%s': %s\n", instance.metadata().instanceName, err.Error())
			return nil, gomerr.BadRequest("Unable to parse request data", fmt.Sprintf("Data does not appear to correlate to a '%s' resource", instance.metadata().instanceName))
		}
	}

	return instance, nil
}

func SaveInstance(i Instance) *gomerr.ApplicationError {
	if ae := i.metadata().dataStore.Update(i); ae != nil {
		return ae
	}

	return nil
}

func DoCreate(i Instance) (interface{}, *gomerr.ApplicationError) {
	applyFieldDefaults(i)

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

	//i.setPersisted(true)

	return scopedResult(i)
}

func DoPatch(i Instance, patch jsonpatch.Patch) (interface{}, *gomerr.ApplicationError) {
	if ae := i.PrePatch(patch); ae != nil {
		return nil, ae
	}

	if ae := i.metadata().dataStore.Read(i); ae != nil {
		return nil, ae
	}

	//i.setPersisted(true)

	if ae := validatePatch(i, patch); ae != nil {
		return nil, ae
	}

	if ae := applyPatch(i, patch); ae != nil {
		return nil, ae
	}

	//if ae := i.ValidateAction(account, Create); ae != nil {
	//	return nil, ae
	//}
	//
	if ae := i.metadata().dataStore.Update(i); ae != nil {
		return nil, ae
	}

	if ae := i.PostPatch(patch); ae != nil {
		return nil, ae
	}

	// assert i.persisted == true

	return scopedResult(i)
}

func DoDelete(i Instance) (interface{}, *gomerr.ApplicationError) {
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

func validatePatch(i Instance, patch jsonpatch.Patch) *gomerr.ApplicationError {
	if patch == nil {
		return gomerr.BadRequest("No applyPatch operations found.")
	}

	metadata := i.metadata()
	patchPaths := make([]string, 0)
	for _, field := range metadata.fields {
		if field.writable(i.Subject().Principal(FieldAccess)) {
			// TODO: handle nested/embedded structs
			patchPaths = append(patchPaths, "/"+field.externalName)
		}
	}

	for _, op := range patch {
		switch op.Kind() {
		case "add":
		case "remove":
		case "replace":
		default:
			return gomerr.BadRequest("Only 'add', 'remove', and 'replace' patch operation are supported.")
		}

		path, err := op.Path()
		if err != nil {
			return gomerr.BadRequest("Patch operation has a missing or invalid 'path'.")
		}

		if !slice.ContainsString(patchPaths, path) {
			return gomerr.Forbidden(fmt.Sprintf("Caller cannot applyPatch '%s'.", path))
		}
	}

	return nil
}

func applyPatch(i Instance, patch jsonpatch.Patch) *gomerr.ApplicationError {
	bytes, err := json.Marshal(i)
	if err != nil {
		logs.Error.Printf("Failed to marshal bytes for applyPatch: %v", err)
		return gomerr.BadRequest("Unable to apply patch operations.")
	}

	updatedBytes, err := patch.Apply(bytes)
	if err != nil {
		logs.Error.Printf("Failed to apply patch to group bytes: %v", err)
		return gomerr.BadRequest("Unable to apply patch operations.")
	}

	if err := json.Unmarshal(updatedBytes, i); err != nil {
		logs.Error.Printf("Failed to unmarshal group bytes after applyPatch: %v", err)
		return gomerr.BadRequest("Unable to apply patch operations.")
	}

	return nil
}

type BaseInstance struct {
	//persistedValues map[string]interface{}
}

func (b *BaseInstance) PreCreate() *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PostCreate() *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PrePatch(jsonpatch.Patch) *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PostPatch(jsonpatch.Patch) *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PreDelete() *gomerr.ApplicationError {
	return nil
}

func (b *BaseInstance) PostDelete() *gomerr.ApplicationError {
	return nil
}
