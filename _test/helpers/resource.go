package helpers

import (
	"encoding/json"
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
)

func Unmarshal(resourceType reflect.Type, subject auth.Subject, bytes []byte) (resource.Resource, gomerr.Gomerr) {
	newResource, ge := resource.New(resourceType, subject)
	if ge != nil {
		return nil, ge
	}

	if len(bytes) != 0 {
		if err := json.Unmarshal(bytes, &newResource); err != nil {
			return nil, gomerr.Unmarshal("Resource", bytes, resourceType).Wrap(err)
		}
	}

	return newResource, nil
}
