package resource

import (
	"reflect"

	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

type Action interface {
	Name() string
	ResourceType() Type
	Pre(Resource) gomerr.Gomerr
	Do(Resource) gomerr.Gomerr
	OnDoSuccess(Resource) (Resource, gomerr.Gomerr)
	OnDoFailure(Resource, gomerr.Gomerr) gomerr.Gomerr
}

func DoAction(resource Resource, action Action) (Resource, gomerr.Gomerr) {
	if ge := prepareAndValidateFields(resource, action); ge != nil {
		return nil, ge
	}

	if ge := action.Pre(resource); ge != nil {
		return nil, ge
	}

	if ge := action.Do(resource); ge != nil {
		return nil, action.OnDoFailure(resource, ge)
	}

	return action.OnDoSuccess(resource)
}

func prepareAndValidateFields(resource Resource, action Action) gomerr.Gomerr {
	rv := reflect.ValueOf(resource).Elem()

	// TODO:p1 Have InstanceAction provide WriteAccess() instead of checking Action type directly. Alternatively,
	//         implement enhancements to 'fields' package that would support this (along w/ other use cases).
	fs := resource.metadata().fields
	switch action.(type) {
	case *createAction, queryAction:
		if ge := fs.RemoveNonWritable(rv, fields.CreateAccess, resource.Subject().Principal(fields.FieldAccess).(fields.AccessPrincipal)); ge != nil {
			return ge
		}
	case *updateAction:
		if ge := fs.RemoveNonWritable(rv, fields.UpdateAccess, resource.Subject().Principal(fields.FieldAccess).(fields.AccessPrincipal)); ge != nil {
			return ge
		}
	}

	// TODO:p2 Consider adding 'context' to 'default' tags as well. May be useful if, for example, an update
	//         action takes in directive fields that should default to something
	if ge := fs.ApplyDefaults(rv, action.Name()); ge != nil {
		return ge
	}

	if ge := fs.Validate(rv, action.Name()); ge != nil {
		return ge
	}

	return nil
}
