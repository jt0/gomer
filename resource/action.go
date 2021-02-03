package resource

import (
	"reflect"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/id"
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
	tc := fields.EnsureContext().Add(fields.ScopeKey, action.Name())
	switch action.(type) {
	case *createAction, queryAction:
		tc = auth.AddClearIfDeniedAction(resource.Subject(), auth.CreatePermission, tc)
	case *updateAction:
		tc = auth.AddClearIfDeniedAction(resource.Subject(), auth.UpdatePermission, tc)
	}

	// Sometimes one might want different tool contexts for different tools, but in this case we can use the same one.
	return resource.metadata().fields.ApplyTools(
		reflect.ValueOf(resource).Elem(),
		fields.ToolWithContext{auth.FieldAccessTool, tc},
		fields.ToolWithContext{fields.FieldDefaultTool, tc},
		fields.ToolWithContext{id.IdFieldTool, tc},
		fields.ToolWithContext{constraint.FieldValidationTool, tc},
	)
}
