package id_test

import (
	"reflect"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/id"
)

type Base struct {
	BaseId   string
	SecretId string
}

type One struct {
	Base
	Id string `id:"BaseId,~SecretId"`
}

type Two struct {
	One
}

func init() {
	fields.FieldToolConfigProvider = fields.StructTagConfigProvider{}.WithKey("id", id.CopyIdsFieldTool())
}

func TestCopyFromStruct(t *testing.T) {
	source := &One{Base{"base_id", "secret_id"}, "wrapper_id"}
	toolContext := fields.EnsureContext().Add(id.SourceValue, source)

	destination := &One{}
	targetType := reflect.TypeOf(destination).Elem()
	fs, ge := fields.Get(targetType)
	assert.Success(t, ge)

	ge = fs.ApplyTools(reflect.ValueOf(destination).Elem(), fields.Application{id.CopyIdsFieldTool().Name(), toolContext})
	assert.Success(t, ge)
	assert.Equals(t, source, destination)
}

func TestCopyFromAnonymous(t *testing.T) {
	source := &Two{One{Base{"base_id", "secret_id"}, "wrapper_id"}}
	toolContext := fields.EnsureContext().Add(id.SourceValue, source)

	destination := &Two{}
	targetType := reflect.TypeOf(destination).Elem()
	fs, ge := fields.Get(targetType)
	assert.Success(t, ge)

	ge = fs.ApplyTools(reflect.ValueOf(destination).Elem(), fields.Application{id.CopyIdsFieldTool().Name(), toolContext})
	assert.Success(t, ge)
	assert.Equals(t, source, destination)
}

func TestTwoIdFieldsFail(t *testing.T) {
	type TwoIdsTest struct {
		Id1 string `id:""`
		Id2 string `id:""`
	}

	_, ge := fields.Get(reflect.TypeOf(TwoIdsTest{}))
	assert.ErrorType(t, ge, &gomerr.ConfigurationError{}, "Should fail due to multiple fields with 'id' struct tag")
}
