package resource_test

import (
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/resource"
	"github.com/jt0/gomer/structs"
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

var _ = resource.NewIdTool(structs.StructTagDirectiveProvider{"id"})

func TestCopyFromStruct(t *testing.T) {
	source := &One{Base{"base_id", "secret_id"}, "wrapper_id"}
	tc := structs.EnsureContext().Put(resource.SourceValue, source)

	destination := &One{}
	ge := structs.ApplyTools(destination, tc, resource.DefaultIdFieldTool)
	assert.Success(t, ge)
	assert.Equals(t, source, destination)
}

func TestCopyFromAnonymous(t *testing.T) {
	source := &Two{One{Base{"base_id", "secret_id"}, "wrapper_id"}}
	tc := structs.EnsureContext().Put(resource.SourceValue, source)

	destination := &Two{}
	ge := structs.ApplyTools(destination, tc, resource.DefaultIdFieldTool)
	assert.Success(t, ge)
	assert.Equals(t, source, destination)
}

func TestTwoIdFieldsFail(t *testing.T) {
	type TwoIdsTest struct {
		Id1 string `id:""`
		Id2 string `id:""`
	}

	ge := structs.ApplyTools(TwoIdsTest{"a", "b"}, nil, resource.DefaultIdFieldTool)
	assert.ErrorType(t, ge, &gomerr.ConfigurationError{}, "Should fail due to multiple fields with 'id' struct tag")
}
