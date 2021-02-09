package auth_test

import (
	"reflect"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/_test/helpers/fields_test"
	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/fields"
	"github.com/jt0/gomer/gomerr"
)

type AccessTest struct {
	A string `access:"rwrw"`
	B string `access:"rwrc"`
	C string `access:"rwru"`
	D string `access:"rwr-"`
	E string `access:"rw-w"`
	F string `access:"rw-c"`
	G string `access:"rw-u"`
	H string `access:"rw--"`
	I string `access:"rpr-"` // acts like writable for both principals
	J string `access:"rp--"` // acts like writable for both principals
	K string
}

var (
	one = auth.NewFieldAccessPrincipal("one")
	two = auth.NewFieldAccessPrincipal("two")

	sOne = auth.NewSubject(one)
	sTwo = auth.NewSubject(two)
)

func init() {
	fields.SetTagKeyToFieldToolMap(map[string]fields.FieldTool{
		"access": auth.FieldAccessTool,
	})

	auth.RegisterFieldAccessPrincipals(one, two)
}

func TestAccessTool(t *testing.T) {
	copiedTo := &AccessTest{}
	fields_test.RunTests(t, []fields_test.TestCase{
		{"Remove non-readable as 'one'", auth.FieldAccessTool, clear(sOne, auth.ReadPermission), all(), nil, allExpected()},
		{"Remove non-readable as 'two'", auth.FieldAccessTool, clear(sTwo, auth.ReadPermission), all(), nil, partial("ABCDI")},
		{"Remove non-creatable as 'one'", auth.FieldAccessTool, clear(sOne, auth.CreatePermission), all(), nil, allExpected()},
		{"Remove non-creatable as 'two'", auth.FieldAccessTool, clear(sTwo, auth.CreatePermission), all(), nil, partial("ABEFIJ")},
		{"Remove non-updatable as 'one'", auth.FieldAccessTool, clear(sOne, auth.UpdatePermission), all(), nil, allExpected()},
		{"Remove non-updatable as 'two'", auth.FieldAccessTool, clear(sTwo, auth.UpdatePermission), all(), nil, partial("ACEGIJ")},
		{"Copy provided", auth.FieldAccessTool, auth.AddCopyProvidedToContext(reflect.ValueOf(all()).Elem(), nil), copiedTo, copiedTo, partial("IJ")},
	})
}

func TestPermissionsWithProvidedVerifiesForwardsCompatibility(t *testing.T) {
	type test struct {
		permissions string
		error       gomerr.Gomerr
	}

	structType := reflect.TypeOf(test{})
	structField, _ := structType.FieldByName("permissions")

	var configurationError *gomerr.ConfigurationError

	tests := []test{
		{"rpr-", nil},
		{"r-rp", configurationError}, // 'p' must be in the first portion
		{"rprc", configurationError}, // if 'p', other principals must be non-writable
		{"rprp", configurationError}, // if 'p', other principals cannot specify 'p'
	}
	for _, tt := range tests {
		t.Run(tt.permissions, func(t *testing.T) {
			_, ge := auth.FieldAccessTool.New(structType, structField, tt.permissions)
			if tt.error == nil {
				assert.Success(t, ge)
			} else {
				assert.ErrorType(t, ge, tt.error, "Non-forwards compatible permissions were not properly detected.")
			}
		})
	}
}

func clear(subject auth.Subject, permission auth.AccessPermissions) fields.ToolContext {
	return auth.AddClearIfDeniedToContext(subject, permission, nil)
}

func all() *AccessTest {
	return &AccessTest{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"}
}

func allExpected() *AccessTest {
	return &AccessTest{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", ""}
}

func partial(assigned string) *AccessTest {
	a := &AccessTest{}
	av := reflect.ValueOf(a).Elem()
	for _, c := range assigned {
		s := string(c)
		av.FieldByName(s).SetString(s)
	}
	return a
}
