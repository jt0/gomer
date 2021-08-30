package auth_test

import (
	"reflect"
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/_test/helpers/structs_test"
	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
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

func TestAccessTool(t *testing.T) {
	auth.RegisterFieldAccessPrincipals(one, two)

	copiedTo := &AccessTest{}
	structs_test.RunTests(t, []structs_test.TestCase{
		{"Remove non-readable as 'one'", auth.DefaultAccessFieldTool, clear(sOne, auth.ReadPermission), all(), allExpected()},
		{"Remove non-readable as 'two'", auth.DefaultAccessFieldTool, clear(sTwo, auth.ReadPermission), all(), partial("ABCDI")},
		{"Remove non-creatable as 'one'", auth.DefaultAccessFieldTool, clear(sOne, auth.CreatePermission), all(), allExpected()},
		{"Remove non-creatable as 'two'", auth.DefaultAccessFieldTool, clear(sTwo, auth.CreatePermission), all(), partial("ABEFIJ")},
		{"Remove non-updatable as 'one'", auth.DefaultAccessFieldTool, clear(sOne, auth.UpdatePermission), all(), allExpected()},
		{"Remove non-updatable as 'two'", auth.DefaultAccessFieldTool, clear(sTwo, auth.UpdatePermission), all(), partial("ACEGIJ")},
		{"Copy provided", auth.DefaultAccessFieldTool, auth.AddCopyProvidedToContext(reflect.ValueOf(all()).Elem(), nil), copiedTo, partial("IJ")},
	})
}

func TestPermissionsWithProvidedVerifiesForwardsCompatibility(t *testing.T) {
	type test struct {
		permissions string
		error       gomerr.Gomerr
	}

	var configurationError *gomerr.ConfigurationError
	tdp := testDirectivesProvider{}
	authTool := auth.NewAccessTool(tdp)

	tests := []test{
		{"rpr-", nil},
		{"r-rp", configurationError}, // 'p' must be in the first portion
		{"rprc", configurationError}, // if 'p', other principals must be non-writable
		{"rprp", configurationError}, // if 'p', other principals cannot specify 'p'
	}
	for _, tt := range tests {
		t.Run(tt.permissions, func(t *testing.T) {
			tdp.directive = tt.permissions
			_, ge := structs.PrepareTools(reflect.TypeOf(test{}), authTool)
			if tt.error == nil {
				assert.Success(t, ge)
			} else {
				assert.ErrorType(t, ge, tt.error, "Non-forwards compatible permissions were not properly detected.")
			}
		})
	}
}

func clear(subject auth.Subject, permission auth.AccessPermissions) structs.ToolContext {
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

type testDirectivesProvider struct {
	directive string
}

func (t testDirectivesProvider) Get(reflect.Type, reflect.StructField) string {
	return t.directive
}
