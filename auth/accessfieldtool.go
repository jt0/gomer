package auth

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

const (
	ReadChar     = 'r' // ReadPermission
	WriteChar    = 'w' // CreatePermission | UpdatePermission
	CreateChar   = 'c' // CreatePermission
	UpdateChar   = 'u' // UpdatePermission
	ProvidedChar = 'p' // Provided (field's value is provided by and should be ignored)
	DenyChar     = '-' // No access
)

// A FieldAccessTool is used to validate that a principal that is performing an action against the fields of a
// particular struct has permissions that grant it to do whatever it's trying to do. For example, an application may
// define three AccessPrincipal types: 'admin', 'user', and 'guest', and each has a different set of fields that
// they can read or write to. An 'admin' may be able to read and write any field, whereas a 'guest' may not be able
// to write to any fields and can read only a subset of attributes. A 'user' may be able to read and write a set of
// values, but may still not have permissions to modify certain field values.
//
// Indicating the permissions per principal is done by a Unix filesystem-like 'mode' string (e.g. "rw-r-r-"). While
// a 'mode' string uses three characters per principal, the FieldAccessTool only uses two. The first indicates whether a
// field is readable (r) or not (-), and the second character indicates whether the field is writable during create (c),
// update (u), both (w), or neither (-). As an example, an application with the three principals from above may define a
// field's access permissions as "rwrcr-", meaning the 'admin' principal can read and write its value, a 'user' can
// specify the field's value when the struct is created but not change it afterwards, and a 'guest' principal can only
// read the value.
//
// A special type of permission is used to indicate a field that is not writable, but is instead provided (p) by the
// application itself in some way and so its value can be ignored. For example, a field that contains a struct's
// identifier may be auto-populated by the application (e.g. via a path parameter), so it doesn't make sense to describe
// the value as writable by any of the principals. Note that if there was some special case where, say, an identifier
// value needed to be updated, the application could use the predefined ReadWriteAll principal to do so.
//
// We don't currently see a use case for allowing some principals to treat an attribute as provided and others not to.
// To keep the door open for this, though, we require that to specify a field is provided, the 'p' must be set in the
// leftmost permissions group's write location, and the other permission groups set their write permission value to '-'.

var (
	DefaultAccessFieldTool = NewAccessTool(structs.StructTagDirectiveProvider{"access"})

	accessRegexp = regexp.MustCompile("(r|-)(w|c|u|p|-)")
	accessGroups = []string{"", "read", "write"}
)

func NewAccessTool(dp structs.DirectiveProvider) *structs.Tool {
	return structs.NewTool(accessToolType, accessApplierProvider{}, dp)
}

type accessApplier struct {
	fieldName   string
	permissions principalPermissions
	provided    bool
	zeroVal     reflect.Value
}

func (a accessApplier) Apply(_ reflect.Value, fv reflect.Value, tc structs.ToolContext) gomerr.Gomerr {
	accessAction, ok := tc[accessToolAction].(action)
	if !ok {
		return nil // no action specified, return
	}

	return accessAction.do(fv, a, tc)
}

const (
	accessToolType   = "auth.AccessTool"
	accessToolAction = "$_access_tool_action"
)

type action interface {
	do(fieldValue reflect.Value, accessTool accessApplier, toolContext structs.ToolContext) gomerr.Gomerr
}

func AddClearIfDeniedToContext(subject Subject, accessPermission AccessPermissions, tcs ...structs.ToolContext) structs.ToolContext {
	// If no access principal, all permissions will be denied
	accessPrincipal, _ := subject.Principal(fieldAccessPrincipal).(AccessPrincipal)
	return structs.EnsureContext(tcs...).Put(accessToolAction, remover{accessPrincipal, accessPermission})
}

type remover struct {
	principal  AccessPrincipal
	permission AccessPermissions
}

func (r remover) do(fv reflect.Value, aa accessApplier, _ structs.ToolContext) (ge gomerr.Gomerr) {
	defer func() {
		if r := recover(); r != nil {
			ge = gomerr.Unprocessable("Unable to remove non-writable field", r)
		}
	}()

	if !aa.permissions.grants(r.principal, r.permission) && !(aa.provided && writable(r.permission)) {
		fv.Set(aa.zeroVal)
		return nil
	}

	return nil
}

type accessApplierProvider struct {
}

func (ap accessApplierProvider) Applier(_ reflect.Type, sf reflect.StructField, directive string) (structs.Applier, gomerr.Gomerr) {
	perPrincipalPermissions := make([]map[string]string, 0)
	for _, match := range accessRegexp.FindAllStringSubmatch(directive, -1) {
		values := make(map[string]string)
		for i, value := range match {
			key := accessGroups[i]
			if key == "" {
				continue
			}
			values[key] = strings.TrimSpace(value)
		}

		perPrincipalPermissions = append(perPrincipalPermissions, values)
	}
	ppPermissionsCount := len(perPrincipalPermissions)

	// If a field has defined no access permissions (by it being absent or via the empty string), we bypass the error
	// and the resulting (empty) fieldPermissions will deny access to all registered principals.
	if ppPermissionsCount > 0 && ppPermissionsCount != len(fieldAccessPrincipalIndexes) {
		return nil, gomerr.Configuration("Incorrect number of 'access' AccessPermissions").
			AddAttribute("Expected", len(fieldAccessPrincipalIndexes)).
			AddAttribute("Actual", len(perPrincipalPermissions))
	}

	var fieldPermissions principalPermissions
	var provided bool
	for i := 0; i < ppPermissionsCount; i++ {
		var principalAccess AccessPermissions

		switch perPrincipalPermissions[i]["read"][0] {
		case ReadChar:
			principalAccess |= ReadPermission
		case DenyChar:
			// nothing to set
		}

		var provides bool
		switch perPrincipalPermissions[i]["write"][0] {
		case WriteChar:
			principalAccess |= CreatePermission | UpdatePermission
		case CreateChar:
			principalAccess |= CreatePermission
		case UpdateChar:
			principalAccess |= UpdatePermission
		case ProvidedChar:
			provided, provides = true, true
		case DenyChar:
			// nothing to set
		}

		if i > 0 && provides || provided && writable(principalAccess) {
			return nil, gomerr.Configuration("To provide Principal permissions (other than the leftmost) cannot specify 'p'." +
				" If 'p' was correctly specified, all other principals must indicate '-' for their write permissions.")
		}

		fieldPermissions = (fieldPermissions << permissionsPerPrincipal) | principalPermissions(principalAccess)
	}

	return accessApplier{
		fieldName:   sf.Name,
		permissions: fieldPermissions,
		provided:    provided,
		zeroVal:     reflect.Zero(sf.Type),
	}, nil
}

func AddCopyProvidedToContext(fromStruct reflect.Value, tcs ...structs.ToolContext) structs.ToolContext {
	return structs.EnsureContext(tcs...).Put(accessToolAction, copyProvided(fromStruct))
}

type copyProvided reflect.Value

func (cf copyProvided) do(fv reflect.Value, at accessApplier, _ structs.ToolContext) (ge gomerr.Gomerr) {
	defer func() {
		if r := recover(); r != nil {
			ge = gomerr.Unprocessable("Unable to copy field", r)
		}
	}()

	if !at.provided {
		return nil
	}

	fromFv := reflect.Value(cf).FieldByName(at.fieldName)
	if !fromFv.IsValid() || fromFv.IsZero() {
		return nil
	}

	fv.Set(fromFv)

	return nil
}

func writable(permissions AccessPermissions) bool {
	return permissions&LifecyclePermissions != 0
}
