package auth

import (
	"strconv"
	"unsafe"

	"github.com/jt0/gomer/gomerr"
)

const (
	// Reserved AccessPrincipal names:
	ReadWriteAll = "ReadWriteAll"
	ReadAll      = "ReadAll"
	NoAccess     = "NoAccess"
)

//goland:noinspection GoUnusedGlobalVariable
var (
	// Built-in, field-scoped AccessPrincipals. Additional principals can be specified by calling
	// RegisterFieldAccessPrincipals
	ReadWriteAllFields = AccessPrincipal{ReadWriteAll, fieldAccessPrincipal}
	ReadAllFields      = AccessPrincipal{ReadAll, fieldAccessPrincipal}
	NoFieldAccess      = AccessPrincipal{NoAccess, fieldAccessPrincipal}
)

func NewFieldAccessPrincipal(name string) AccessPrincipal {
	if name == ReadWriteAll || name == ReadAll || name == NoAccess {
		panic("Cannot create an AccessPrincipal with one a predefined name: " + name)
	}

	return AccessPrincipal{name /* fieldAccessScope, */, fieldAccessPrincipal}
}

// RegisterFieldAccessPrincipals allows the application to define named principals with different levels of access
// to the fields that comprise that application's domain entities.
func RegisterFieldAccessPrincipals(accessPrincipals ...AccessPrincipal) {
	l := len(accessPrincipals)
	if l > maximumRegisteredAccessPrincipals {
		panic("Too many principals - maximum count = " + strconv.Itoa(maximumRegisteredAccessPrincipals))
	}

	l--
	for i, r := range accessPrincipals {
		fieldAccessPrincipalIndexes[r] = uint(l - i)
	}
}

// At some point we may want to support additional access scopes, but for now, accessScope and fieldAccessScope
// remain unexported. When we're ready, the only thing to do is to export these, add one or more new scope types,
// and create a NewAccessPrincipal(name, accessScope) function and/or additional scope-specific helpers.
// AccessPrincipal registration can either be done per scope, or a single function can be provided. In the latter
// case, the logic will need to be updated to deal w/ mixed scopes per call.
const (
	fieldAccessScope accessScope = "Field"

	fieldAccessPrincipal PrincipalType = fieldAccessScope + "AccessPrincipal"
)

type accessScope = PrincipalType

var fieldAccessPrincipalIndexes = make(map[Principal]uint)

// AccessPrincipal corresponds to a named Principal that can be associated with permissions to grant CRUD (extensible
// to others) abilities on different things w/in an application.
type AccessPrincipal struct {
	name string
	// scope         accessScope
	principalType PrincipalType
}

func (p AccessPrincipal) Id() string {
	return p.name
}

func (p AccessPrincipal) Type() PrincipalType {
	return p.principalType
}

func (p AccessPrincipal) Release(bool) gomerr.Gomerr {
	return nil
}

type AccessPermissions uint8

//goland:noinspection GoUnusedConst
const (
	ReadPermission   AccessPermissions = 1 << iota // = 0b00000001 (1)
	reserved2                                      // = 0b00000010 (2)
	reserved3                                      // = 0b00000100 (4)
	CreatePermission                               // = 0b00001000 (8)
	UpdatePermission                               // = 0b00010000 (16)
	deletePermission                               // = 0b00100000 (32) NB: until exported, this need not be for 'delete'
	reserved7                                      // = 0b01000000 (64)
	reserved8                                      // = 0b10000000 (128)

	readPermissions  = ReadPermission
	writePermissions = CreatePermission | UpdatePermission | deletePermission
	noPermissions    = 0b00000000
	allPermissions   = ^noPermissions // 0b11111111

	sizeOfAccessPermissions          = unsafe.Sizeof(principalPermissions(0))
	sizeOfPrincipalAccessPermissions = unsafe.Sizeof(ReadPermission)

	maximumRegisteredAccessPrincipals = int(sizeOfAccessPermissions / sizeOfPrincipalAccessPermissions)

	bitsPerByte             = 8
	permissionsPerPrincipal = uint(sizeOfPrincipalAccessPermissions) * bitsPerByte
)

type principalPermissions uint32

func (p principalPermissions) grants(principal AccessPrincipal, permissionsNeeded AccessPermissions) bool {
	if permissionsNeeded == noPermissions {
		return false // TODO: should this return true or false?
	}

	switch principal.name {
	case NoAccess:
		return false
	case ReadWriteAll:
		return true
	case ReadAll:
		return ReadPermission&permissionsNeeded == ReadPermission
	default:
		return p.principalAccessPermissions(principal)&permissionsNeeded == permissionsNeeded
	}
}

func (p principalPermissions) principalAccessPermissions(principal AccessPrincipal) AccessPermissions {
	principalIndex, ok := fieldAccessPrincipalIndexes[principal]
	if !ok {
		return noPermissions
	}

	// Move the principal's permissions to the lowest-order bits, then convert to the right type which will
	// truncate the rest.
	return AccessPermissions(p >> (permissionsPerPrincipal * principalIndex))
}
