package fields

import (
	"fmt"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
)

type AccessPrincipal string

const (
	FieldAccess auth.PrincipalType = "FieldAccess"

	ReadWriteAll AccessPrincipal = "ReadWriteAll"
	ReadAll      AccessPrincipal = "ReadAll"
	NoAccess     AccessPrincipal = "NoAccess"
)

func (f AccessPrincipal) Id() string {
	return string(f)
}

func (f AccessPrincipal) Type() auth.PrincipalType {
	return FieldAccess
}

func (f AccessPrincipal) Release(_ bool) gomerr.Gomerr {
	return nil
}

var bitsLocationForPrincipal = make(map[auth.Principal]uint)
var notReservedPrincipalsConstraint = constraint.Not(constraint.OneOf(ReadWriteAll, ReadAll, NoAccess))

func RegisterAccessPrincipals(accessPrincipals ...AccessPrincipal) {
	if len(accessPrincipals) > 7 {
		panic(fmt.Sprintf("too many accessPrincipals (maximum = 7): %v", accessPrincipals))
	}

	for i, r := range accessPrincipals {
		if ge := gomerr.Test("AccessPrincipal.Id()", r, notReservedPrincipalsConstraint); ge != nil {
			panic(ge)
		}

		bitsLocationForPrincipal[r] = uint(i)
	}
}
