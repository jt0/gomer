package fields

import (
	"strconv"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
)

type AccessPrincipal string

func (f AccessPrincipal) Id() string {
	return string(f)
}

func (f AccessPrincipal) Type() auth.PrincipalType {
	return FieldAccess
}

func (f AccessPrincipal) Release(bool) gomerr.Gomerr {
	return nil
}

type fieldAccessBits uint32

const (
	FieldAccess auth.PrincipalType = "FieldAccess"

	ReadWriteAll AccessPrincipal = "ReadWriteAll"
	ReadAll      AccessPrincipal = "ReadAll"
	NoAccess     AccessPrincipal = "NoAccess"

	ReadChar   = 'r'
	WriteChar  = 'w'
	CreateChar = 'c'
	UpdateChar = 'u'
	DenyChar   = '-'

	ReadAccess = fieldAccessBits(1 << iota)
	CreateAccess
	UpdateAccess
	numAccessTypes = iota // iota is not reset, so this is the number of access types starting w/ ReadAccess
)

var reservedPrincipalsConstraint = constraint.Not(constraint.OneOf(ReadWriteAll, ReadAll, NoAccess))

func RegisterAccessPrincipals(accessPrincipals ...AccessPrincipal) {
	if ge := gomerr.Test("AccessPrincipals", accessPrincipals, constraint.MaxLength(7)); ge != nil {
		panic(ge)
	}

	for i, r := range accessPrincipals {
		if ge := gomerr.Test("AccessPrincipal.Id()", r, reservedPrincipalsConstraint); ge != nil {
			panic(ge)
		}
		bitsLocationForPrincipal[r] = uint(i)
	}
}

func (f *field) accessTag(accessTag string) gomerr.Gomerr {
	if accessTag == "" {
		return nil // no access
	}

	parts := strings.Split(accessTag, ",")
	if len(parts) > 1 {
		f.provided = parts[1] == "p" || parts[1] == "provided"
	}

	access := strings.TrimSpace(parts[0])
	if len(access) == 0 {
		return nil
	} else if len(access) > 16 {
		return gomerr.Configuration("'access' tag can support up to 8 field access principals, but got: " + strconv.Itoa(len(access)/2))
	} else if len(access)%2 != 0 {
		return gomerr.Configuration("'access' tag must have two parts for each field access principals, but got: " + access)
	}

	var accessBits fieldAccessBits
	for i := 0; i < len(access); i += 2 {
		accessBits <<= numAccessTypes // Prepare to write the next access bits.  The first time this still results in no set bits

		switch access[i] {
		case ReadChar:
			accessBits |= ReadAccess
		case DenyChar:
			// nothing to set
		default:
			return gomerr.Configuration("Expected one of read access values (r, -), but got: " + string(access[i]))
		}

		switch access[i+1] {
		case WriteChar:
			accessBits |= CreateAccess | UpdateAccess
		case CreateChar:
			accessBits |= CreateAccess
		case UpdateChar:
			accessBits |= UpdateAccess
		case DenyChar:
			// nothing to set
		default:
			return gomerr.Configuration("Expected one of write access values (w, c, u, -), but got: " + string(access[i]))
		}
	}

	f.accessBits = accessBits

	return nil
}

var bitsLocationForPrincipal = make(map[auth.Principal]uint)

func (f *field) access(accessPrincipal AccessPrincipal, accessType fieldAccessBits) bool {
	switch accessPrincipal {
	case ReadWriteAll:
		return true
	case ReadAll:
		return accessType&ReadAccess == ReadAccess
	case NoAccess:
		fallthrough
	case "":
		return false
	default:
		bitsIndex, ok := bitsLocationForPrincipal[accessPrincipal]
		if !ok {
			return false // no matching principal
		}

		accessTypeBits := accessType << (numAccessTypes * bitsIndex)
		return f.accessBits&accessTypeBits == accessTypeBits
	}
}
