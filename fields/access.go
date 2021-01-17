package fields

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/gomerr"
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

	ReadChar     = 'r'
	WriteChar    = 'w'
	CreateChar   = 'c'
	UpdateChar   = 'u'
	ProvidedChar = 'p'
	DenyChar     = '-'
)

const (
	ReadAccess fieldAccessBits = 1 << iota
	CreateAccess
	UpdateAccess
	numAccessTypes = iota // iota is not reset, so this is the number of access types starting w/ ReadAccess
)

var (
	maxPrincipalsConstraint      = constraint.MaxLength(7)
	reservedPrincipalsConstraint = constraint.Not(constraint.OneOf(ReadWriteAll, ReadAll, NoAccess))
)

func RegisterAccessPrincipals(accessPrincipals ...AccessPrincipal) {
	if ge := maxPrincipalsConstraint.Validate(accessPrincipals); ge != nil {
		panic(ge)
	}

	for i, r := range accessPrincipals {
		if ge := reservedPrincipalsConstraint.Validate(r); ge != nil {
			panic(ge)
		}
		bitsLocationForPrincipal[r] = uint(i)
	}
}

func (f *field) accessTag(accessTag string) gomerr.Gomerr {
	if accessTag == "" {
		return nil // no access
	}

	access := strings.TrimSpace(accessTag)
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
		case ProvidedChar:
			f.provided = true
		case DenyChar:
			// nothing to set
		default:
			return gomerr.Configuration("Expected one of write access values (w, c, u, p, -), but got: " + string(access[i]))
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
	case NoAccess, "":
		return false
	default:
		if accessType == 0 {
			return false
		}

		bitsIndex, ok := bitsLocationForPrincipal[accessPrincipal]
		if !ok {
			return false // no matching principal
		}

		accessTypeBits := accessType << (numAccessTypes * bitsIndex)
		return f.accessBits&accessTypeBits == accessTypeBits
	}
}

func (fs *Fields) RemoveNonReadable(v reflect.Value, principal AccessPrincipal) bool {
	empty := true // Start w/ the assumption that all fields will be zeroed out

	for _, field := range fs.fieldMap {
		if field.access(principal, ReadAccess) {
			empty = false // At least one field is present, so mark empty as false
		} else {
			fv := v.FieldByName(field.name)
			if !fv.IsValid() || fv.IsZero() {
				continue
			}

			// TODO:p0 need to recurse on struct values
			fv.Set(field.zeroVal)
		}
	}

	return empty
}

var canSet = constraint.NewType(func(value interface{}) bool { return value.(reflect.Value).CanSet() }, "CanSet", true)

func (fs *Fields) RemoveNonWritable(v reflect.Value, accessType fieldAccessBits, principal AccessPrincipal) gomerr.Gomerr {
	for _, field := range fs.fieldMap {
		// TODO:p1 handle nested/embedded structs
		if field.provided || field.access(principal, accessType) { //  || strings.Contains(field.location, ".")
			continue
		}

		fv := v.FieldByName(field.name)
		if !fv.IsValid() || fv.IsZero() {
			continue
		}

		if ge := canSet.Validate(fv); ge != nil {
			return ge.AddAttribute("field.name", field.name)
		}

		fv.Set(field.zeroVal)
	}

	return nil
}

func (fs *Fields) CopyProvided(from, to reflect.Value) gomerr.Gomerr {
	for _, field := range fs.fieldMap {
		// TODO: handle nested/embedded structs
		if !field.provided || strings.Contains(field.location, ".") {
			continue
		}

		ffv := from.FieldByName(field.name)
		if !ffv.IsValid() || ffv.IsZero() {
			continue
		}

		tfv := to.FieldByName(field.name)
		if ge := canSet.Validate(tfv); ge != nil {
			return ge.AddAttribute("field.name", field.name)
		}

		tfv.Set(ffv)
	}

	return nil
}
