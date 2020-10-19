package resource

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/gomerr/constraint"
)

type field struct {
	name                 string
	externalName         string
	location             string
	accessBits           fieldAccessBits
	provided             bool
	defaultValue         string
	defaultValueFunction FieldDefaultFunction
	bypassDefaultIfSet   bool
	zeroVal              reflect.Value
}

type fieldAccessBits uint32
type FieldDefaultFunction func(Instance) interface{}

func (f *field) idTag(idTag string) gomerr.Gomerr {
	parts := strings.Split(idTag, ",")

	if len(parts) > 2 {
		return gomerr.Configuration("Expected format '(<defaultTagValue>)?(,<externalName>)?', but got: " + idTag)
	}

	f.defaultTag(parts[0])

	if len(parts) == 2 {
		f.externalName = strings.TrimSpace(parts[1])
	}

	return nil
}

func (f *field) externalNameTag(nameTag string) {
	if nameTag == "" {
		f.externalName = f.name
		return
	}

	nameTagParts := strings.Split(nameTag, ",")
	name := strings.TrimSpace(nameTagParts[0])

	if name == "" {
		f.externalName = f.name
		return
	}

	f.externalName = name
}

const (
	readAccess = fieldAccessBits(1 << iota)
	createAccess
	updateAccess
	numAccessTypes = iota // iota is not reset, so this is the number of access types starting w/ readAccess

	readChar   = 'r'
	writeChar  = 'w'
	createChar = 'c'
	updateChar = 'u'
	dashChar   = '-'
)

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
	} else if !mod2(access) {
		return gomerr.Configuration("'access' tag must have two parts for each field access principals, but got: " + access)
	}

	var accessBits fieldAccessBits
	for i := 0; i < len(access); i += 2 {
		accessBits <<= numAccessTypes // Prepare to write the next access bits.  The first time this still results in no set bits

		switch access[i] {
		case readChar:
			accessBits |= readAccess
		case dashChar:
			// nothing to set
		default:
			return gomerr.Configuration("Expected one of read access values (r, -), but got: " + string(access[i]))
		}

		switch access[i+1] {
		case writeChar:
			accessBits |= createAccess | updateAccess
		case createChar:
			accessBits |= createAccess
		case updateChar:
			accessBits |= updateAccess
		case dashChar:
			// nothing to set
		default:
			return gomerr.Configuration("Expected one of write access values (w, c, u, -), but got: " + string(access[i]))
		}
	}

	f.accessBits = accessBits

	return nil
}

func (f *field) access(fieldAccessPrincipal auth.Principal, accessType fieldAccessBits) bool {
	switch fieldAccessPrincipal {
	case ReadWriteAll:
		return true
	case ReadAll:
		return accessType&readAccess == readAccess
	case NoAccess:
		fallthrough
	case nil:
		return false
	default:
		bitsIndex, ok := bitsLocationForPrincipal[fieldAccessPrincipal]
		if !ok {
			return false // no matching principal
		}

		accessTypeBits := accessType << (numAccessTypes * bitsIndex)
		return f.accessBits&accessTypeBits == accessTypeBits
	}
}

func mod2(i interface{}) bool {
	return len(i.(string))%2 == 0
}

func (f *field) defaultTag(defaultTag string) {
	if defaultTag == "" {
		return
	}

	if defaultTag[:1] == "?" {
		f.bypassDefaultIfSet = true
		defaultTag = defaultTag[1:]
	}

	if defaultTag[:1] == "$" {
		if fn, ok := fieldDefaultFunctions[defaultTag]; ok {
			f.defaultValueFunction = fn
			return
		}
	}

	f.defaultValue = defaultTag
}

type FieldAccessPrincipal string

const (
	FieldAccess auth.PrincipalType = "FieldAccess"

	ReadWriteAll FieldAccessPrincipal = "ReadWriteAll"
	ReadAll      FieldAccessPrincipal = "ReadAll"
	NoAccess     FieldAccessPrincipal = "NoAccess"
)

func (f FieldAccessPrincipal) Id() string {
	return string(f)
}

func (f FieldAccessPrincipal) Type() auth.PrincipalType {
	return FieldAccess
}

func (f FieldAccessPrincipal) Release(_ bool) gomerr.Gomerr {
	return nil
}

var bitsLocationForPrincipal = make(map[auth.Principal]uint)
var notReservedPrincipalsConstraint = constraint.Not(constraint.OneOf(ReadWriteAll, ReadAll, NoAccess))

func RegisterFieldAccessPrincipals(fieldAccessPrincipals ...FieldAccessPrincipal) {
	if len(fieldAccessPrincipals) > 7 {
		panic(fmt.Sprintf("too many fieldAccessPrincipals (maximum = 7): %v", fieldAccessPrincipals))
	}

	for i, r := range fieldAccessPrincipals {
		if ge := gomerr.Test("FieldAccessPrincipal.Id()", r, notReservedPrincipalsConstraint); ge != nil {
			panic(ge)
		}

		bitsLocationForPrincipal[r] = uint(i)
	}
}

var fieldDefaultFunctions map[string]FieldDefaultFunction

func RegisterFieldDefaultFunctions(functions map[string]FieldDefaultFunction) {
	for k := range functions {
		if k[:1] != "$" {
			panic("Default functions must start with a '$' symbol")
		}

		if k[1:2] == "_" {
			panic("Default function names must not begin with an underscore")
		}
	}

	fieldDefaultFunctions = functions
}
