package fields

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/jt0/gomer/gomerr"
)

type field struct {
	name                 string
	externalName         string
	location             string
	accessBits           fieldAccessBits
	provided             bool
	defaultValue         string
	defaultValueFunction DefaultFunction
	bypassDefaultIfSet   bool
	zeroVal              reflect.Value
}

type fieldAccessBits uint32
type DefaultFunction func(reflect.Value) interface{}

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
	ReadAccess = fieldAccessBits(1 << iota)
	CreateAccess
	UpdateAccess
	numAccessTypes = iota // iota is not reset, so this is the number of access types starting w/ ReadAccess

	ReadChar   = 'r'
	WriteChar  = 'w'
	CreateChar = 'c'
	UpdateChar = 'u'
	DenyChar   = '-'
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
