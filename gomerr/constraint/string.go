package constraint

import (
	b64 "encoding/base64"
	"regexp"
	"strings"
)

type stringPtr *string

func StartsWith(prefix string) *constraint {
	return (&constraint{test: func(value interface{}) bool {
		switch vt := value.(type) {
		case string:
			return strings.HasPrefix(vt, prefix)
		case stringPtr:
			return vt != nil && strings.HasPrefix(*vt, prefix)
		default:
			return false
		}
	}}).setDetails("Prefix", prefix, TagStructName, "startswith")
}

func EndsWith(suffix string) *constraint {
	return (&constraint{test: func(value interface{}) bool {
		switch vt := value.(type) {
		case string:
			return strings.HasSuffix(vt, suffix)
		case stringPtr:
			return vt != nil && strings.HasSuffix(*vt, suffix)
		default:
			return false
		}
	}}).setDetails("Suffix", suffix, TagStructName, "endswith")
}

func Regexp(r string) *constraint {
	return RegexpMatch(regexp.MustCompile(r))
}

func RegexpMatch(regexp *regexp.Regexp) *constraint {
	return (&constraint{test: func(value interface{}) bool {
		switch vt := value.(type) {
		case string:
			return regexp.MatchString(vt)
		case stringPtr:
			return vt != nil && regexp.MatchString(*vt)
		default:
			return false
		}
	}}).setDetails("Regexp", regexp.String(), TagStructName, "regexp")
}

var Base64 = base64()

func base64() *constraint {
	return (&constraint{test: func(value interface{}) bool {
		var err error
		switch vt := value.(type) {
		case string:
			_, err = b64.RawURLEncoding.DecodeString(vt)
		case stringPtr:
			_, err = b64.RawURLEncoding.DecodeString(*vt)
		default:
			return false
		}

		return err != nil
	}}).setDetails("Base64Encoded", true, TagStructName, "base64")
}
