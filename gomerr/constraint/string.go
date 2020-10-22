package constraint

import (
	b64 "encoding/base64"
	"regexp"
	"strings"
)

type stringPtr *string

func StartsWith(prefix string) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		switch vt := value.(type) {
		case string:
			return strings.HasPrefix(vt, prefix)
		case stringPtr:
			return vt != nil && strings.HasPrefix(*vt, prefix)
		default:
			return false
		}
	}}.setDetails("Prefix", prefix, LookupName, "startswith")
}

func EndsWith(suffix string) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		switch vt := value.(type) {
		case string:
			return strings.HasSuffix(vt, suffix)
		case stringPtr:
			return vt != nil && strings.HasSuffix(*vt, suffix)
		default:
			return false
		}
	}}.setDetails("Suffix", suffix, LookupName, "endswith")
}

func RegexpMust(r string) Constrainer {
	return Regexp(regexp.MustCompile(r))
}

func Regexp(regexp *regexp.Regexp) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		switch vt := value.(type) {
		case string:
			return regexp.MatchString(vt)
		case stringPtr:
			return vt != nil && regexp.MatchString(*vt)
		default:
			return false
		}
	}}.setDetails("Regexp", regexp.String(), LookupName, "regexp")
}

var Base64 = base64()

func base64() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
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
	}}.setDetails("Base64Encoded", true, LookupName, "base64")
}
