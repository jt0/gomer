package constraint

import (
	b64 "encoding/base64"
	"regexp"
	"strings"
)

type stringPtr *string

func StartsWith(prefix string) Constraint {
	return New("StartsWith", prefix, func(toTest interface{}) bool {
		switch tt := toTest.(type) {
		case string:
			return strings.HasPrefix(tt, prefix)
		case stringPtr:
			return tt != nil && strings.HasPrefix(*tt, prefix)
		default:
			return false
		}
	})
}

func EndsWith(suffix string) Constraint {
	return New("EndsWith", suffix, func(toTest interface{}) bool {
		switch tt := toTest.(type) {
		case string:
			return strings.HasSuffix(tt, suffix)
		case stringPtr:
			return tt != nil && strings.HasSuffix(*tt, suffix)
		default:
			return false
		}
	})
}

func Regexp(r string) Constraint {
	return RegexpMatch(regexp.MustCompile(r))
}

func RegexpMatch(regexp *regexp.Regexp) Constraint {
	return New("Regexp", regexp.String(), func(toTest interface{}) bool {
		switch tt := toTest.(type) {
		case string:
			return regexp.MatchString(tt)
		case stringPtr:
			return tt != nil && regexp.MatchString(*tt)
		default:
			return false
		}
	})
}

var Base64 = base64()

func base64() Constraint {
	return New("Base64Encoded", true, func(toTest interface{}) bool {
		var err error
		switch tt := toTest.(type) {
		case string:
			_, err = b64.RawURLEncoding.DecodeString(tt)
		case stringPtr:
			_, err = b64.RawURLEncoding.DecodeString(*tt)
		default:
			return false
		}

		return err != nil
	})
}
