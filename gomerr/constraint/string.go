package constraint

import (
	b64 "encoding/base64"
	"regexp"
	"strings"
)

func StartsWith(prefix string) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		s, ok := value.(string)
		if !ok {
			return false
		}

		return strings.HasPrefix(s, prefix)
	}}.setDetails("Prefix", prefix)
}

func EndsWith(suffix string) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		s, ok := value.(string)
		if !ok {
			return false
		}

		return strings.HasSuffix(s, suffix)
	}}.setDetails("Suffix", suffix)
}

func RegexpMust(r string) Constrainer {
	return Regexp(regexp.MustCompile(r))
}

func Regexp(regexp *regexp.Regexp) Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		s, ok := value.(string)
		if !ok {
			return false
		}

		return regexp.MatchString(s)
	}}.setDetails("Regexp", regexp.String())
}

var Base64 = base64()

func base64() Constrainer {
	return Constrainer{test: func(value interface{}) bool {
		s, ok := value.(string)
		if !ok {
			return false
		}

		_, err := b64.RawURLEncoding.DecodeString(s)

		return err != nil
	}}.setDetails("Base64Encoded", true)
}
