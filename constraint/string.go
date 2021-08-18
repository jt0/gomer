package constraint

import (
	b64 "encoding/base64"
	"regexp"
	"strings"

	"github.com/jt0/gomer/gomerr"
)

func StartsWith(prefix *string) Constraint {
	return New("StartsWith", prefix, func(toTest interface{}) gomerr.Gomerr {
		switch tt := toTest.(type) {
		case string:
			if !strings.HasPrefix(tt, *prefix) {
				return NotSatisfied(toTest)
			}
		case *string:
			if tt == nil || !strings.HasPrefix(*tt, *prefix) {
				return NotSatisfied(toTest)
			}
		default:
			return gomerr.Unprocessable("StartsWith requires a string or *string test value", toTest)
		}

		return nil
	})
}

func EndsWith(suffix *string) Constraint {
	return New("EndsWith", suffix, func(toTest interface{}) gomerr.Gomerr {
		switch tt := toTest.(type) {
		case string:
			if !strings.HasSuffix(tt, *suffix) {
				return NotSatisfied(toTest)
			}
		case *string:
			if tt == nil || !strings.HasSuffix(*tt, *suffix) {
				return NotSatisfied(toTest)
			}
		default:
			return gomerr.Unprocessable("EndsWith requires a string or *string test value", toTest)
		}

		return nil
	})
}

func Regexp(r string) Constraint {
	return RegexpMatch(regexp.MustCompile(r))
}

func RegexpMatch(regexp *regexp.Regexp) Constraint {
	return New("Regexp", regexp.String(), func(toTest interface{}) gomerr.Gomerr {
		switch tt := toTest.(type) {
		case string:
			if !regexp.MatchString(tt) {
				return NotSatisfied(toTest)
			}
		case *string:
			if tt == nil || !regexp.MatchString(*tt) {
				return NotSatisfied(toTest)
			}
		default:
			return gomerr.Unprocessable("Regexp requires a string or *string test value", toTest)
		}

		return nil
	})
}

var IsRegexp = New("IsRegexp", nil, func(toTest interface{}) gomerr.Gomerr {
	var err error
	switch tt := toTest.(type) {
	case string:
		_, err = regexp.Compile(tt)
	case *string:
		if tt != nil {
			_, err = regexp.Compile(*tt)
		}
	default:
		return gomerr.Unprocessable("IsRegexp requires a string or *string test value", toTest)
	}

	if err != nil {
		return NotSatisfied(toTest).Wrap(err)
	}

	return nil
})

var Base64 = New("Base64Encoded", nil, func(toTest interface{}) gomerr.Gomerr {
	var err error
	switch tt := toTest.(type) {
	case string:
		_, err = b64.RawURLEncoding.DecodeString(tt)
	case *string:
		if tt != nil {
			_, err = b64.RawURLEncoding.DecodeString(*tt)
		}
	default:
		return gomerr.Unprocessable("Base64Encoded requires a string or *string test value", toTest)
	}

	if err != nil {
		return NotSatisfied(toTest).Wrap(err)
	}

	return nil
})
