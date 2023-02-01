package constraint

import (
	"regexp"
	"strings"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

func StartsWith(prefix *string) Constraint {
	return stringTest("StartsWith", prefix, func(s string) bool {
		return prefix == nil || strings.HasPrefix(s, *prefix)
	})
}

func EndsWith(suffix *string) Constraint {
	return stringTest("EndsWith", suffix, func(s string) bool {
		return suffix == nil || strings.HasSuffix(s, *suffix)
	})
}

func Regexp(r string) Constraint {
	return stringTest("Regexp", r, func(s string) bool {
		re, err := regexp.Compile(r)
		if err != nil {
			return false
		}
		return re.MatchString(s)
	})
}

func RegexpMatch(regexp *regexp.Regexp) Constraint {
	if regexp == nil {
		return ConfigurationError("regexp is nil")
	}

	return stringTest("Regexp", regexp.String(), func(s string) bool {
		return regexp.MatchString(s)
	})
}

var IsRegexp = stringTest("IsRegexp", nil, func(s string) bool {
	_, err := regexp.Compile(s)
	return err == nil
})

func stringTest(name string, parameters interface{}, test func(s string) bool) Constraint {
	return New(name, parameters, func(toTest interface{}) gomerr.Gomerr {
		if tt, ok := flect.IndirectInterface(toTest); !ok {
			return NotSatisfied(toTest)
		} else if s, ok := tt.(string); !ok {
			return gomerr.Unprocessable(name+" requires a string or *string test value", toTest)
		} else if !test(s) {
			return NotSatisfied(toTest)
		}
		return nil
	})
}
