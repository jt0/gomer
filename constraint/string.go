package constraint

import (
	"regexp"
	"strings"

	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
)

func StartsWith(prefix *string) Constraint {
	return stringTest("startsWith", prefix, func(s string) bool {
		return prefix == nil || strings.HasPrefix(s, *prefix)
	})
}

func EndsWith(suffix *string) Constraint {
	return stringTest("endsWith", suffix, func(s string) bool {
		return suffix == nil || strings.HasSuffix(s, *suffix)
	})
}

func Regexp(r string) Constraint {
	re, err := regexp.Compile(r)
	if err != nil {
		panic(gomerr.Configuration("'" + r + "' is not a valid regexp pattern: " + err.Error()))
	}
	return stringTest("regexp", r, func(s string) bool {
		return re.MatchString(s)
	})
}

func RegexpMatch(re *regexp.Regexp) Constraint {
	if re == nil {
		return ConfigurationError("regexp is nil")
	}

	return stringTest("regexp", re.String(), func(s string) bool {
		return re.MatchString(s)
	})
}

var IsRegexp = stringTest("isRegexp", nil, func(s string) bool {
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
			return NotSatisfied(s)
		}
		return nil
	})
}
