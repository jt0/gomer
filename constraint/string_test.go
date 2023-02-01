package constraint_test

import (
	"testing"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/constraint"
)

func TestStartsWithNilSucceeds(t *testing.T) {
	c := constraint.StartsWith(nil)
	ge := c.Validate("field", "abc")
	assert.Success(t, ge)
}

func TestStartsWith(t *testing.T) {
	tests := []struct {
		name          string
		prefix        string
		toTest        string
		shouldSucceed bool
	}{
		{"ZeroValueStartsWithZeroValue", "", "", true},
		{"ZeroValueStartsWithValue", "hello", "", false},
		{"CaseMatches", "hello", "hello world", true},
		{"CaseDoesNotMatch", "hello", "Hello world", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := constraint.StartsWith(&tt.prefix)
			ge := c.Validate("field", tt.toTest)
			if tt.shouldSucceed {
				assert.Success(t, ge)
			} else {
				assert.Fail(t, ge)
			}
		})
	}
}

func TestEndsWithNilSucceeds(t *testing.T) {
	c := constraint.EndsWith(nil)
	ge := c.Validate("field", "abc")
	assert.Success(t, ge)
}

func TestEndsWith(t *testing.T) {
	tests := []struct {
		name          string
		suffix        string
		toTest        string
		shouldSucceed bool
	}{
		{"ZeroValueStartsWithZeroValue", "", "", true},
		{"ZeroValueStartsWithValue", "world", "", false},
		{"CaseMatches", "world", "hello world", true},
		{"CaseDoesNotMatch", "world", "Hello World", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := constraint.EndsWith(&tt.suffix)
			ge := c.Validate("field", tt.toTest)
			if tt.shouldSucceed {
				assert.Success(t, ge)
			} else {
				assert.Fail(t, ge)
			}
		})
	}
}

func TestRegexp(t *testing.T) {
	tests := []struct {
		name          string
		regexp        string
		toTest        string
		shouldSucceed bool
	}{
		{"ZeroValueMatchesZeroValue", "", "", true},
		{"ZeroValueMatchesChar", ".", "", false},
		{"ZeroValueMatches0orMoreChar", ".*", "", true},
		{"ValueMatches0orMoreChar", ".*", "abc", true},
		{"BadPattern", "bad[", "abc", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := constraint.Regexp(tt.regexp)
			ge := c.Validate("field", tt.toTest)
			if tt.shouldSucceed {
				assert.Success(t, ge)
			} else {
				assert.Fail(t, ge)
			}
		})
	}
}
