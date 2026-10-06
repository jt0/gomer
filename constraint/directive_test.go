package constraint

import (
	"reflect"
	"testing"
)

func mustParse(t *testing.T, tag string) Node {
	t.Helper()
	node, ge := parseDirective(tag)
	if ge != nil {
		t.Fatalf("parseDirective(%q): %v", tag, ge)
	}
	return node
}

func TestParseDirective(t *testing.T) {
	cases := []struct {
		tag  string
		want Node
	}{
		{"required", Node{kind: kindPresent, name: "required"}},
		{"gte(0)", Node{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}},
		{"len(1,64)", Node{kind: kindLength, name: "len", params: []operand{{Value: "1"}, {Value: "64"}}}},
		{"lte($.Max)", Node{kind: kindCompare, name: "lte", params: []operand{{Ref: "$.Max"}}}},
		{
			"len(1,2),required",
			Node{kind: kindAnd, name: "and", children: []Node{
				{kind: kindLength, name: "len", params: []operand{{Value: "1"}, {Value: "2"}}},
				{kind: kindPresent, name: "required"},
			}},
		},
		{
			"or(zero,gte(10))",
			Node{kind: kindOr, name: "or", children: []Node{
				{kind: kindZero, name: "zero"},
				{kind: kindCompare, name: "gte", params: []operand{{Value: "10"}}},
			}},
		},
		{
			"elements(len(1,16))",
			Node{kind: kindElements, name: "elements", children: []Node{
				{kind: kindLength, name: "len", params: []operand{{Value: "1"}, {Value: "16"}}},
			}},
		},
	}
	for _, c := range cases {
		got := mustParse(t, c.tag)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("parseDirective(%q) = %+v, want %+v", c.tag, got, c.want)
		}
	}
}

// TestParsePrintRoundTrip checks that a parsed Node prints to tag text that parses back
// to the same Node.
func TestParsePrintRoundTrip(t *testing.T) {
	tags := []string{
		"required",
		"notzero",
		"gte(0)",
		"between(1,10)",
		"len(1,64)",
		"len(2,)",
		"regexp(^[a-z]+$)",
		"oneof(red,green,blue)",
		"len(1,2),required",
		"or(zero,gte(10))",
		"not(zero)",
		"elements(len(1,16))",
		"or(and(gte(1),lte(2)),required)",
		"lte($.Max)",
	}
	for _, tag := range tags {
		first := mustParse(t, tag)
		printed := first.String()
		second := mustParse(t, printed)
		if !reflect.DeepEqual(first, second) {
			t.Errorf("round trip of %q via %q diverged:\n first=%+v\nsecond=%+v", tag, printed, first, second)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, tag := range []string{
		"",
		"nope",
		"or(",
		"and()",
	} {
		if _, ge := parseDirective(tag); ge == nil {
			t.Errorf("parseDirective(%q): want an error", tag)
		}
	}
}

// TestFieldRefIsCompileBoundary checks that a $.Field operand parses fine but cannot be
// compiled against a standalone value, since there is no enclosing struct to read the
// field from.
func TestFieldRefIsCompileBoundary(t *testing.T) {
	node := mustParse(t, "lte($.Max)")
	if !node.params[0].isRef() {
		t.Fatal("want the operand kept as a field reference")
	}
	if _, ge := node.Validator(intType); ge == nil {
		t.Error("want a compile error: a field reference needs whole-struct compilation")
	}
}
