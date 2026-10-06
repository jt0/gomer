package constraint

import (
	"reflect"
	"testing"
)

func TestNodeString(t *testing.T) {
	cases := []struct {
		node Node
		want string
	}{
		{Node{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}, "gte(0)"},
		{Node{kind: kindBetween, params: []operand{{Value: "1"}, {Value: "10"}}}, "between(1,10)"},
		{Node{kind: kindLength, params: []operand{{Value: "1"}, {Value: "64"}}}, "len(1,64)"},
		{Node{kind: kindLength, params: []operand{{Value: "1"}, {Value: ""}}}, "len(1,)"},
		{Node{kind: kindPattern, params: []operand{{Value: "^a+$"}}}, "regexp(^a+$)"},
		{Node{kind: kindOneOf, params: []operand{{Value: "A"}, {Value: "B"}}}, "oneof(A,B)"},
		{Node{kind: kindPresent}, "required"},
		{Node{kind: kindNotZero}, "notzero"},
		{Node{kind: kindCompare, name: "lte", params: []operand{{Ref: "$.Max"}}}, "lte($.Max)"},
		{
			Node{kind: kindAnd, children: []Node{
				{kind: kindLength, params: []operand{{Value: "1"}, {Value: "2"}}},
				{kind: kindPresent},
			}},
			"len(1,2),required",
		},
		{
			Node{kind: kindOr, children: []Node{
				{kind: kindZero},
				{kind: kindCompare, name: "gte", params: []operand{{Value: "10"}}},
			}},
			"or(zero,gte(10))",
		},
		{Node{kind: kindNot, children: []Node{{kind: kindZero}}}, "not(zero)"},
		{Node{kind: kindElements, children: []Node{{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}}}, "elements(gte(0))"},
		{
			Node{kind: kindOr, children: []Node{
				{kind: kindAnd, children: []Node{
					{kind: kindCompare, name: "gte", params: []operand{{Value: "1"}}},
					{kind: kindCompare, name: "lte", params: []operand{{Value: "2"}}},
				}},
				{kind: kindPresent},
			}},
			"or(and(gte(1),lte(2)),required)",
		},
	}
	for _, c := range cases {
		node := c.node
		if got := node.String(); got != c.want {
			t.Errorf("String() = %q, want %q", got, c.want)
		}
	}
}

func TestNodeStringZero(t *testing.T) {
	var n Node
	if got := n.String(); got != "" {
		t.Errorf("zero Node String() = %q, want empty", got)
	}
}

func TestJsonSchema(t *testing.T) {
	cases := []struct {
		name string
		node Node
		want map[string]any
	}{
		{
			"gte",
			Node{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}},
			map[string]any{"type": "number", "minimum": int64(0)},
		},
		{
			"gt exclusive",
			Node{kind: kindCompare, name: "gt", params: []operand{{Value: "0"}}},
			map[string]any{"type": "number", "exclusiveMinimum": int64(0)},
		},
		{
			"lte float bound",
			Node{kind: kindCompare, name: "lte", params: []operand{{Value: "1.5"}}},
			map[string]any{"type": "number", "maximum": 1.5},
		},
		{
			"between",
			Node{kind: kindBetween, name: "between", params: []operand{{Value: "1"}, {Value: "10"}}},
			map[string]any{"type": "number", "minimum": int64(1), "maximum": int64(10)},
		},
		{
			"len string",
			Node{kind: kindLength, name: "len", params: []operand{{Value: "1"}, {Value: "64"}}},
			map[string]any{"type": "string", "minLength": int64(1), "maxLength": int64(64)},
		},
		{
			"len minimum only",
			Node{kind: kindLength, name: "len", params: []operand{{Value: "1"}}},
			map[string]any{"type": "string", "minLength": int64(1)},
		},
		{
			"len maximum only",
			Node{kind: kindLength, name: "len", params: []operand{{Value: ""}, {Value: "3"}}},
			map[string]any{"type": "string", "maxLength": int64(3)},
		},
		{
			"len elements",
			Node{kind: kindLength, name: "len", unit: UnitElements, params: []operand{{Value: "1"}, {Value: "5"}}},
			map[string]any{"type": "array", "minItems": int64(1), "maxItems": int64(5)},
		},
		{
			"len decoded bytes",
			Node{kind: kindLength, name: "len", unit: UnitDecodedBytes, params: []operand{{Value: "0"}, {Value: "1024"}}},
			map[string]any{"type": "string", "contentEncoding": "base64", "minLength": int64(0), "maxLength": int64(1024)},
		},
		{
			"regexp",
			Node{kind: kindPattern, name: "regexp", params: []operand{{Value: "^a+$"}}},
			map[string]any{"type": "string", "pattern": "^a+$"},
		},
		{
			"isregexp",
			Node{kind: kindIsRegexp, name: "isregexp"},
			map[string]any{"type": "string", "format": "regex"},
		},
		{
			"oneof strings",
			Node{kind: kindOneOf, name: "oneof", params: []operand{{Value: "A"}, {Value: "B"}}},
			map[string]any{"enum": []any{"A", "B"}},
		},
		{
			"oneof ints",
			Node{kind: kindOneOf, name: "oneof", params: []operand{{Value: "1"}, {Value: "2"}}},
			map[string]any{"enum": []any{int64(1), int64(2)}},
		},
		{
			"equals",
			Node{kind: kindEquals, name: "eq", params: []operand{{Value: "READ"}}},
			map[string]any{"const": "READ"},
		},
		{
			"true",
			Node{kind: kindTrue, name: "true"},
			map[string]any{"type": "boolean", "const": true},
		},
		{
			"fail rejects everything",
			Node{kind: kindFail, name: "fail"},
			map[string]any{"not": map[string]any{}},
		},
		{
			"present has no value-level keyword",
			Node{kind: kindPresent, name: "required"},
			map[string]any{},
		},
		{
			"field reference bound omitted",
			Node{kind: kindCompare, name: "lte", params: []operand{{Ref: "$.Max"}}},
			map[string]any{},
		},
		{
			"and merges to one string schema",
			Node{kind: kindAnd, name: "and", children: []Node{
				{kind: kindPresent, name: "required"},
				{kind: kindLength, name: "len", params: []operand{{Value: "1"}, {Value: "64"}}},
				{kind: kindPattern, name: "regexp", params: []operand{{Value: "^a+$"}}},
			}},
			map[string]any{"type": "string", "minLength": int64(1), "maxLength": int64(64), "pattern": "^a+$"},
		},
		{
			"and with conflicting keyword falls back to allOf",
			Node{kind: kindAnd, name: "and", children: []Node{
				{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}},
				{kind: kindCompare, name: "gte", params: []operand{{Value: "5"}}},
			}},
			map[string]any{"allOf": []map[string]any{
				{"type": "number", "minimum": int64(0)},
				{"type": "number", "minimum": int64(5)},
			}},
		},
		{
			"or becomes anyOf",
			Node{kind: kindOr, name: "or", children: []Node{
				{kind: kindZero, name: "zero"},
				{kind: kindPattern, name: "regexp", params: []operand{{Value: "^a+$"}}},
			}},
			map[string]any{"anyOf": []map[string]any{
				{},
				{"type": "string", "pattern": "^a+$"},
			}},
		},
		{
			"not",
			Node{kind: kindNot, name: "not", children: []Node{
				{kind: kindZero, name: "zero"},
			}},
			map[string]any{"not": map[string]any{}},
		},
		{
			"elements",
			Node{kind: kindElements, name: "elements", children: []Node{
				{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}},
			}},
			map[string]any{"type": "array", "items": map[string]any{"type": "number", "minimum": int64(0)}},
		},
		{
			"mapvalues",
			Node{kind: kindMapValues, name: "mapvalues", children: []Node{
				{kind: kindLength, name: "len", params: []operand{{Value: "1"}, {Value: "8"}}},
			}},
			map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string", "minLength": int64(1), "maxLength": int64(8)}},
		},
	}

	for _, c := range cases {
		node := c.node
		got := node.JsonSchema()
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: JsonSchema() = %#v, want %#v", c.name, got, c.want)
		}
	}
}

func TestJsonSchemaZero(t *testing.T) {
	var n Node
	if got := n.JsonSchema(); !reflect.DeepEqual(got, map[string]any{}) {
		t.Errorf("zero Node JsonSchema() = %#v, want empty map", got)
	}
}
