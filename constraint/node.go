package constraint

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jt0/gomer/gomerr"
)

// kind identifies what a node checks. The compiler switches on it to choose how to
// specialize a node for a type; the printer and the parser map it to and from a tag
// token.
type kind uint8

const (
	kindInvalid kind = iota

	// Leaf kinds test a single value.
	kindCompare // ordered comparison; Name is the operator: gt, gte, lt, lte
	kindBetween // inclusive range; params are the lower and upper bounds
	kindLength  // length within bounds; params are min and max, unit how it counts
	kindPattern // value matches a regular expression; params[0] is the pattern
	kindOneOf   // value equals one of params
	kindPresent // value is present (non-nil); the meaning of "required"
	kindNotZero // value is not the zero value for its type
	kindNil
	kindNotNil
	kindZero
	kindTrue
	kindFalse
	kindIsRegexp  // string is itself a compilable regular expression
	kindEquals    // value equals params[0]
	kindNotEquals // value does not equal params[0]
	kindFail      // always fails

	// Logic kinds compose children.
	kindAnd
	kindOr
	kindNot
	kindWhen // run the child constraint only when a $.Field condition holds

	// Container kinds descend into a value's elements, keys or values.
	kindElements
	kindMapKeys
	kindMapValues
	kindStruct // validate a nested struct with its own tags
	kindUnion  // exactly one member is set

	// Custom is a check supplied in Go.
	kindCustom
)

// String returns the kind's name for diagnostics. It is not the tag token, which depends
// on the node's Name as well (see Node.token).
func (k kind) String() string {
	switch k {
	case kindCompare:
		return "Compare"
	case kindBetween:
		return "Between"
	case kindLength:
		return "Length"
	case kindPattern:
		return "Pattern"
	case kindOneOf:
		return "OneOf"
	case kindPresent:
		return "Present"
	case kindNotZero:
		return "NotZero"
	case kindNil:
		return "Nil"
	case kindNotNil:
		return "NotNil"
	case kindZero:
		return "Zero"
	case kindTrue:
		return "True"
	case kindFalse:
		return "False"
	case kindIsRegexp:
		return "IsRegexp"
	case kindEquals:
		return "Equals"
	case kindNotEquals:
		return "NotEquals"
	case kindFail:
		return "Fail"
	case kindAnd:
		return "And"
	case kindOr:
		return "Or"
	case kindNot:
		return "Not"
	case kindWhen:
		return "When"
	case kindElements:
		return "Elements"
	case kindMapKeys:
		return "MapKeys"
	case kindMapValues:
		return "MapValues"
	case kindStruct:
		return "Struct"
	case kindUnion:
		return "Union"
	case kindCustom:
		return "Custom"
	default:
		return "Invalid"
	}
}

func (k kind) token() string {
	switch k {
	case kindBetween:
		return "between"
	case kindLength:
		return "len"
	case kindPattern:
		return "regexp"
	case kindOneOf:
		return "oneof"
	case kindPresent:
		return "required"
	case kindNotZero:
		return "notzero"
	case kindNil:
		return "nil"
	case kindNotNil:
		return "notnil"
	case kindZero:
		return "zero"
	case kindTrue:
		return "true"
	case kindFalse:
		return "false"
	case kindIsRegexp:
		return "isregexp"
	case kindEquals:
		return "eq"
	case kindNotEquals:
		return "neq"
	case kindFail:
		return "fail"
	case kindAnd:
		return "and"
	case kindOr:
		return "or"
	case kindNot:
		return "not"
	case kindWhen:
		return "when"
	case kindElements:
		return "elements"
	case kindMapKeys:
		return "mapkeys"
	case kindMapValues:
		return "mapvalues"
	case kindStruct:
		return "struct"
	case kindUnion:
		return "union"
	default:
		return "?"
	}
}

// A Node is a tree of plain data describing what to check. It has no knowledge of the
// type it will check; its Validator method compiles it for one.
type Node struct {
	kind     kind
	name     string     // reported in failures and printed, such as "gte" or "len"
	params   []operand  // static values or field references, in the kind's order
	children []Node     // and/or/not operands, or a container's element node
	unit     LengthUnit // kindLength only: how a length is counted
	custom   Custom     // kindCustom only
}

// Validator compiles the node for type t. Every configuration error in the node is
// reported together, each naming its position in the node. A nil t compiles to dynamic
// checks, as an interface type does.
func (n Node) Validator(t reflect.Type) (*Validator, gomerr.Gomerr) {
	cc := &compileCtx{}
	root := cc.compileNode(&n, t)
	if ge := cc.errors.GomerrOrNil(); ge != nil {
		return nil, ge
	}

	return &Validator{root: root, typ: t}, nil
}

// Name is the name a failure reports, such as "gte", "len" or a registered custom
// check's name. The zero node's name is the empty string.
func (n Node) Name() string { return n.name }

// String prints the node as canonical tag text, which parseDirective reads back to the
// same node. The zero node prints as the empty string, so a NotSatisfiedError without
// a node still serializes.
func (n Node) String() string {
	if n.kind == kindInvalid {
		return ""
	}
	var b strings.Builder
	n.write(&b, true)
	return b.String()
}

// CountedIn returns a copy of a length node that counts length in unit. It has no effect
// on other nodes.
func (n Node) CountedIn(unit LengthUnit) Node {
	if n.kind == kindLength {
		n.unit = unit
	}
	return n
}

// JsonSchema renders the node as a JSON Schema fragment: the keywords that constrain a
// single value, as a map ready to marshal.
//
// A node has no Go type, so a keyword is emitted only where the node's kind fixes the
// JSON type (a pattern implies a string, a range a number, a container an array or
// object) or a LengthUnit says how a length is counted. Constraints that depend on the
// enclosing object ($.Field bounds, when conditions, presence) or on Go (custom checks)
// have no value-level keyword and are left out.
//
// The zero node renders as an empty schema.
func (n Node) JsonSchema() map[string]any {
	if n.kind == kindInvalid {
		return map[string]any{}
	}
	return n.jsonSchema()
}

// write appends the node's tag text to b. A top-level and prints as comma-joined terms,
// the usual tag form; a nested and prints as and(...) so its terms stay grouped.
func (n Node) write(b *strings.Builder, top bool) {
	switch n.kind {
	case kindAnd:
		if !top {
			b.WriteString("and(")
		}
		writeChildren(b, n.children)
		if !top {
			b.WriteByte(')')
		}
	case kindOr:
		b.WriteString("or(")
		writeChildren(b, n.children)
		b.WriteByte(')')
	case kindNot:
		b.WriteString("not(")
		if len(n.children) > 0 {
			n.children[0].write(b, false)
		}
		b.WriteByte(')')
	case kindWhen:
		b.WriteString("when(")
		for i := 0; i < 3 && i < len(n.params); i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(operandString(n.params[i]))
		}
		if len(n.children) > 0 {
			b.WriteByte(',')
			n.children[0].write(b, false)
		}
		b.WriteByte(')')
	case kindElements, kindMapKeys, kindMapValues:
		b.WriteString(n.kind.token())
		b.WriteByte('(')
		if len(n.children) > 0 {
			n.children[0].write(b, false)
		}
		b.WriteByte(')')
	case kindStruct, kindUnion:
		b.WriteString(n.kind.token())
	case kindCustom:
		b.WriteString(n.name)
	default:
		b.WriteString(n.token())
		if len(n.params) == 0 {
			return
		}
		b.WriteByte('(')
		for i, op := range n.params {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(operandString(op))
		}
		b.WriteByte(')')
	}
}

func writeChildren(b *strings.Builder, children []Node) {
	for i := range children {
		if i > 0 {
			b.WriteByte(',')
		}
		children[i].write(b, false)
	}
}

// token is the node's tag token: the operator for a comparison, the registered name for a
// custom check, and otherwise the kind's token.
func (n Node) token() string {
	switch n.kind {
	case kindCompare, kindCustom:
		return n.name
	default:
		return n.kind.token()
	}
}

// operand is one argument to a leaf: a static value, or a reference to another field of
// the enclosing struct when Ref is set. Static values from a tag arrive as strings and
// are converted to the field's type when the node is compiled.
type operand struct {
	Value any
	Ref   string // "$.Field" when the operand reads another field's value
}

// isRef reports whether the operand reads another field rather than carrying a value.
func (o operand) isRef() bool { return o.Ref != "" }

// LengthUnit selects how a length is counted. UnitDefault counts UTF-8 bytes for a
// string, as Go's len does, and elements for an array, slice or map.
type LengthUnit uint8

const (
	UnitDefault LengthUnit = iota
	UnitCodePoints
	UnitElements
	UnitDecodedBytes // base64-encoded string: count the decoded bytes
)

func (n Node) jsonSchema() map[string]any {
	switch n.kind {
	case kindAnd:
		// The children's keywords merge into one schema, so len and regexp read as one
		// string schema. Children that set a keyword differently fall back to allOf.
		merged := map[string]any{}
		for i := range n.children {
			for k, v := range n.children[i].jsonSchema() {
				if existing, ok := merged[k]; ok && !reflect.DeepEqual(existing, v) {
					return map[string]any{"allOf": n.childSchemas()}
				}
				merged[k] = v
			}
		}
		return merged
	case kindOr:
		return map[string]any{"anyOf": n.childSchemas()}
	case kindNot:
		if len(n.children) == 0 {
			return map[string]any{}
		}
		return map[string]any{"not": n.children[0].jsonSchema()}
	case kindElements:
		return map[string]any{"type": "array", "items": n.childSchemas()[0]}
	case kindMapValues:
		return map[string]any{"type": "object", "additionalProperties": n.childSchemas()[0]}
	case kindMapKeys:
		return map[string]any{"type": "object", "propertyNames": n.childSchemas()[0]}
	case kindLength:
		return n.lengthSchema()
	case kindCompare:
		return n.compareSchema()
	case kindBetween:
		return n.betweenSchema()
	case kindPattern:
		if len(n.params) == 0 || n.params[0].isRef() {
			return map[string]any{}
		}
		return map[string]any{"type": "string", "pattern": operandString(n.params[0])}
	case kindIsRegexp:
		return map[string]any{"type": "string", "format": "regex"}
	case kindOneOf:
		values := make([]any, 0, len(n.params))
		for i := range n.params {
			if v, ok := jsonValueOf(n.params[i]); ok {
				values = append(values, v)
			}
		}
		if len(values) == 0 {
			return map[string]any{}
		}
		return map[string]any{"enum": values}
	case kindEquals:
		if v, ok := jsonValueOf(n.firstParam()); ok {
			return map[string]any{"const": v}
		}
		return map[string]any{}
	case kindNotEquals:
		if v, ok := jsonValueOf(n.firstParam()); ok {
			return map[string]any{"not": map[string]any{"const": v}}
		}
		return map[string]any{}
	case kindTrue:
		return map[string]any{"type": "boolean", "const": true}
	case kindFalse:
		return map[string]any{"type": "boolean", "const": false}
	case kindFail:
		return map[string]any{"not": map[string]any{}}
	default:
		// kindPresent, kindNotNil, kindNil, kindZero, kindNotZero, kindWhen, kindStruct,
		// kindUnion and kindCustom have no value-level keyword.
		return map[string]any{}
	}
}

func (n Node) childSchemas() []map[string]any {
	out := make([]map[string]any, 0, len(n.children))
	for _, child := range n.children {
		out = append(out, child.jsonSchema())
	}
	return out
}

func (n Node) firstParam() operand {
	if len(n.params) == 0 {
		return operand{}
	}
	return n.params[0]
}

// lengthSchema maps a length to the keywords for its unit: minItems and maxItems for an
// element count, and minLength and maxLength otherwise. A decoded-byte count also marks
// the string as base64. A missing or empty operand leaves that bound open, as the
// compiler reads it.
func (n Node) lengthSchema() map[string]any {
	schema := map[string]any{}
	minKey, maxKey := "minLength", "maxLength"
	switch n.unit {
	case UnitElements:
		schema["type"] = "array"
		minKey, maxKey = "minItems", "maxItems"
	case UnitDecodedBytes:
		schema["type"] = "string"
		schema["contentEncoding"] = "base64"
	default:
		schema["type"] = "string"
	}
	for i, key := range []string{minKey, maxKey} {
		if i >= len(n.params) {
			break
		}
		if v, ok := jsonNumberOf(n.params[i]); ok {
			if bound, iOk := v.(int64); iOk && bound >= 0 {
				schema[key] = bound
			}
		}
	}
	return schema
}

func (n Node) compareSchema() map[string]any {
	if len(n.params) == 0 {
		return map[string]any{}
	}
	num, ok := jsonNumberOf(n.params[0])
	if !ok {
		return map[string]any{}
	}
	schema := map[string]any{"type": "number"}
	switch n.name {
	case "gte":
		schema["minimum"] = num
	case "gt":
		schema["exclusiveMinimum"] = num
	case "lte":
		schema["maximum"] = num
	case "lt":
		schema["exclusiveMaximum"] = num
	default:
		return map[string]any{}
	}
	return schema
}

func (n Node) betweenSchema() map[string]any {
	if len(n.params) < 2 {
		return map[string]any{}
	}
	schema := map[string]any{}
	if num, ok := jsonNumberOf(n.params[0]); ok {
		schema["minimum"] = num
	}
	if num, ok := jsonNumberOf(n.params[1]); ok {
		schema["maximum"] = num
	}
	if len(schema) == 0 {
		return schema
	}
	schema["type"] = "number"
	return schema
}

// jsonValueOf reads an operand as the JSON value it represents: a bool, an integer, a
// float or a string. A field reference has no static value, so ok is false.
func jsonValueOf(op operand) (any, bool) {
	if op.isRef() {
		return nil, false
	}
	switch v := op.Value.(type) {
	case nil:
		return nil, false
	case string:
		switch v {
		case "true":
			return true, true
		case "false":
			return false, true
		}
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
		return v, true
	case bool:
		return v, true
	case int:
		return int64(v), true
	case int64, uint64, float64:
		return v, true
	default:
		return operandString(op), true
	}
}

// jsonNumberOf reads an operand as a JSON number. ok is false for a reference, a
// non-numeric string or a bool, so a numeric keyword is emitted only for a numeric bound.
func jsonNumberOf(op operand) (any, bool) {
	v, ok := jsonValueOf(op)
	if !ok {
		return nil, false
	}
	switch v.(type) {
	case int64, uint64, float64:
		return v, true
	}
	return nil, false
}

func operandString(op operand) string {
	if op.isRef() {
		return op.Ref
	}
	switch v := op.Value.(type) {
	case nil:
		return ""
	case string:
		return v
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", v)
	}
}
