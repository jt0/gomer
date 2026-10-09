package constraint

import (
	"reflect"

	"github.com/jt0/gomer/gomerr"
)

// Required checks that a value is present: not a nil pointer, slice, map or interface.
func Required() Node { return Node{kind: kindPresent, name: "required"} }

// NotZero checks that a value is not its type's zero value.
func NotZero() Node { return Node{kind: kindNotZero, name: "notzero"} }

// Pattern checks that a string matches the regular expression re.
func Pattern(re string) Node {
	return Node{kind: kindPattern, name: "regexp", params: []operand{{Value: re}}}
}

// Length checks that a value's length is between min and max, inclusive.
func Length(min, max uint64) Node {
	return Node{kind: kindLength, name: "len", params: []operand{{Value: min}, {Value: max}}}
}

// MinLength checks that a value's length is at least min.
func MinLength(min uint64) Node {
	return Node{kind: kindLength, name: "len", params: []operand{{Value: min}}}
}

// MaxLength checks that a value's length is at most max.
func MaxLength(max uint64) Node {
	return Node{kind: kindLength, name: "len", params: []operand{{}, {Value: max}}}
}

// Gte, Gt, Lte and Lt compare a value against bound.
func Gte(bound any) Node { return compareNode("gte", bound) }
func Gt(bound any) Node  { return compareNode("gt", bound) }
func Lte(bound any) Node { return compareNode("lte", bound) }
func Lt(bound any) Node  { return compareNode("lt", bound) }

func compareNode(op string, bound any) Node {
	return Node{kind: kindCompare, name: op, params: []operand{{Value: bound}}}
}

// Between checks that a value is between lower and upper, inclusive.
func Between(lower, upper any) Node {
	params := []operand{{Value: lower}, {Value: upper}}
	return Node{kind: kindBetween, name: "between", params: params}
}

// OneOf checks that a value equals one of values.
func OneOf(values ...any) Node {
	params := make([]operand, len(values))
	for i, v := range values {
		params[i] = operand{Value: v}
	}
	return Node{kind: kindOneOf, name: "oneof", params: params}
}

// And checks every node, reporting each that fails.
func And(nodes ...Node) Node { return Node{kind: kindAnd, name: "and", children: nodes} }

// Union checks that exactly one of a struct's fields is set.
func Union() Node { return Node{kind: kindUnion, name: "union"} }

// Fail always fails under name. It labels a failure found outside a Validator, such as a
// uniqueness conflict a data store reports, so a renderer can tell it apart by
// Node.Name().
func Fail(name string) Node { return Node{kind: kindFail, name: name} }

// Custom is a constraint type for logic the built-in kinds can't express.
type Custom interface {
	Name() string

	// Accepts rejects an incompatible field type when the node is compiled.
	Accepts(reflect.Type) gomerr.Gomerr

	// Check runs the test. A Check that panics is reported as a fault.
	Check(v any, cc CustomContext) bool

	// Describe supplies the Expected text a failure reports.
	Describe() string
}

// CustomContext is handed to a Custom at evaluation. If Custom.Check only identifies
// one problem, it returns false and the field gets one failure. If the Custom constraint
// ranges over a collection, for example, it can report each item-specific problem
// through Report.
type CustomContext interface {
	// Enclosing returns the struct that contains the field under test, and reports false
	// when validating a single value.
	Enclosing() (any, bool)

	// Report records a failure of value at target, nested under the field's target. value
	// is nil for a problem no single value shows. Once a check calls Report, its failures
	// are the reported ones, whatever it returns. A report is dropped inside an or-branch
	// or if it exceeds the failure budget.
	Report(target, expected string, value any)
}
