package constraint

import (
	"errors"
	"reflect"
	"testing"

	"github.com/jt0/gomer/gomerr"
)

func mustValidator(t *testing.T, node Node, typ reflect.Type) *Validator {
	t.Helper()
	vr, ge := node.Validator(typ)
	if ge != nil {
		t.Fatalf("compile %q against %v: %v", node.String(), typ, ge)
	}
	return vr
}

func assertPass(t *testing.T, vr *Validator, v any) {
	t.Helper()
	if ge := vr.Validate(v); ge != nil {
		t.Errorf("Validate(%#v): want pass, got %v", v, ge)
	}
}

func assertNotSatisfied(t *testing.T, vr *Validator, v any) {
	t.Helper()
	_ = asNotSatisfied(t, vr, v)
}

func asNotSatisfied(t *testing.T, vr *Validator, v any) *NotSatisfiedError {
	t.Helper()
	ge := vr.Validate(v)
	nse, ok := errors.AsType[*NotSatisfiedError](ge)
	if !ok {
		t.Fatalf("Validate(%#v): want NotSatisfiedError, got %v", v, ge)
	}
	return nse
}

func assertZeroAlloc(t *testing.T, f func()) {
	t.Helper()
	if n := testing.AllocsPerRun(200, f); n != 0 {
		t.Errorf("want zero allocations on the success path, got %v", n)
	}
}

var (
	intType     = reflect.TypeFor[int]()
	int64Type   = reflect.TypeFor[int64]()
	stringType  = reflect.TypeFor[string]()
	boolType    = reflect.TypeFor[bool]()
	stringPtr   = reflect.TypeFor[*string]()
	stringSlice = reflect.TypeFor[[]string]()
	// anyInterfaceType compiles a Node against the dynamic-dispatch path, where kind
	// mismatches surface at evaluation rather than at compile time.
	anyInterfaceType = reflect.TypeFor[any]()
)

// TestFailureBudget checks that the budget caps the number of reported failures.
func TestFailureBudget(t *testing.T) {
	node := Node{kind: kindElements, children: []Node{{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}}}
	values := make([]int, failureBudget+10)
	for i := range values {
		values[i] = -1
	}

	vr := mustValidator(t, node, reflect.TypeFor[[]int]())
	if be, ok := errors.AsType[*gomerr.BatchError](vr.Validate(values)); !ok {
		t.Errorf("want a BatchError aggregating the problems, got %v", be)
	} else if len(be.Errors()) != failureBudget {
		t.Errorf("want %d failures, got %v", failureBudget, be)
	}
}

func TestValidatorTypeGuard(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindNotZero}, int64Type)
	ge := vr.Validate("a string")
	if _, ok := errors.AsType[*gomerr.UnprocessableError](ge); !ok {
		t.Errorf("want an Unprocessable for a mismatched type, got %v", ge)
	}
}

func TestValidatorValidateAtTargetsFailures(t *testing.T) {
	vr, ge := (&Node{kind: kindLength, name: "len", params: []operand{{Value: uint64(1)}}}).Validator(reflect.TypeFor[string]())
	if ge != nil {
		t.Fatal(ge)
	}
	if nse, ok := errors.AsType[*NotSatisfiedError](vr.ValidateAt("name", "")); !ok {
		t.Fatalf("want a NotSatisfiedError, got %v", ge)
	} else if nse.Target != "name" {
		t.Fatalf("want a failure targeted at 'name', got %s", nse.Target)
	}
	if ge = vr.ValidateAt("name", "ok"); ge != nil {
		t.Fatal(ge)
	}
}

// Benchmarks

var benchSink any

// BenchmarkNodeValidator measures the one-time cost a first use pays to compile a
// representative multi-constraint field node against its type.
func BenchmarkNodeValidator(b *testing.B) {
	node := Node{kind: kindAnd, children: []Node{
		{kind: kindLength, name: "len", params: []operand{{Value: "1"}, {Value: "64"}}},
		{kind: kindPattern, name: "regexp", params: []operand{{Value: "^[a-z][a-z0-9-]*$"}}},
	}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		vr, ge := node.Validator(stringType)
		if ge != nil {
			b.Fatal(ge)
		}
		benchSink = vr
	}
}

func BenchmarkOrNilPresent(b *testing.B) {
	node := Node{kind: kindOr, children: []Node{
		{kind: kindNil},
		{kind: kindLength, params: []operand{{Value: "1"}, {Value: "16"}}},
	}}
	vr, _ := node.Validator(stringPtr)
	present := new("hello")
	var pv any = present
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = vr.Validate(pv)
	}
}

func BenchmarkOrNilAbsent(b *testing.B) {
	node := Node{kind: kindOr, children: []Node{
		{kind: kindNil},
		{kind: kindLength, params: []operand{{Value: "1"}, {Value: "16"}}},
	}}
	vr, _ := node.Validator(stringPtr)
	var absent any = (*string)(nil)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = vr.Validate(absent)
	}
}

func BenchmarkElements10(b *testing.B)   { benchElements(b, 10) }
func BenchmarkElements1000(b *testing.B) { benchElements(b, 1000) }

func benchElements(b *testing.B, n int) {
	node := Node{kind: kindElements, children: []Node{{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}}}
	vr, _ := node.Validator(reflect.TypeFor[[]int]())
	data := make([]int, n)
	var v any = data
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = vr.Validate(v)
	}
}

func BenchmarkCompareInt(b *testing.B) {
	vr, _ := (&Node{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}).Validator(int64Type)
	var v any = int64(5)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = vr.Validate(v)
	}
}

func BenchmarkLengthString(b *testing.B) {
	vr, _ := (&Node{kind: kindLength, params: []operand{{Value: "1"}, {Value: "16"}}}).Validator(stringType)
	var v any = "hello"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = vr.Validate(v)
	}
}

func BenchmarkLengthCodePoints(b *testing.B) {
	vr, _ := (&Node{kind: kindLength, unit: UnitCodePoints, params: []operand{{Value: "1"}, {Value: "16"}}}).Validator(stringType)
	var v any = "héllo"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = vr.Validate(v)
	}
}

func BenchmarkPattern(b *testing.B) {
	vr, _ := (&Node{kind: kindPattern, params: []operand{{Value: "^[a-z]+$"}}}).Validator(stringType)
	var v any = "hello"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = vr.Validate(v)
	}
}

func BenchmarkOneOfString(b *testing.B) {
	vr, _ := (&Node{kind: kindOneOf, params: []operand{{Value: "red"}, {Value: "green"}, {Value: "blue"}}}).Validator(stringType)
	var v any = "green"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchSink = vr.Validate(v)
	}
}
