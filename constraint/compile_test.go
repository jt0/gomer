package constraint

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jt0/gomer/gomerr"
)

// TestValidatorReportsEveryError checks that a Node with several configuration errors
// reports all of them at once. Compiling against the dynamic path isolates one error per
// node (a concrete type would add type errors too), so the count is exact.
func TestValidatorReportsEveryError(t *testing.T) {
	node := Node{kind: kindAnd, children: []Node{
		{kind: kindLength}, // no bound
		{kind: kindBetween, params: []operand{{Value: "1"}}}, // only one bound
		{kind: kindOneOf}, // no values
	}}
	_, ge := node.Validator(anyInterfaceType)
	if ge == nil {
		t.Fatal("want aggregated compile errors, got nil")
	}

	if be, ok := errors.AsType[*gomerr.BatchError](ge); !ok {
		t.Fatalf("want a BatchError aggregating the problems, got %T: %v", ge, ge)
	} else if len(be.Errors()) != 3 {
		t.Errorf("want 3 aggregated errors, got %d: %v", len(be.Errors()), ge)
	}
}

func TestValidatorErrorNamesPath(t *testing.T) {
	node := Node{kind: kindOr, children: []Node{
		{kind: kindPattern, params: []operand{{Value: "("}}},
	}}
	_, ge := node.Validator(stringType)
	if ge == nil {
		t.Fatal("want a compile error")
	}
	path, ok := ge.AttributeLookup("path")
	if !ok || !strings.Contains(path.(string), "regexp") {
		t.Errorf("want a path naming the regexp node, got %v", ge.Attributes())
	}
}

func TestComplexityLimits(t *testing.T) {
	deep := Node{kind: kindNotZero}
	for i := 0; i <= maxDepth; i++ {
		deep = Node{kind: kindNot, children: []Node{deep}}
	}
	if _, ge := deep.Validator(intType); ge == nil {
		t.Error("want a depth-limit error")
	}

	wide := Node{kind: kindAnd}
	for range maxNodes {
		wide.children = append(wide.children, Node{kind: kindNotZero})
	}
	if _, ge := wide.Validator(intType); ge == nil {
		t.Error("want a node-limit error")
	}

	patterns := Node{kind: kindAnd}
	for i := 0; i <= maxPatterns; i++ {
		patterns.children = append(patterns.children, Node{kind: kindPattern, params: []operand{{Value: "a"}}})
	}
	if _, ge := patterns.Validator(stringType); ge == nil {
		t.Error("want a pattern-limit error")
	}
}

// Logic

func TestAnd(t *testing.T) {
	node := Node{kind: kindAnd, children: []Node{
		{kind: kindCompare, name: "gte", params: []operand{{Value: "10"}}},
		{kind: kindCompare, name: "lte", params: []operand{{Value: "20"}}},
	}}
	vr := mustValidator(t, node, intType)
	assertPass(t, vr, 15)
	assertNotSatisfied(t, vr, 25)

	both := Node{kind: kindAnd, children: []Node{
		{kind: kindCompare, name: "gte", params: []operand{{Value: "10"}}},
		{kind: kindEquals, params: []operand{{Value: "99"}}},
	}}
	vrBoth := mustValidator(t, both, intType)
	if be, ok := errors.AsType[*gomerr.BatchError](vrBoth.Validate(5)); !ok {
		t.Fatalf("want a BatchError aggregating the problems, got %T: %v", be, be)
	} else if len(be.Errors()) != 2 {
		t.Errorf("want 2 failures reported, got %v", be)
	}
}

func TestOr(t *testing.T) {
	node := Node{kind: kindOr, children: []Node{
		{kind: kindZero},
		{kind: kindCompare, name: "gte", params: []operand{{Value: "10"}}},
	}}
	vr := mustValidator(t, node, intType)
	assertPass(t, vr, 0)  // zero branch
	assertPass(t, vr, 20) // gte branch
	nse := asNotSatisfied(t, vr, 5)
	if nse.Node.kind != kindCompare {
		t.Errorf("want the guarded branch's failure reported, got %s", nse.Node.kind)
	}
}

// TestOrAlternativesReportTheOr checks that an Or offering two real alternatives reports
// one failure naming the Or node.
func TestOrAlternativesReportTheOr(t *testing.T) {
	node := Node{kind: kindOr, children: []Node{
		{kind: kindCompare, name: "gte", params: []operand{{Value: "100"}}},
		{kind: kindCompare, name: "lte", params: []operand{{Value: "10"}}},
	}}
	vr := mustValidator(t, node, intType)
	if nse := asNotSatisfied(t, vr, 50); nse.Node.kind != kindOr {
		t.Errorf("want the failure reported against the or node, got %s", nse.Node.kind)
	}
}

// TestOrBranchFailureIsFree checks that a failed Or branch records nothing: validating a
// value that takes the second branch reports no error for the first.
func TestOrBranchFailureIsFree(t *testing.T) {
	node := Node{kind: kindOr, children: []Node{
		{kind: kindCompare, name: "gte", params: []operand{{Value: "100"}}},
		{kind: kindCompare, name: "lte", params: []operand{{Value: "10"}}},
	}}
	vr := mustValidator(t, node, intType)
	assertPass(t, vr, 5) // fails the first branch, passes the second, overall pass with no error
}

func TestNot(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindNot, children: []Node{{kind: kindZero}}}, intType)
	assertPass(t, vr, 5)
	assertNotSatisfied(t, vr, 0)
}

func TestLogicCompileErrors(t *testing.T) {
	if _, ge := (&Node{kind: kindAnd}).Validator(intType); ge == nil {
		t.Error("want an error for an empty and")
	}
	if _, ge := (&Node{kind: kindNot, children: []Node{{kind: kindZero}, {kind: kindZero}}}).Validator(intType); ge == nil {
		t.Error("want an error for a two-child not")
	}
}

func TestWhenRunsThenOnlyWhenConditionHolds(t *testing.T) {
	type subject struct {
		Mode   string  `validate:"oneof(READ_ONLY,READ_WRITE)"`
		Create *string `validate:"when($.Mode,eq,READ_WRITE,required)"`
	}
	vr, ge := NewValidator(reflect.TypeFor[subject](), "")
	if ge != nil {
		t.Fatalf("compile: %v", ge)
	}

	readWrite := "READ_WRITE"
	if ge = vr.Validate(subject{Mode: readWrite}); ge == nil {
		t.Error("Mode READ_WRITE with absent Create should fail the then-constraint")
	}

	private := "PRIVATE"
	if ge = vr.Validate(subject{Mode: readWrite, Create: &private}); ge != nil {
		t.Errorf("Mode READ_WRITE with present Create should pass: %v", ge)
	}

	if ge = vr.Validate(subject{Mode: "READ_ONLY"}); ge != nil {
		t.Errorf("Mode READ_ONLY should skip the then-constraint: %v", ge)
	}
}

func TestWhenConditionFieldAbsentSkipsThen(t *testing.T) {
	type subject struct {
		Mode   *string `validate:"or(nil,oneof(READ_ONLY,READ_WRITE))"`
		Create *string `validate:"when($.Mode,eq,READ_WRITE,required)"`
	}
	vr, ge := NewValidator(reflect.TypeFor[subject](), "")
	if ge != nil {
		t.Fatalf("compile: %v", ge)
	}
	if ge = vr.Validate(subject{}); ge != nil {
		t.Errorf("absent condition field should skip the then-constraint: %v", ge)
	}
}

func TestWhenRequiresWholeStructCompilation(t *testing.T) {
	node := mustParse(t, "when($.Mode,eq,READ_WRITE,required)")
	if _, ge := node.Validator(reflect.TypeFor[string]()); ge == nil {
		t.Fatal("when outside whole-struct compilation should be a configuration error")
	}
}

func TestWhenRejectsUnknownOperator(t *testing.T) {
	type subject struct {
		Mode   string `validate:"oneof(a,b)"`
		Create string `validate:"when($.Mode,between,b,required)"`
	}
	if _, ge := NewValidator(reflect.TypeFor[subject](), ""); ge == nil {
		t.Fatal("an unknown when operator should be a configuration error")
	}
}

func TestWhenRoundTrips(t *testing.T) {
	for _, tag := range []string{
		"when($.AccessMode,eq,READ_WRITE,required)",
		"when($.Mode,neq,OFF,and(required,oneof(a,b)))",
	} {
		node, ge := parseDirective(tag)
		if ge != nil {
			t.Fatalf("parse %q: %v", tag, ge)
		}
		if got := node.String(); got != tag {
			t.Errorf("round-trip: got %q, want %q", got, tag)
		}
	}
}

// TestCustomReportSuppressedInOr checks that a reporting check inside an Or branch
// records nothing: the branch's per-element reports are suppressed and, when the whole Or
// fails, it reports one failure naming the Or node, not the check's targets.
func TestCustomReportSuppressedInOr(t *testing.T) {
	custom := nonNegativeEach()
	node := Node{kind: kindOr, children: []Node{
		{kind: kindCustom, name: custom.Name(), custom: custom},
	}}
	vr := mustValidator(t, node, intSliceType)

	ge := vr.Validate([]int{-1, -2})
	if _, ok := errors.AsType[*gomerr.BatchError](ge); ok {
		t.Fatalf("want one failure for the Or node, got a batch of the branch's reports: %v", ge)
	}
	nse := asNotSatisfied(t, vr, []int{-1, -2})
	if nse.Target != "" {
		t.Errorf("want the Or failure at the field target, got %q", nse.Target)
	}
	if nse.Expected == "a non-negative number" {
		t.Error("the Or must not surface the suppressed branch's reported detail")
	}
}

func TestOrNilZeroAlloc(t *testing.T) {
	node := Node{kind: kindOr, children: []Node{
		{kind: kindNil},
		{kind: kindLength, params: []operand{{Value: "1"}, {Value: "16"}}},
	}}
	vr := mustValidator(t, node, stringPtr)
	var present any = new("hello")
	var absent any = (*string)(nil)
	assertZeroAlloc(t, func() { _ = vr.Validate(present) })
	assertZeroAlloc(t, func() { _ = vr.Validate(absent) })
}

// Container

func TestElements(t *testing.T) {
	node := Node{kind: kindElements, children: []Node{{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}}}
	vr := mustValidator(t, node, reflect.TypeFor[[]int]())
	assertPass(t, vr, []int{0, 1, 2})
	nse := asNotSatisfied(t, vr, []int{1, -1, 2})
	if nse.Target != "1" {
		t.Errorf("want the second element targeted as %q, got %q", "1", nse.Target)
	}

	// A nil collection passes.
	assertPass(t, mustValidator(t, node, reflect.TypeFor[[]int]()), []int(nil))
}

func TestElementsAnyWrongKind(t *testing.T) {
	node := Node{kind: kindElements, children: []Node{{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}}}
	vr := mustValidator(t, node, anyInterfaceType)
	assertNotSatisfied(t, vr, 42) // not a slice
}

func TestElementsWrongTypeCompile(t *testing.T) {
	node := Node{kind: kindElements, children: []Node{{kind: kindNotZero}}}
	if _, ge := node.Validator(intType); ge == nil {
		t.Error("want a compile error: elements on a non-slice type")
	}
}

func TestMapValues(t *testing.T) {
	node := Node{kind: kindMapValues, children: []Node{{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}}}
	vr := mustValidator(t, node, reflect.TypeFor[map[string]int]())
	assertPass(t, vr, map[string]int{"a": 1, "b": 2})
	nse := asNotSatisfied(t, vr, map[string]int{"bad": -1})
	if nse.Target != "bad" {
		t.Errorf("want the value targeted by its key %q, got %q", "bad", nse.Target)
	}
}

func TestMapKeys(t *testing.T) {
	node := Node{kind: kindMapKeys, children: []Node{{kind: kindLength, params: []operand{{Value: "1"}, {Value: "3"}}}}}
	vr := mustValidator(t, node, reflect.TypeFor[map[string]int]())
	assertPass(t, vr, map[string]int{"ab": 1})
	assertNotSatisfied(t, vr, map[string]int{"toolong": 1})
}

func TestElementsZeroAlloc(t *testing.T) {
	node := Node{kind: kindElements, children: []Node{{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}}}
	vr := mustValidator(t, node, reflect.TypeFor[[]int]())
	var v any = make([]int, 100)
	assertZeroAlloc(t, func() { _ = vr.Validate(v) })
}

func mustNewValidator(t *testing.T, v any, scope string) *Validator {
	t.Helper()
	vr, ge := NewValidator(reflect.TypeOf(v), scope)
	if ge != nil {
		t.Fatalf("NewValidator(%T, %q): %v", v, scope, ge)
	}
	return vr
}

type simpleStruct struct {
	N int    `validate:"gte(0)"`
	S string `validate:"len(1,8)"`
}

func TestNewValidatorFields(t *testing.T) {
	vr := mustNewValidator(t, simpleStruct{}, "")
	assertPass(t, vr, &simpleStruct{N: 1, S: "ok"})

	nse := asNotSatisfied(t, vr, &simpleStruct{N: -1, S: "ok"})
	if nse.Target != "N" {
		t.Errorf("want target N, got %q", nse.Target)
	}

	// Both fields fail: And over fields reports each.
	if be, ok := errors.AsType[*gomerr.BatchError](vr.Validate(&simpleStruct{N: -1, S: ""})); !ok {
		t.Fatalf("want a BatchError aggregating the problems, got %T: %v", be, be)
	} else if len(be.Errors()) != 2 {
		t.Errorf("want 2 failures reported, got %v", be)
	}
}

type EmbeddedInner struct {
	Items []string `validate:"len(0,2)"`
}

type embeddingOuter struct {
	EmbeddedInner `validate:"notzero($.Items)"`
}

// An embedded struct's own tags run even when the embedded field carries a directive of
// its own.
func TestNewValidatorEmbeddedWithDirective(t *testing.T) {
	vr := mustNewValidator(t, embeddingOuter{}, "")
	assertPass(t, vr, &embeddingOuter{EmbeddedInner{Items: []string{"a"}}})
	assertNotSatisfied(t, vr, &embeddingOuter{})                                              // the directive: Items must be set
	assertNotSatisfied(t, vr, &embeddingOuter{EmbeddedInner{Items: []string{"a", "b", "c"}}}) // the promoted field's len
}

func TestNewValidatorNilRule(t *testing.T) {
	// A nil struct pointer passes by the nil rule.
	vr := mustNewValidator(t, simpleStruct{}, "")
	assertPass(t, vr, (*simpleStruct)(nil))
}

type refBounds struct {
	Min *int `validate:"or(nil,gte(0))"`
	Max *int `validate:"gte($.Min)"`
}

func TestNewValidatorFieldRefBound(t *testing.T) {
	vr := mustNewValidator(t, refBounds{}, "")

	assertPass(t, vr, &refBounds{Min: new(1), Max: new(5)})         // 5 >= 1
	assertPass(t, vr, &refBounds{Min: nil, Max: new(5)})            // unset $.Min skips the comparison
	assertPass(t, vr, &refBounds{Min: new(3), Max: nil})            // nil Max passes by the nil rule
	assertNotSatisfied(t, vr, &refBounds{Min: new(3), Max: new(1)}) // 1 >= 3 is false
}

type redirectStruct struct {
	Floor   *int `validate:"or(nil,gte(0))"`
	Default *int `validate:"or(nil($.Floor),gte($.Floor))"`
}

func TestNewValidatorSubjectRedirectSkip(t *testing.T) {
	vr := mustNewValidator(t, redirectStruct{}, "")

	// nil($.Floor) holds when Floor is unset, so the Or short-circuits to pass.
	assertPass(t, vr, &redirectStruct{Floor: nil, Default: new(-5)})
	// Floor set: Default must be >= Floor.
	assertPass(t, vr, &redirectStruct{Floor: new(2), Default: new(2)})
	assertNotSatisfied(t, vr, &redirectStruct{Floor: new(2), Default: new(1)})
}

type missingRef struct {
	A int `validate:"gte($.Nope)"`
}

func TestNewValidatorMissingRefIsCompileError(t *testing.T) {
	_, ge := NewValidator(reflect.TypeFor[missingRef](), "")
	if ge == nil {
		t.Fatal("want a compile error for a reference to a missing field")
	}
}

type scopedStruct struct {
	Id   string `validate:"len(3,8);list:"`
	Name string `validate:"create:len(1,4)"`
}

func TestNewValidatorScopes(t *testing.T) {
	create := mustNewValidator(t, scopedStruct{}, "create")
	assertPass(t, create, &scopedStruct{Id: "abc", Name: "ok"})
	assertNotSatisfied(t, create, &scopedStruct{Id: "ab", Name: "ok"})       // Id too short
	assertNotSatisfied(t, create, &scopedStruct{Id: "abc", Name: "toolong"}) // Name only validated in create

	// In the list scope Id has an empty section (skipped) and Name has no section, so
	// nothing is validated and a value that would fail in create passes.
	list := mustNewValidator(t, scopedStruct{}, "list")
	assertPass(t, list, &scopedStruct{Id: "ab", Name: "toolong"})
}

type innerStruct struct {
	V string `validate:"len(1,3)"`
}

type structHolder struct {
	In innerStruct `validate:"struct"`
}

func TestNewValidatorNested(t *testing.T) {
	vr := mustNewValidator(t, structHolder{}, "")
	assertPass(t, vr, &structHolder{In: innerStruct{V: "ab"}})
	nse := asNotSatisfied(t, vr, &structHolder{In: innerStruct{V: "abcd"}})
	if nse.Target != "In.V" {
		t.Errorf("want nested target In.V, got %q", nse.Target)
	}
}

type unionMembers struct {
	A *innerStruct `validate:"struct"`
	B *innerStruct `validate:"struct"`
}

type unionHolder struct {
	U unionMembers `validate:"union"`
}

func TestNewValidatorUnion(t *testing.T) {
	vr := mustNewValidator(t, unionHolder{}, "")

	assertPass(t, vr, &unionHolder{U: unionMembers{A: &innerStruct{V: "ab"}}})
	assertNotSatisfied(t, vr, &unionHolder{U: unionMembers{}})                                                   // none set
	assertNotSatisfied(t, vr, &unionHolder{U: unionMembers{A: &innerStruct{V: "ab"}, B: &innerStruct{V: "cd"}}}) // two set
	assertNotSatisfied(t, vr, &unionHolder{U: unionMembers{A: &innerStruct{V: "toolong"}}})                      // set member invalid
}

type tree struct {
	Name  string `validate:"len(1,4)"`
	Child *tree  `validate:"or(nil,struct)"`
}

func TestNewValidatorRecursive(t *testing.T) {
	vr := mustNewValidator(t, tree{}, "")
	assertPass(t, vr, &tree{Name: "a", Child: &tree{Name: "b", Child: &tree{Name: "c"}}})
	assertNotSatisfied(t, vr, &tree{Name: "a", Child: &tree{Name: "toolong"}})
}

func TestNewValidatorCacheReturnsSameValidator(t *testing.T) {
	vr1, ge := NewValidator(reflect.TypeFor[simpleStruct](), "cachetest")
	if ge != nil {
		t.Fatal(ge)
	}
	vr2, ge := NewValidator(reflect.TypeFor[simpleStruct](), "cachetest")
	if ge != nil {
		t.Fatal(ge)
	}
	if vr1 != vr2 {
		t.Error("want the cache to return the same Validator for the same key")
	}
}

func TestNewValidatorTargetNamer(t *testing.T) {
	lower := func(_ reflect.Type, sf reflect.StructField) string {
		return "_" + sf.Name
	}
	SetTargetNamer(lower)
	defer SetTargetNamer(nil)
	vr := mustNewValidator(t, simpleStruct{}, "namer")
	nse := asNotSatisfied(t, vr, &simpleStruct{N: -1, S: "ok"})
	if nse.Target != "_N" {
		t.Errorf("want renamed target _N, got %q", nse.Target)
	}
}

func TestNewValidatorZeroAllocSuccess(t *testing.T) {
	vr := mustNewValidator(t, simpleStruct{}, "zeroalloc")
	v := &simpleStruct{N: 1, S: "ok"}
	assertZeroAlloc(t, func() { _ = vr.Validate(v) })

	// A $.Field comparison reads its bound without boxing, so it stays zero-allocation
	// too.
	vrRefs := mustNewValidator(t, refBounds{}, "zeroallocref")
	rv := &refBounds{Min: new(1), Max: new(5)}
	assertZeroAlloc(t, func() { _ = vrRefs.Validate(rv) })
}

// TestNewValidatorConcurrentFirstUse compiles and validates one type from many goroutines
// at once. The cache's compile-once discipline must give every goroutine a correct result
// with no race; run under -race.
func TestNewValidatorConcurrentFirstUse(t *testing.T) {
	var wrong atomic.Int64
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 500 {
				vr, ge := NewValidator(reflect.TypeFor[refBounds](), "concurrent")
				if ge != nil {
					wrong.Add(1)
					return
				}
				minimum, maximum := 0, 5
				if (g+i)%2 == 0 {
					minimum = 10 // 5 >= 10 is false
				}
				wantPass := maximum >= minimum
				if (vr.Validate(&refBounds{Min: &minimum, Max: &maximum}) == nil) != wantPass {
					wrong.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	if n := wrong.Load(); n > 0 {
		t.Fatalf("%d concurrent validations were wrong", n)
	}
}

type customCheck struct {
	name     string
	accepts  func(t reflect.Type) gomerr.Gomerr
	check    func(v any, _ CustomContext) bool
	describe func() string
}

func (c customCheck) Name() string {
	return c.name
}

func (c customCheck) Accepts(r reflect.Type) gomerr.Gomerr {
	if c.accepts != nil {
		return c.accepts(r)
	}
	return nil
}

func (c customCheck) Check(v any, cc CustomContext) bool {
	return c.check(v, cc)
}

func (c customCheck) Describe() string {
	if c.describe != nil {
		return c.describe()
	}
	return ""
}

func evenCheck() Custom {
	return customCheck{
		name: "$even",
		accepts: func(t reflect.Type) gomerr.Gomerr {
			if t != nil && t.Kind() == reflect.Int {
				return nil
			}
			return gomerr.Configuration("$even requires an int")
		},
		check:    func(v any, _ CustomContext) bool { return v.(int)%2 == 0 },
		describe: func() string { return "an even number" },
	}
}

func TestCustomFieldLevel(t *testing.T) {
	node := Node{kind: kindCustom, name: "$even", custom: evenCheck()}
	vr := mustValidator(t, node, intType)
	assertPass(t, vr, 4)
	nse := asNotSatisfied(t, vr, 3)
	if nse.Expected != "an even number" {
		t.Errorf("want the Describe text as Expected, got %q", nse.Expected)
	}
}

func TestCustomAcceptsRejectsType(t *testing.T) {
	node := Node{kind: kindCustom, name: "$even", custom: evenCheck()}
	if _, ge := node.Validator(stringType); ge == nil {
		t.Error("want a compile error: $even rejects a string field")
	}
}

// TestCustomPanicIsFault checks that a custom check that panics on a value is reported
// as a fault, not dropped and not a validation failure.
func TestCustomPanicIsFault(t *testing.T) {
	custom := customCheck{
		name:     "$panic",
		check:    func(v any, _ CustomContext) bool { return v.(string) == "" }, // panics on a non-string
		describe: func() string { return "a string" },
	}
	node := Node{kind: kindCustom, name: "$panic", custom: &custom}
	vr := mustValidator(t, node, anyInterfaceType)
	ge := vr.Validate(42)
	if ge == nil {
		t.Fatal("want a fault from the panicking check")
	}
	if _, ok := errors.AsType[*NotSatisfiedError](ge); ok {
		t.Errorf("a fault should not be reported as a validation failure: %v", ge)
	}
}

func TestEnclosingIsAbsentForFieldCheck(t *testing.T) {
	var sawEnclosing bool
	custom := customCheck{
		name: "$peek",
		check: func(v any, cc CustomContext) bool {
			_, sawEnclosing = cc.Enclosing()
			return true
		},
	}
	node := Node{kind: kindCustom, name: "$peek", custom: &custom}
	vr := mustValidator(t, node, intType)
	assertPass(t, vr, 1)
	if sawEnclosing {
		t.Error("a field-level custom check has no enclosing struct")
	}
}

type peekHolder struct {
	Limit int
	Value int `validate:"$ltlimit"`
}

// TestEnclosingInWholeStruct checks that a custom check inside a whole-struct Validator
// sees the struct under test through Enclosing, so it can read a sibling field.
func TestEnclosingInWholeStruct(t *testing.T) {
	ge := RegisterCustom(customCheck{
		name: "$ltlimit",
		check: func(v any, cc CustomContext) bool {
			enc, ok := cc.Enclosing()
			if !ok {
				return false
			}
			return v.(int) <= enc.(peekHolder).Limit
		},
		describe: func() string { return "at most Limit" },
	})
	if ge != nil {
		t.Fatal(ge)
	}
	vr := mustNewValidator(t, peekHolder{}, "enclosing")
	assertPass(t, vr, &peekHolder{Limit: 10, Value: 5})
	assertNotSatisfied(t, vr, &peekHolder{Limit: 3, Value: 5})
}

var intSliceType = reflect.TypeFor[[]int]()

// nonNegativeEach reports one failure per negative element, each targeted by the
// element's index, rather than failing the whole field.
func nonNegativeEach() Custom {
	return customCheck{
		name: "$nonnegeach",
		accepts: func(t reflect.Type) gomerr.Gomerr {
			if t != nil && t.Kind() == reflect.Slice {
				return nil
			}
			return gomerr.Configuration("$nonnegeach requires a slice")
		},
		check: func(v any, cc CustomContext) bool {
			ok := true
			for i, x := range v.([]int) {
				if x < 0 {
					cc.Report(strconv.Itoa(i), "a non-negative number")
					ok = false
				}
			}
			return ok
		},
	}
}

func TestCustomReportsEachFailure(t *testing.T) {
	custom := nonNegativeEach()
	node := Node{kind: kindCustom, name: custom.Name(), custom: custom}
	vr := mustValidator(t, node, intSliceType)

	assertPass(t, vr, []int{0, 1, 2})

	be, ok := errors.AsType[*gomerr.BatchError](vr.Validate([]int{-1, 2, -3}))
	if !ok || len(be.Errors()) != 2 {
		t.Fatalf("want 2 reported failures, got %v", be)
	}
	wantTargets := map[string]bool{"0": true, "2": true}
	for _, ge := range be.Errors() {
		nse, nOk := errors.AsType[*NotSatisfiedError](ge)
		if !nOk {
			t.Fatalf("want NotSatisfiedError, got %T: %v", ge, ge)
		} else if !wantTargets[nse.Target] {
			t.Errorf("unexpected target %q", nse.Target)
		} else if nse.Expected != "a non-negative number" {
			t.Errorf("target %q: want the Report expected text, got %q", nse.Target, nse.Expected)
		}
		delete(wantTargets, nse.Target)
	}
	if len(wantTargets) != 0 {
		t.Errorf("missing targets %v", wantTargets)
	}
}

// TestCustomReportSingleIsNotBatched checks that one reported failure comes back as a
// lone NotSatisfiedError, not a batch.
func TestCustomReportSingleIsNotBatched(t *testing.T) {
	custom := nonNegativeEach()
	node := Node{kind: kindCustom, name: custom.Name(), custom: custom}
	vr := mustValidator(t, node, intSliceType)

	nse := asNotSatisfied(t, vr, []int{5, -1})
	if nse.Target != "1" {
		t.Errorf("want target of the one bad element, got %q", nse.Target)
	}
}

// TestCustomReportHonorsBudget caps the reported failures at the failure budget, as every
// other failure path does.
func TestCustomReportHonorsBudget(t *testing.T) {
	custom := nonNegativeEach()
	node := Node{kind: kindCustom, name: custom.Name(), custom: custom}
	vr := mustValidator(t, node, intSliceType)

	values := make([]int, failureBudget+10)
	for i := range values {
		values[i] = -1
	}

	if be, ok := errors.AsType[*gomerr.BatchError](vr.Validate(values)); !ok {
		t.Fatalf("want a BatchError aggregating the problems, got %T: %v", be, be)
	} else if len(be.Errors()) != failureBudget {
		t.Fatalf("want the budget to cap reports at %d, got %v", failureBudget, be)
	}
}

type eachHolder struct {
	Values []int `validate:"$nonnegeach"`
}

// TestCustomReportTargetNestsUnderField checks that when a slice field's custom check
// reports per element, each failure's target nests under the field name.
func TestCustomReportTargetNestsUnderField(t *testing.T) {
	if ge := RegisterCustom(nonNegativeEach()); ge != nil {
		t.Fatal(ge)
	}
	vr := mustNewValidator(t, eachHolder{}, "nesting")

	assertPass(t, vr, &eachHolder{Values: []int{0, 1}})

	ge := vr.Validate(&eachHolder{Values: []int{-1, 5, -2}})
	be, ok := errors.AsType[*gomerr.BatchError](ge)
	if !ok {
		t.Fatalf("want a BatchError aggregating the problems, got %T: %v", ge, ge)
	} else if len(be.Errors()) != 2 {
		t.Fatalf("want 2 reported failures, got %v", be)
	}
	wantTargets := map[string]bool{"Values.0": true, "Values.2": true}
	for _, ge = range be.Errors() {
		nse, nOk := errors.AsType[*NotSatisfiedError](ge)
		if !nOk {
			t.Fatalf("want NotSatisfiedError, got %T: %v", ge, ge)
		} else if !wantTargets[nse.Target] {
			t.Errorf("unexpected target %v", nse.Target)
		}
		delete(wantTargets, nse.Target)
	}
	if len(wantTargets) != 0 {
		t.Errorf("missing targets %v", wantTargets)
	}
}

// TestCustomPassingPathAllocatesNothing asserts the reporting plumbing adds no allocation
// on a passing path. The field is a map so boxing the value under test as an any is free
// (a map is pointer-shaped), which isolates the context handoff: it must box a pointer,
// not a struct value, and must not box the enclosing struct unless Enclosing is called. A
// slice- or scalar-valued check still boxes its own value by the any contract.
func TestCustomPassingPathAllocatesNothing(t *testing.T) {
	custom := customCheck{
		name:  "$nonemptymap",
		check: func(v any, _ CustomContext) bool { return len(v.(map[string]int)) > 0 },
	}
	node := Node{kind: kindCustom, name: custom.name, custom: &custom}
	vr := mustValidator(t, node, reflect.TypeFor[map[string]int]())

	var v any = map[string]int{"a": 1}
	assertPass(t, vr, v)
	assertZeroAlloc(t, func() { benchSink = vr.Validate(v) })
}

func TestCompareTyped(t *testing.T) {
	gte10 := Node{kind: kindCompare, name: "gte", params: []operand{{Value: "10"}}}

	vrInt := mustValidator(t, gte10, intType)
	assertPass(t, vrInt, 10)
	assertPass(t, vrInt, 11)
	assertNotSatisfied(t, vrInt, 9)

	vrStr := mustValidator(t, Node{kind: kindCompare, name: "gt", params: []operand{{Value: "m"}}}, stringType)
	assertPass(t, vrStr, "n")
	assertNotSatisfied(t, vrStr, "a")

	vrUint := mustValidator(t, gte10, reflect.TypeFor[uint16]())
	assertPass(t, vrUint, uint16(10))
	assertNotSatisfied(t, vrUint, uint16(0))
}

// TestCompareNilRule checks that a comparison passes an absent (nil pointer) value;
// presence is a separate concern.
func TestCompareNilRule(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindCompare, name: "gte", params: []operand{{Value: "10"}}}, reflect.TypeFor[*int]())
	assertPass(t, vr, (*int)(nil))
	assertPass(t, vr, new(10))
	assertNotSatisfied(t, vr, new(9))
}

// TestCompareWrongKindCompile checks that a comparison against a type it can't
// order is caught when the Node compiles.
func TestCompareWrongKindCompile(t *testing.T) {
	if _, ge := (&Node{kind: kindCompare, name: "gte", params: []operand{{Value: "x"}}}).Validator(boolType); ge == nil {
		t.Error("want a compile error comparing a bool")
	}
	if _, ge := (&Node{kind: kindCompare, name: "gte", params: []operand{{Value: "x"}}}).Validator(intType); ge == nil {
		t.Error("want a compile error for a non-numeric bound")
	}
}

// TestCompareAnyWrongKind checks that, on the dynamic path, a value whose kind can't be
// compared fails at evaluation rather than panicking.
func TestCompareAnyWrongKind(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindCompare, name: "gte", params: []operand{{Value: "10"}}}, anyInterfaceType)
	var good any = 20
	assertPass(t, vr, good)
	nse := asNotSatisfied(t, vr, struct{}{})
	if nse.Expected == "" {
		t.Error("want an Expected describing the comparable type requirement")
	}
}

func TestBetween(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindBetween, params: []operand{{Value: "1"}, {Value: "10"}}}, intType)
	assertPass(t, vr, 1)
	assertPass(t, vr, 10)
	assertNotSatisfied(t, vr, 0)
	assertNotSatisfied(t, vr, 11)
	if nse := asNotSatisfied(t, vr, 11); nse.Node.kind != kindBetween {
		t.Errorf("want the failure reported against the between node, got %s", nse.Node.kind)
	}
}

func TestLengthString(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindLength, params: []operand{{Value: "1"}, {Value: "3"}}}, stringType)
	assertPass(t, vr, "a")
	assertPass(t, vr, "abc")
	assertNotSatisfied(t, vr, "")
	assertNotSatisfied(t, vr, "abcd")
}

// TestLengthUnit covers counting a string's length in bytes vs code points.
func TestLengthUnit(t *testing.T) {
	bytes := mustValidator(t, Node{kind: kindLength, params: []operand{{Value: "1"}, {Value: "1"}}}, stringType)
	assertNotSatisfied(t, bytes, "é") // 2 bytes

	points := mustValidator(t, Node{kind: kindLength, unit: UnitCodePoints, params: []operand{{Value: "1"}, {Value: "1"}}}, stringType)
	assertPass(t, points, "é") // 1 code point
}

func TestLengthOpenBound(t *testing.T) {
	minOnly := mustValidator(t, Node{kind: kindLength, params: []operand{{Value: "2"}, {Value: ""}}}, stringType)
	assertNotSatisfied(t, minOnly, "a")
	assertPass(t, minOnly, "abcdef")

	maxOnly := mustValidator(t, Node{kind: kindLength, params: []operand{{Value: ""}, {Value: "2"}}}, stringType)
	assertPass(t, maxOnly, "ab")
	assertNotSatisfied(t, maxOnly, "abc")
}

func TestLengthSliceAndNilRule(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindLength, params: []operand{{Value: "1"}, {Value: "2"}}}, stringSlice)
	assertPass(t, vr, []string{"a"})
	assertNotSatisfied(t, vr, []string{})

	// A minimum length on a nil pointer passes under the nil rule.
	vrPtr := mustValidator(t, Node{kind: kindLength, params: []operand{{Value: "1"}, {Value: ""}}}, stringPtr)
	assertPass(t, vrPtr, (*string)(nil))
}

func TestLengthWrongTypeCompile(t *testing.T) {
	if _, ge := (&Node{kind: kindLength, params: []operand{{Value: "1"}}}).Validator(intType); ge == nil {
		t.Error("want a compile error measuring an int")
	}
}

func TestPattern(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindPattern, params: []operand{{Value: "^a+$"}}}, stringType)
	assertPass(t, vr, "aaa")
	assertNotSatisfied(t, vr, "b")

	if _, ge := (&Node{kind: kindPattern, params: []operand{{Value: "("}}}).Validator(stringType); ge == nil {
		t.Error("want a compile error for an uncompilable pattern")
	}
	if _, ge := (&Node{kind: kindPattern, params: []operand{{Value: "a"}}}).Validator(intType); ge == nil {
		t.Error("want a compile error for a pattern on a non-string field")
	}
}

func TestOneOf(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindOneOf, params: []operand{{Value: "A"}, {Value: "B"}}}, stringType)
	assertPass(t, vr, "A")
	assertPass(t, vr, "B")
	assertNotSatisfied(t, vr, "C")

	vrInt := mustValidator(t, Node{kind: kindOneOf, params: []operand{{Value: "1"}, {Value: "2"}}}, intType)
	assertPass(t, vrInt, 1)
	assertNotSatisfied(t, vrInt, 3)
}

func TestPresent(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindPresent}, stringPtr)
	assertNotSatisfied(t, vr, (*string)(nil))
	assertPass(t, vr, new("")) // present even when the pointed-to value is empty

	// A non-pointer value is always present, including its zero value.
	vrPresent := mustValidator(t, Node{kind: kindPresent}, intType)
	assertPass(t, vrPresent, 0)
}

func TestNilness(t *testing.T) {
	vr := mustValidator(t, Node{kind: kindNil}, stringPtr)
	assertPass(t, vr, (*string)(nil))
	assertNotSatisfied(t, vr, new("x"))

	vrNotNil := mustValidator(t, Node{kind: kindNotNil}, stringPtr)
	assertNotSatisfied(t, vrNotNil, (*string)(nil))
	assertPass(t, vrNotNil, new("x"))

	// A non-nilable value is never nil, so nil fails it at evaluation (no compile-time
	// rejection).
	nonNilable := mustValidator(t, Node{kind: kindNil}, intType)
	assertNotSatisfied(t, nonNilable, 5)
}

func TestZeroness(t *testing.T) {
	z := mustValidator(t, Node{kind: kindZero}, intType)
	assertPass(t, z, 0)
	assertNotSatisfied(t, z, 1)

	nz := mustValidator(t, Node{kind: kindNotZero}, intType)
	assertNotSatisfied(t, nz, 0)
	assertPass(t, nz, 1)

	// Zero looks through pointers; an absent value is zero.
	vrZeroPtr := mustValidator(t, Node{kind: kindZero}, reflect.TypeFor[*int]())
	assertPass(t, vrZeroPtr, (*int)(nil))
	assertPass(t, vrZeroPtr, new(0))
	assertNotSatisfied(t, vrZeroPtr, new(5))
}

func TestBoolLeaves(t *testing.T) {
	tr := mustValidator(t, Node{kind: kindTrue}, boolType)
	assertPass(t, tr, true)
	assertNotSatisfied(t, tr, false)

	fa := mustValidator(t, Node{kind: kindFalse}, boolType)
	assertPass(t, fa, false)
	assertNotSatisfied(t, fa, true)
}

func TestEquality(t *testing.T) {
	eq := mustValidator(t, Node{kind: kindEquals, params: []operand{{Value: "5"}}}, intType)
	assertPass(t, eq, 5)
	assertNotSatisfied(t, eq, 6)

	neq := mustValidator(t, Node{kind: kindNotEquals, params: []operand{{Value: "5"}}}, intType)
	assertPass(t, neq, 6)
	assertNotSatisfied(t, neq, 5)
}

func TestFail(t *testing.T) {
	assertNotSatisfied(t, mustValidator(t, Node{kind: kindFail}, intType), 1)
}

func TestLeafZeroAlloc(t *testing.T) {
	var vInt any = int64(5)
	var vStr any = "hello"

	cases := []struct {
		name string
		p    *Validator
		v    any
	}{
		{"compareInt", mustValidator(t, Node{kind: kindCompare, name: "gte", params: []operand{{Value: "0"}}}, int64Type), vInt},
		{"lengthString", mustValidator(t, Node{kind: kindLength, params: []operand{{Value: "1"}, {Value: "16"}}}, stringType), vStr},
		{"pattern", mustValidator(t, Node{kind: kindPattern, params: []operand{{Value: "^[a-z]+$"}}}, stringType), vStr},
		{"oneOf", mustValidator(t, Node{kind: kindOneOf, params: []operand{{Value: "hello"}, {Value: "world"}}}, stringType), vStr},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assertPass(t, c.p, c.v)
			assertZeroAlloc(t, func() { _ = c.p.Validate(c.v) })
		})
	}
}
