package constraint

import (
	"cmp"
	"encoding/base64"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/structs"
)

// check is the compiled form of a node. It returns true when v satisfies the node. On
// failure, it records the failure through vc, unless vc is suppressing reports inside an
// or, and returns false. v is the value as the enclosing type stores it; a check follows
// pointers and interfaces itself.
//
// A compile function returns a nil check for a node that doesn't compile, after recording
// the error. No Validator is built from a tree with errors, so a nil check is never run.
type check func(v reflect.Value, vc *validationCtx) bool

// The limits bound what one compilation accepts, so a pathological tag or model can't
// produce an unbounded Validator. Exceeding one is a configuration error.
const (
	maxNodes    = 1000
	maxDepth    = 32
	maxPatterns = 50
)

// compileCtx holds the state of one compilation: the configuration errors found so far,
// which are reported together, the position in the node tree for naming them, and the
// running complexity counts.
type compileCtx struct {
	errors     gomerr.ErrorBatch
	path       []string
	nodes      int
	patterns   int
	overBudget bool
	// enclosing is the struct type a $.Field operand resolves against. It is nil when
	// compiling a single value, where a $.Field operand is a configuration error.
	enclosing reflect.Type
	// scope is the tag section a struct compilation reads, and is empty when compiling a
	// single value. inProgress holds the struct checks being built, so a recursive type
	// resolves to one check.
	scope      string
	inProgress map[structKey]*check
}

func (cc *compileCtx) fail(ge gomerr.Gomerr) {
	if p := strings.Join(cc.path, "."); p != "" {
		ge = ge.AddAttribute("path", p)
	}
	cc.errors.Capture(ge)
}

func (cc *compileCtx) failf(format string, args ...any) {
	cc.fail(gomerr.Configuration(fmt.Sprintf(format, args...)))
}

// compileNode specializes node for type t. It records the node's token on the path for
// error reporting, charges the node against the complexity limits, and dispatches on the
// node's kind. Once a limit is hit, compilation stops descending and reports that limit
// once.
func (cc *compileCtx) compileNode(node *Node, t reflect.Type) check {
	cc.path = append(cc.path, node.token())
	defer func() { cc.path = cc.path[:len(cc.path)-1] }()

	if cc.overBudget {
		return nil
	}
	cc.nodes++
	if node.kind == kindPattern {
		cc.patterns++
	}
	var exceeded string
	switch {
	case cc.nodes > maxNodes:
		exceeded = fmt.Sprintf("the %d node limit", maxNodes)
	case len(cc.path) > maxDepth:
		exceeded = fmt.Sprintf("the %d nesting-depth limit", maxDepth)
	case cc.patterns > maxPatterns:
		exceeded = fmt.Sprintf("the %d pattern limit", maxPatterns)
	}
	if exceeded != "" {
		cc.failf("node exceeds %s", exceeded)
		cc.overBudget = true
		return nil
	}

	switch node.kind {
	case kindAnd, kindOr, kindNot:
		return cc.compileLogic(node, t)
	case kindWhen:
		return cc.compileWhen(node, t)
	case kindElements, kindMapKeys, kindMapValues:
		return cc.compileContainer(node, t)
	case kindStruct, kindUnion:
		return cc.compileStructNode(node, t)
	case kindCustom:
		return cc.compileCustom(node, t)
	case kindInvalid:
		cc.failf("node has no kind")
		return nil
	default:
		return cc.compileLeaf(node, t)
	}
}

// compileLogic specializes and, or and not nodes, compiling each child against the same
// type. And reports every child that fails. Or and not run their children with reporting
// suppressed, so a failing child costs nothing, and report one failure of their own when
// the node as a whole fails.
func (cc *compileCtx) compileLogic(node *Node, t reflect.Type) check {
	children := make([]check, len(node.children))
	for i := range node.children {
		children[i] = cc.compileNode(&node.children[i], t)
	}

	//goland:noinspection GoSwitchMissingCasesForIotaConsts
	switch node.kind {
	case kindAnd:
		if len(children) == 0 {
			cc.failf("and takes at least one operand")
			return nil
		}
		return func(v reflect.Value, vc *validationCtx) bool {
			satisfied := true
			for _, c := range children {
				if !c(v, vc) {
					satisfied = false
				}
				if vc.done() {
					break
				}
			}
			return satisfied
		}
	case kindOr:
		if len(children) == 0 {
			cc.failf("or takes at least one operand")
			return nil
		}
		expected := node.String()
		// or(nil,X) and or(zero,X) mark X optional rather than offer alternatives. When
		// every branch fails, X runs again with reporting on, so its own failures and
		// nested targets are reported in place of a single failure for the or.
		guarded := -1
		if len(node.children) > 1 {
			for i := range node.children {
				if k := node.children[i].kind; k == kindNil || k == kindZero {
					continue
				}
				if guarded >= 0 {
					guarded = -1
					break
				}
				guarded = i
			}
		}
		return func(v reflect.Value, vc *validationCtx) bool {
			saved := vc.report
			vc.report = false
			passed := false
			for _, c := range children {
				if c(v, vc) {
					passed = true
					break
				}
			}
			vc.report = saved
			if passed {
				return true
			}
			if guarded >= 0 {
				return children[guarded](v, vc)
			}
			cv, _ := deref(v)
			return vc.failValue(node, expected, cv)
		}
	case kindNot:
		if len(children) != 1 {
			cc.failf("not takes exactly one operand, found %d", len(children))
			return nil
		}
		child := children[0]
		expected := node.String()
		return func(v reflect.Value, vc *validationCtx) bool {
			saved := vc.report
			vc.report = false
			inner := child(v, vc)
			vc.report = saved
			if inner {
				cv, _ := deref(v)
				return vc.failValue(node, expected, cv)
			}
			return true
		}
	}
	return nil
}

// compileWhen specializes a when node, which runs its child only when a field of the
// enclosing struct compares as the condition states. Like any $.Field operand, the
// condition requires a struct compilation. A condition field that is absent, or whose
// kind can't be compared to the condition value, does not hold, so the value passes.
func (cc *compileCtx) compileWhen(node *Node, t reflect.Type) check {
	if len(node.params) != 3 || !node.params[0].isRef() {
		cc.failf("when takes a $.Field condition, an operator, a value and a then-constraint")
		return nil
	}
	if len(node.children) != 1 {
		cc.failf("when takes exactly one then-constraint, found %d", len(node.children))
		return nil
	}
	switch operator := strings.ToLower(fmt.Sprint(node.params[1].Value)); operator {
	case "eq", "neq", "gt", "gte", "lt", "lte":
		ref, ok := cc.resolveRef(node.params[0].Ref)
		if !ok {
			return nil
		}
		value := toScalar(node.params[2].Value)
		then := cc.compileNode(&node.children[0], t)

		return func(v reflect.Value, vc *validationCtx) bool {
			cv, present := vc.refValue(ref)
			if !present {
				return true
			}
			order, kindOk := orderScalar(cv, value)
			if !kindOk || !orderSatisfies(operator, order) {
				return true
			}
			return then(v, vc)
		}
	default:
		cc.failf("when operator %q is not one of eq, neq, gt, gte, lt, lte", operator)
		return nil
	}
}

// compileContainer specializes elements, mapkeys and mapvalues nodes. The child node is
// compiled against the element, key or value type and run at each position, extending the
// failure target with the index or key. An absent container passes. A value of the wrong
// kind fails at compile time for a concrete field and at evaluation for a dynamic one.
func (cc *compileCtx) compileContainer(node *Node, t reflect.Type) check {
	if len(node.children) != 1 {
		cc.failf("%s takes exactly one element node, found %d", node.token(), len(node.children))
		return nil
	}

	var elemT reflect.Type
	if bt := baseType(t); bt != nil {
		switch node.kind {
		case kindElements:
			if bt.Kind() != reflect.Slice && bt.Kind() != reflect.Array {
				cc.failf("elements requires a slice or array field, found %s", bt)
				return nil
			}
			elemT = bt.Elem()
		case kindMapKeys, kindMapValues:
			if bt.Kind() != reflect.Map {
				cc.failf("%s requires a map field, found %s", node.token(), bt)
				return nil
			}
			elemT = bt.Elem()
			if node.kind == kindMapKeys {
				elemT = bt.Key()
			}
		}
	}

	child := cc.compileNode(&node.children[0], elemT)
	expected := node.String()

	if node.kind == kindElements {
		return func(v reflect.Value, vc *validationCtx) bool {
			cv, ok := deref(v)
			if !ok {
				return true
			}
			if cv.Kind() != reflect.Slice && cv.Kind() != reflect.Array {
				return vc.failValue(node, expected, cv)
			}
			satisfied := true
			for i := 0; i < cv.Len(); i++ {
				n := len(vc.path)
				if n > 0 {
					vc.path = append(vc.path, '.')
				}
				vc.path = strconv.AppendInt(vc.path, int64(i), 10)
				if !child(cv.Index(i), vc) {
					satisfied = false
				}
				vc.truncate(n)
				if vc.done() {
					break
				}
			}
			return satisfied
		}
	}

	keys := node.kind == kindMapKeys
	return func(v reflect.Value, vc *validationCtx) bool {
		cv, ok := deref(v)
		if !ok {
			return true
		}
		if cv.Kind() != reflect.Map {
			return vc.failValue(node, expected, cv)
		}
		satisfied := true
		iter := cv.MapRange()
		for iter.Next() {
			n := vc.push(fmt.Sprintf("%v", iter.Key().Interface()))
			elem := iter.Value()
			if keys {
				elem = iter.Key()
			}
			if !child(elem, vc) {
				satisfied = false
			}
			vc.truncate(n)
			if vc.done() {
				break
			}
		}
		return satisfied
	}
}

// compileStructNode specializes struct and union nodes, which validate a nested struct
// against its own tags. Union also requires exactly one exported field to be set. Both
// need a struct compilation for the scope to read the nested tags in.
func (cc *compileCtx) compileStructNode(node *Node, t reflect.Type) check {
	if cc.scope == "" {
		cc.failf("%s validation requires whole-struct compilation", node.kind)
		return nil
	}

	bt := derefStructType(t)
	if bt == nil {
		cc.failf("%s requires a struct field, found %s", node.token(), t)
		return nil
	}

	nested := cc.structCheck(bt, cc.scope)
	if node.kind == kindStruct {
		return nested
	}

	expected := "exactly one member"
	return func(v reflect.Value, vc *validationCtx) bool {
		sv, ok := deref(v)
		if !ok {
			return true
		}
		if sv.Kind() != reflect.Struct {
			return vc.failValue(node, expected, sv)
		}
		count := 0
		for _, fv := range sv.Fields() {
			if fv.IsValid() && fv.CanInterface() && !fv.IsZero() {
				count++
			}
		}
		satisfied := true
		if count != 1 {
			satisfied = vc.failValue(node, expected, sv)
		}
		if !nested(sv, vc) {
			satisfied = false
		}
		return satisfied
	}
}

func derefStructType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}
	return t
}

// compileCustom specializes a custom check for type t. A field type the check's Accepts
// rejects is a configuration error. At evaluation an absent value passes, and a check
// that panics is reported as a fault rather than a validation failure.
func (cc *compileCtx) compileCustom(node *Node, t reflect.Type) check {
	custom := node.custom
	if custom == nil {
		cc.failf("custom check %q has no implementation", node.name)
		return nil
	} else if ge := custom.Accepts(t); ge != nil {
		cc.fail(ge)
		return nil
	}
	expected := custom.Describe()

	return func(v reflect.Value, vc *validationCtx) (satisfied bool) {
		cv, present := deref(v)
		if !present {
			return true
		}
		cx := &vc.customCtx
		cx.vc = vc
		cx.node = node
		cx.reported = 0
		defer func() {
			if r := recover(); r != nil {
				// A fault isn't counted against the failure budget, so it is always
				// reported.
				ge := gomerr.Unprocessable("custom check panicked", r).
					AddAttribute("check", custom.Name)
				if target := string(vc.path); target != "" {
					ge = ge.AddAttribute("target", target)
				}
				vc.errors.Capture(ge)
				satisfied = false
			}
		}()
		passed := custom.Check(ifaceOf(cv), cx)
		if cx.reported > 0 {
			return false
		}
		if passed {
			return true
		}
		return vc.failValue(node, expected, cv)
	}
}

// compileLeaf specializes a leaf node for type t. For a concrete type, a leaf that can't
// apply to the field, such as a comparison on a bool or an invalid pattern, is a
// configuration error. When t is nil or an interface, the leaf compiles to a dynamic
// check that fails a value of the wrong kind at evaluation.
func (cc *compileCtx) compileLeaf(node *Node, t reflect.Type) check {
	for i := range node.params {
		if node.params[i].isRef() {
			return cc.compileLeafRef(node)
		}
	}

	art, ok := cc.leafArtifacts(node)
	if !ok {
		return nil
	}
	if bt := baseType(t); bt != nil {
		cc.validateLeafType(node, bt, art)
	}
	return makeLeafCheck(node, art)
}

// leafArtifact holds a leaf's compile-time work, so evaluation neither parses nor
// allocates: operands read into every scalar form they take, the compiled pattern, and
// the length bounds.
type leafArtifact struct {
	expected string
	params   []scalar
	re       *regexp.Regexp
	lenMin   int
	lenMax   int
	hasMin   bool
	hasMax   bool
}

// leafArtifacts does a leaf's compile-time work. ok is false when the operands are the
// wrong number or form, so compileLeaf stops before building a check that would read
// operands the node doesn't have.
func (cc *compileCtx) leafArtifacts(node *Node) (leafArtifact, bool) {
	art := leafArtifact{expected: node.String()}
	art.params = make([]scalar, len(node.params))
	for i := range node.params {
		art.params[i] = toScalar(node.params[i].Value)
	}

	ok := true
	fail := func(format string, args ...any) {
		cc.failf(format, args...)
		ok = false
	}

	//goland:noinspection GoSwitchMissingCasesForIotaConsts
	switch node.kind {
	case kindCompare, kindEquals, kindNotEquals:
		if len(node.params) != 1 {
			fail("%s takes one operand, found %d", node.token(), len(node.params))
		}
	case kindBetween:
		if len(node.params) != 2 {
			fail("between takes a lower and an upper operand, found %d", len(node.params))
		}
	case kindOneOf:
		if len(node.params) == 0 {
			fail("oneof takes at least one operand")
		}
	case kindPattern:
		pattern := ""
		if len(art.params) > 0 {
			pattern = art.params[0].s
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			fail("%q is not a valid regular expression: %s", pattern, err.Error())
		}
		art.re = re
	case kindLength:
		if n, uOk := uintOperand(art.params, 0); uOk {
			art.lenMin, art.hasMin = n, true
		}
		if n, uOk := uintOperand(art.params, 1); uOk {
			art.lenMax, art.hasMax = n, true
		}
		if !art.hasMin && !art.hasMax {
			fail("len takes at least one of a minimum or maximum bound")
		}
		if art.hasMin && art.hasMax && art.lenMin > art.lenMax {
			fail("len minimum %d is greater than maximum %d", art.lenMin, art.lenMax)
		}
	}
	return art, ok
}

// uintOperand reads the operand at i as a non-negative integer. An absent or empty
// operand leaves that bound open, which is how len states a one-sided range.
func uintOperand(params []scalar, i int) (int, bool) {
	if i >= len(params) {
		return 0, false
	}
	s := params[i]
	if s.s == "" {
		return 0, false
	}
	if s.iOk && s.i >= 0 {
		return int(s.i), true
	}
	if s.uOk {
		return int(s.u), true
	}
	return 0, false
}

// validateLeafType reports a configuration error when a leaf can't apply to field type
// bt. The leaves not listed here apply to any type.
func (cc *compileCtx) validateLeafType(node *Node, bt reflect.Type, art leafArtifact) {
	token := node.token()
	//goland:noinspection GoSwitchMissingCasesForIotaConsts
	switch node.kind {
	case kindCompare, kindBetween:
		k := bt.Kind()
		if !isSignedKind(k) && !isUnsignedKind(k) && !isFloatKind(k) && k != reflect.String &&
			bt != timeType {
			cc.failf("%s requires an ordered field, found %s", token, bt)
			return
		}
		for i := range art.params {
			cc.checkBound(bt, art.params[i], token)
		}
	case kindLength:
		switch bt.Kind() {
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice, reflect.String:
		default:
			cc.failf("len requires a string, slice, array, map or channel field, found %s", bt)
		}
	case kindPattern, kindIsRegexp:
		if bt.Kind() != reflect.String {
			cc.failf("%s requires a string field, found %s", token, bt)
		}
	case kindTrue, kindFalse:
		if bt.Kind() != reflect.Bool {
			cc.failf("%s requires a bool field, found %s", token, bt)
		}
	}
}

// checkBound reports a configuration error when comparison bound b doesn't read as a
// value of field type bt.
func (cc *compileCtx) checkBound(bt reflect.Type, b scalar, token string) {
	switch {
	case isSignedKind(bt.Kind()):
		if !b.iOk && !b.fOk {
			cc.failf("%s bound %q is not a number for %s field", token, b.s, bt)
		}
	case isUnsignedKind(bt.Kind()):
		if b.uOk {
			return
		}
		if b.iOk && b.i < 0 {
			cc.failf("%s bound %q is negative for unsigned %s field", token, b.s, bt)
			return
		}
		if !b.fOk {
			cc.failf("%s bound %q is not a number for %s field", token, b.s, bt)
		}
	case isFloatKind(bt.Kind()):
		if !b.fOk {
			cc.failf("%s bound %q is not a number for %s field", token, b.s, bt)
		}
	case bt == timeType:
		if !b.tOk {
			cc.failf("%s bound %q is not an RFC3339 time for %s field", token, b.s, bt)
		}
	}
}

// makeLeafCheck returns the check for a leaf. The presence leaves (required, nil, zero
// and their negations) decide for themselves what an absent value means; every other leaf
// passes an absent value and otherwise defers to the leaf's predicate.
func makeLeafCheck(node *Node, art leafArtifact) check {
	expected := art.expected
	switch node.kind {
	case kindPresent, kindNotNil:
		return func(v reflect.Value, vc *validationCtx) bool {
			if _, ok := deref(v); ok {
				return true
			}
			return vc.fail(node, expected, nil)
		}
	case kindNil:
		return func(v reflect.Value, vc *validationCtx) bool {
			cv, ok := deref(v)
			if !ok {
				return true
			}
			return vc.failValue(node, expected, cv)
		}
	case kindNotZero:
		return func(v reflect.Value, vc *validationCtx) bool {
			cv, ok := deref(v)
			if !ok || cv.IsZero() {
				return vc.failValue(node, expected, cv)
			}
			return true
		}
	case kindZero:
		return func(v reflect.Value, vc *validationCtx) bool {
			cv, ok := deref(v)
			if !ok || cv.IsZero() {
				return true
			}
			return vc.failValue(node, expected, cv)
		}
	case kindFail:
		return func(v reflect.Value, vc *validationCtx) bool {
			cv, _ := deref(v)
			return vc.failValue(node, expected, cv)
		}
	default:
		pred := leafPredicate(node, art)
		return func(v reflect.Value, vc *validationCtx) bool {
			cv, ok := deref(v)
			if !ok {
				return true
			}
			if sat, kindOk := pred(cv); kindOk && sat {
				return true
			}
			return vc.failValue(node, expected, cv)
		}
	}
}

// leafPredicate returns the test for a value leaf. kindOk is false when the value's kind
// is one the leaf can't test, which for a concrete field was already a compile error.
func leafPredicate(node *Node, art leafArtifact) func(reflect.Value) (sat, kindOk bool) {
	switch node.kind {
	case kindCompare:
		op, b := node.name, art.params[0]
		return func(cv reflect.Value) (bool, bool) {
			order, ok := orderScalar(cv, b)
			if !ok {
				return false, false
			}
			return orderSatisfies(op, order), true
		}
	case kindBetween:
		lo, hi := art.params[0], art.params[1]
		return func(cv reflect.Value) (bool, bool) {
			loOrder, ok := orderScalar(cv, lo)
			if !ok {
				return false, false
			}
			hiOrder, hiOk := orderScalar(cv, hi)
			if !hiOk {
				return false, false
			}
			return loOrder >= 0 && hiOrder <= 0, true
		}
	case kindLength:
		minimum, maximum, hasMin, hasMax := art.lenMin, art.lenMax, art.hasMin, art.hasMax
		unit := node.unit
		return func(cv reflect.Value) (bool, bool) {
			n, ok := lengthOf(cv, unit)
			if !ok {
				return false, false
			}
			if hasMin && n < minimum {
				return false, true
			}
			if hasMax && n > maximum {
				return false, true
			}
			return true, true
		}
	case kindPattern:
		re := art.re
		return func(cv reflect.Value) (bool, bool) {
			if cv.Kind() != reflect.String {
				return false, false
			}
			return re.MatchString(cv.String()), true
		}
	case kindIsRegexp:
		return func(cv reflect.Value) (bool, bool) {
			if cv.Kind() != reflect.String {
				return false, false
			}
			_, err := regexp.Compile(cv.String())
			return err == nil, true
		}
	case kindOneOf:
		vals := art.params
		return func(cv reflect.Value) (bool, bool) {
			kindOk := false
			for i := range vals {
				if order, ok := orderScalar(cv, vals[i]); ok {
					kindOk = true
					if order == 0 {
						return true, true
					}
				}
			}
			return false, kindOk
		}
	case kindEquals, kindNotEquals:
		b, equals := art.params[0], node.kind == kindEquals
		return func(cv reflect.Value) (bool, bool) {
			order, ok := orderScalar(cv, b)
			if !ok {
				return false, false
			}
			return (order == 0) == equals, true
		}
	case kindTrue, kindFalse:
		want := node.kind == kindTrue
		return func(cv reflect.Value) (bool, bool) {
			if cv.Kind() != reflect.Bool {
				return false, false
			}
			return cv.Bool() == want, true
		}
	default:
		return func(reflect.Value) (bool, bool) { return false, false }
	}
}

// lengthOf counts a value's length in unit. A string counts UTF-8 bytes unless unit asks
// for code points or decoded base64 bytes; other kinds with a length count elements. ok
// is false for a kind with no length, or a string that isn't valid base64 when decoded
// bytes are asked for.
func lengthOf(cv reflect.Value, unit LengthUnit) (int, bool) {
	switch cv.Kind() {
	case reflect.String:
		s := cv.String()
		switch unit {
		case UnitCodePoints:
			return utf8.RuneCountInString(s), true
		case UnitDecodedBytes:
			decoded, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return 0, false
			}
			return len(decoded), true
		default:
			return len(s), true
		}
	case reflect.Slice, reflect.Array, reflect.Map, reflect.Chan:
		return cv.Len(), true
	default:
		return 0, false
	}
}

// scalar holds a static operand read into each numeric, boolean and time form it parses
// as, so comparing it to a value of any kind does no parsing at evaluation. Each Ok field
// reports whether that reading succeeded.
type scalar struct {
	s   string
	i   int64
	u   uint64
	f   float64
	b   bool
	t   time.Time
	iOk bool
	uOk bool
	fOk bool
	bOk bool
	tOk bool
}

func toScalar(v any) scalar {
	var sc scalar
	switch x := v.(type) {
	case nil:
		return sc
	case string:
		sc.s = x
	case int:
		sc.s = strconv.FormatInt(int64(x), 10)
	case int64:
		sc.s = strconv.FormatInt(x, 10)
	case uint64:
		sc.s = strconv.FormatUint(x, 10)
	case float64:
		sc.s = strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		sc.s = strconv.FormatBool(x)
	case time.Time:
		sc.s = x.Format(time.RFC3339)
	default:
		sc.s = reflect.ValueOf(x).String()
	}
	if n, err := strconv.ParseInt(sc.s, 10, 64); err == nil {
		sc.i, sc.iOk = n, true
	}
	if n, err := strconv.ParseUint(sc.s, 10, 64); err == nil {
		sc.u, sc.uOk = n, true
	}
	if f, err := strconv.ParseFloat(sc.s, 64); err == nil {
		sc.f, sc.fOk = f, true
	}
	if sc.s == "true" || sc.s == "false" {
		sc.b, sc.bOk = sc.s == "true", true
	}
	if tm, err := time.Parse(time.RFC3339, sc.s); err == nil {
		sc.t, sc.tOk = tm, true
	}
	return sc
}

// orderScalar compares a value to an operand, returning -1, 0 or 1. ok is false when the
// value's kind can't be compared to the operand. The comparison happens in the value's
// own domain, so no range or precision is lost.
func orderScalar(cv reflect.Value, b scalar) (int, bool) {
	switch {
	case isSignedKind(cv.Kind()):
		v := cv.Int()
		switch {
		case b.iOk:
			return cmp.Compare(v, b.i), true
		case b.fOk:
			return cmp.Compare(float64(v), b.f), true
		case b.uOk:
			if v < 0 {
				return -1, true
			}
			return cmp.Compare(uint64(v), b.u), true
		}
		return 0, false
	case isUnsignedKind(cv.Kind()):
		v := cv.Uint()
		switch {
		case b.uOk:
			return cmp.Compare(v, b.u), true
		case b.iOk:
			if b.i < 0 {
				return 1, true
			}
			return cmp.Compare(v, uint64(b.i)), true
		case b.fOk:
			return cmp.Compare(float64(v), b.f), true
		}
		return 0, false
	case isFloatKind(cv.Kind()):
		if !b.fOk {
			return 0, false
		}
		return cmp.Compare(cv.Float(), b.f), true
	case cv.Kind() == reflect.String:
		return strings.Compare(cv.String(), b.s), true
	case cv.Kind() == reflect.Bool:
		if !b.bOk {
			return 0, false
		}
		if cv.Bool() == b.b {
			return 0, true
		}
		return 1, true
	case cv.Kind() == reflect.Struct && cv.Type() == timeType:
		if !b.tOk {
			return 0, false
		}
		return cv.Interface().(time.Time).Compare(b.t), true
	}
	return 0, false
}

func orderSatisfies(op string, order int) bool {
	switch op {
	case "gt":
		return order > 0
	case "gte":
		return order >= 0
	case "lt":
		return order < 0
	case "lte":
		return order <= 0
	case "eq":
		return order == 0
	case "neq":
		return order != 0
	}
	return false
}

// baseType strips pointers from t. It returns nil for an interface or a nil type, which
// compile to dynamic checks.
func baseType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || t.Kind() == reflect.Interface {
		return nil
	}
	return t
}

func isSignedKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

func isUnsignedKind(k reflect.Kind) bool {
	switch k {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func isFloatKind(k reflect.Kind) bool {
	return k == reflect.Float32 || k == reflect.Float64
}

// ifaceOf reads a value as an any for a failure's ToTest. It is called only when a
// failure is reported, so a passing value is never boxed.
func ifaceOf(cv reflect.Value) any {
	if cv.IsValid() && cv.CanInterface() {
		return cv.Interface()
	}
	return nil
}

var timeType = reflect.TypeFor[time.Time]()

// anyScope names the tag section that applies when no section names the current scope.
// It matches the structs package's wildcard scope.
const anyScope = "*"

// tagKey is the struct tag a field's directive is read from.
const tagKey = "validate"

// fieldCheck is one field's entry in a struct check: the field index, the target a
// failure reports, and the compiled check. An embedded entry has no target, so the
// embedded struct's fields report as if they were the parent's.
type fieldCheck struct {
	index  int
	target string
	check  check
	embed  bool
}

// structCheck returns the check for struct type t in scope. A type that refers to itself,
// directly or through another type, gets a thunk that calls the check once it is built.
// The thunk is only called at evaluation, after compilation has finished.
func (cc *compileCtx) structCheck(t reflect.Type, scope string) check {
	bt := derefStructType(t)
	if bt == nil {
		cc.failf("struct validation requires a struct type, found %s", t)
		return nil
	}
	if cc.inProgress == nil {
		cc.inProgress = map[structKey]*check{}
	}

	key := structKey{bt, scope}
	if slot, ok := cc.inProgress[key]; ok {
		return func(v reflect.Value, vc *validationCtx) bool { return (*slot)(v, vc) }
	}

	slot := new(check)
	cc.inProgress[key] = slot
	*slot = cc.buildStructCheck(bt, scope)
	return *slot
}

// buildStructCheck compiles each exported field's directive for scope, plus any directive
// on a blank (_) field, which checks the struct as a whole.
func (cc *compileCtx) buildStructCheck(bt reflect.Type, scope string) check {
	savedEnc := cc.enclosing
	cc.enclosing = bt
	defer func() { cc.enclosing = savedEnc }()

	var fields []fieldCheck
	var structLevel []check

	for i := 0; i < bt.NumField(); i++ {
		sf := bt.Field(i)
		directive, hasDirective := "", false
		if tagText, ok := sf.Tag.Lookup(tagKey); ok {
			directive, hasDirective = scopeDirective(tagText, scope)
		}

		if sf.Name == "_" {
			if hasDirective && directive != "" {
				if chk := cc.compileFieldNode(directive, bt); chk != nil {
					structLevel = append(structLevel, chk)
				}
			}
			continue
		}
		if !sf.IsExported() {
			continue
		}

		// An embedded struct's fields are promoted, so they are always validated as part
		// of this struct. A directive on the embedded field applies to it as well.
		if sf.Anonymous && derefStructType(sf.Type) != nil {
			embedded := fieldCheck{index: i, check: cc.structCheck(sf.Type, scope), embed: true}
			fields = append(fields, embedded)
		}
		if !hasDirective || directive == "" {
			continue
		}

		chk := cc.compileFieldNode(directive, sf.Type)
		if chk == nil {
			continue
		}
		target := sf.Name
		if targetNamer != nil {
			target = targetNamer(bt, sf)
		}
		fields = append(fields, fieldCheck{index: i, target: target, check: chk})
	}

	return func(v reflect.Value, vc *validationCtx) bool {
		sv, ok := deref(v)
		if !ok {
			return true
		}
		if sv.Kind() != reflect.Struct {
			return vc.failValue(nil, "a struct", sv)
		}
		savedRoot := vc.root
		vc.root = sv
		satisfied := true
		for i := range fields {
			fc := &fields[i]
			fv := sv.Field(fc.index)
			if fc.embed {
				if !fc.check(fv, vc) {
					satisfied = false
				}
			} else {
				n := vc.push(fc.target)
				if !fc.check(fv, vc) {
					satisfied = false
				}
				vc.truncate(n)
			}
			if vc.done() {
				break
			}
		}
		for _, c := range structLevel {
			if !c(sv, vc) {
				satisfied = false
			}
			if vc.done() {
				break
			}
		}
		vc.root = savedRoot
		return satisfied
	}
}

func (cc *compileCtx) compileFieldNode(directive string, ft reflect.Type) check {
	node, ge := parseDirective(directive)
	if ge != nil {
		cc.fail(gomerr.Configuration("cannot parse validate directive").Wrap(ge).
			AddAttribute("directive", directive))
		return nil
	}
	return cc.compileNode(&node, ft)
}

// scopeRegexp matches one "[<scope>:]<directive>" section of a scoped tag. It repeats the
// pattern the structs package uses, which structs doesn't export.
var scopeRegexp = regexp.MustCompile(`(?:([^;:]*[^\\]):)?([^;]*)`)

// scopeDirective selects the section of a tag that applies to scope. A tag's format is
// [<scope>:]<directive>[;[<scope>:]<directive>]*, where a section without a scope applies
// when no section names scope. A section's scope may be an alias registered with
// structs.ScopeAlias, such as create for resource.CreateAction. The second return is false
// when no section applies, so the field isn't validated in scope.
func scopeDirective(tagText, scope string) (string, bool) {
	if !strings.ContainsAny(tagText, ";:") {
		return tagText, true
	}

	wildcard := ""
	hasWildcard := false
	for _, m := range scopeRegexp.FindAllStringSubmatch(tagText, -1) {
		if m[0] == "" {
			continue
		}
		section := m[1]
		if section == "" {
			section = anyScope
		} else {
			section = structs.ResolveScope(section)
		}
		directive := strings.ReplaceAll(m[2], `\:`, ":")
		if section == scope {
			return directive, true
		}
		if section == anyScope {
			wildcard, hasWildcard = directive, true
		}
	}
	return wildcard, hasWildcard
}

// fieldRef is a $.Field operand resolved at compile time: the index of the referenced
// field in the struct under test, and its type. An operand naming a missing field is a
// configuration error.
type fieldRef struct {
	index []int
	typ   reflect.Type
}

// compileLeafRef compiles a leaf with a $.Field operand. For the presence leaves the
// operand replaces the subject, so nil($.Min) tests whether Min is set. For the
// comparison leaves the operand is a bound read from the struct under test, as in
// gte($.Min). A bound whose field is absent skips the comparison, so a missing optional
// field doesn't fail a cross-field rule.
func (cc *compileCtx) compileLeafRef(node *Node) check {
	expected := node.String()

	switch node.kind {
	case kindNil, kindNotNil, kindPresent, kindZero, kindNotZero:
		if len(node.params) != 1 || !node.params[0].isRef() {
			cc.failf("%s with a field reference takes exactly one $.Field operand", node.token())
			return nil
		}
		fr, ok := cc.resolveRef(node.params[0].Ref)
		if !ok {
			return nil
		}
		kind := node.kind
		return func(_ reflect.Value, vc *validationCtx) bool {
			cv, present := vc.refValue(fr)
			switch kind {
			case kindNil:
				if !present {
					return true
				}
				return vc.failValue(node, expected, cv)
			case kindNotNil, kindPresent:
				if present {
					return true
				}
				return vc.fail(node, expected, nil)
			case kindZero:
				if !present || cv.IsZero() {
					return true
				}
				return vc.failValue(node, expected, cv)
			default: // kindNotZero
				if present && !cv.IsZero() {
					return true
				}
				return vc.failValue(node, expected, cv)
			}
		}
	case kindCompare:
		return cc.compileCompareRef(node, expected)
	case kindBetween:
		return cc.compileBetweenRef(node, expected)
	case kindOneOf, kindEquals, kindNotEquals:
		return cc.compileEqualityRef(node, expected)
	default:
		cc.failf("%s does not accept a field reference", node.token())
		return nil
	}
}

// compileCompareRef compiles a comparison with a $.Field operand. With one operand, the
// field under test is compared to a bound read from another field. With two, the first
// operand names the field to compare and the second is the bound.
func (cc *compileCtx) compileCompareRef(node *Node, expected string) check {
	op := node.name
	switch len(node.params) {
	case 1:
		bound, ok := cc.dynamicOperand(node.params[0])
		if !ok {
			return nil
		}
		return func(v reflect.Value, vc *validationCtx) bool {
			cv, present := deref(v)
			if !present {
				return true
			}
			bs, bok := bound.scalar(vc)
			if !bok {
				return true
			}
			order, kindOk := orderScalar(cv, bs)
			if kindOk && orderSatisfies(op, order) {
				return true
			}
			return vc.failValue(node, expected, cv)
		}
	case 2:
		if !node.params[0].isRef() {
			cc.failf("%s with two operands expects a $.Field subject", op)
			return nil
		}
		subj, ok := cc.resolveRef(node.params[0].Ref)
		bound, bok := cc.dynamicOperand(node.params[1])
		if !ok || !bok {
			return nil
		}
		return func(_ reflect.Value, vc *validationCtx) bool {
			sv, present := vc.refValue(subj)
			if !present {
				return true
			}
			bs, bPresent := bound.scalar(vc)
			if !bPresent {
				return true
			}
			order, kindOk := orderScalar(sv, bs)
			if kindOk && orderSatisfies(op, order) {
				return true
			}
			return vc.failValue(node, expected, sv)
		}
	default:
		cc.failf("%s with a field reference takes one or two operands, found %d",
			op, len(node.params))
		return nil
	}
}

func (cc *compileCtx) compileBetweenRef(node *Node, expected string) check {
	if len(node.params) != 2 {
		cc.failf("between takes a lower and an upper operand, found %d", len(node.params))
		return nil
	}
	lo, loOk := cc.dynamicOperand(node.params[0])
	hi, hiOk := cc.dynamicOperand(node.params[1])
	if !loOk || !hiOk {
		return nil
	}
	return func(v reflect.Value, vc *validationCtx) bool {
		cv, present := deref(v)
		if !present {
			return true
		}
		loS, loPresent := lo.scalar(vc)
		hiS, hiPresent := hi.scalar(vc)
		if !loPresent || !hiPresent {
			return true
		}
		loOrder, ok1 := orderScalar(cv, loS)
		hiOrder, ok2 := orderScalar(cv, hiS)
		if ok1 && ok2 && loOrder >= 0 && hiOrder <= 0 {
			return true
		}
		return vc.failValue(node, expected, cv)
	}
}

func (cc *compileCtx) compileEqualityRef(node *Node, expected string) check {
	operands := make([]dynamicOperand, len(node.params))
	for i := range node.params {
		op, ok := cc.dynamicOperand(node.params[i])
		if !ok {
			return nil
		}
		operands[i] = op
	}
	kind := node.kind
	return func(v reflect.Value, vc *validationCtx) bool {
		cv, present := deref(v)
		if !present {
			return true
		}
		switch kind {
		case kindEquals, kindNotEquals:
			bs, bok := operands[0].scalar(vc)
			if !bok {
				return true
			}
			order, kindOk := orderScalar(cv, bs)
			if !kindOk {
				return vc.failValue(node, expected, cv)
			}
			if (kind == kindEquals) == (order == 0) {
				return true
			}
			return vc.failValue(node, expected, cv)
		default: // kindOneOf
			for i := range operands {
				bs, bok := operands[i].scalar(vc)
				if !bok {
					continue
				}
				if order, kindOk := orderScalar(cv, bs); kindOk && order == 0 {
					return true
				}
			}
			return vc.failValue(node, expected, cv)
		}
	}
}

func (cc *compileCtx) dynamicOperand(op operand) (dynamicOperand, bool) {
	if op.isRef() {
		fr, ok := cc.resolveRef(op.Ref)
		if !ok {
			return dynamicOperand{}, false
		}
		return dynamicOperand{isRef: true, ref: fr}, true
	}
	return dynamicOperand{static: toScalar(op.Value)}, true
}

// resolveRef resolves a $.Field operand against the enclosing struct. A reference outside
// a struct compilation, or one that names no field, is a configuration error.
func (cc *compileCtx) resolveRef(ref string) (fieldRef, bool) {
	if cc.enclosing == nil {
		cc.failf("operand %s is a field reference, which requires whole-struct compilation", ref)
		return fieldRef{}, false
	}
	name := strings.TrimPrefix(ref, "$.")
	sf, ok := cc.enclosing.FieldByName(name)
	if !ok {
		cc.failf("field reference %s names no field of %s", ref, cc.enclosing)
		return fieldRef{}, false
	}
	return fieldRef{index: sf.Index, typ: sf.Type}, true
}

// dynamicOperand is a comparison operand: a static scalar, or a $.Field read from the
// struct under test at each evaluation.
type dynamicOperand struct {
	isRef  bool
	static scalar
	ref    fieldRef
}

// scalar returns the operand's value. present is false when a referenced field is absent
// or has a kind with no scalar form, which the comparison treats as a skip. A referenced
// value is read without boxing, except for a time.
func (d dynamicOperand) scalar(vc *validationCtx) (sc scalar, present bool) {
	if !d.isRef {
		return d.static, true
	}
	v, present := vc.refValue(d.ref)
	if !present {
		return scalar{}, false
	}
	switch {
	case isSignedKind(v.Kind()):
		n := v.Int()
		sc.i, sc.iOk = n, true
		sc.f, sc.fOk = float64(n), true
		if n >= 0 {
			sc.u, sc.uOk = uint64(n), true
		}
	case isUnsignedKind(v.Kind()):
		n := v.Uint()
		sc.u, sc.uOk = n, true
		sc.f, sc.fOk = float64(n), true
		if n <= 1<<63-1 {
			sc.i, sc.iOk = int64(n), true
		}
	case isFloatKind(v.Kind()):
		sc.f, sc.fOk = v.Float(), true
	case v.Kind() == reflect.String:
		sc.s = v.String()
	case v.Kind() == reflect.Bool:
		sc.b, sc.bOk = v.Bool(), true
	case v.Kind() == reflect.Struct && v.Type() == timeType:
		sc.t, sc.tOk = v.Interface().(time.Time), true
	default:
		return scalar{}, false
	}
	return sc, true
}

// refValue reads a referenced field from the struct under test. present is false when the
// field is absent: a nil pointer, interface, slice or map.
func (vc *validationCtx) refValue(fr fieldRef) (reflect.Value, bool) {
	if !vc.root.IsValid() {
		return reflect.Value{}, false
	}
	return deref(vc.root.FieldByIndex(fr.index))
}
