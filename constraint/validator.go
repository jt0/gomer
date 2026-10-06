package constraint

import (
	"reflect"
	"sync"

	"github.com/jt0/gomer/bind"
	"github.com/jt0/gomer/gomerr"
)

// A Validator checks values of one Go type against a compiled node. It is immutable and
// safe for concurrent use. A passing value allocates nothing; each reported failure
// allocates one NotSatisfiedError.
type Validator struct {
	root check
	typ  reflect.Type
}

// NewValidator returns the Validator for struct type t's validate tags in scope. It
// checks every field in field order and descends into nested structs through the struct
// and union kinds. Validators are cached by type and scope, so concurrent first uses
// compile once and share the result.
func NewValidator(t reflect.Type, scope string) (*Validator, gomerr.Gomerr) {
	if scope == "" {
		scope = anyScope
	}

	key := structKey{t, scope}
	if vr, ok := validatorCache.Load(key); ok {
		return vr.(*Validator), nil
	}

	validatorMu.Lock()
	defer validatorMu.Unlock()
	if vr, ok := validatorCache.Load(key); ok {
		return vr.(*Validator), nil
	}

	cc := &compileCtx{scope: scope, inProgress: map[structKey]*check{}}
	root := cc.structCheck(t, scope)
	if ge := cc.errors.GomerrOrNil(); ge != nil {
		return nil, ge
	}

	vr := &Validator{root: root}
	validatorCache.Store(key, vr)
	return vr, nil
}

type structKey struct {
	t     reflect.Type
	scope string
}

// validatorCache holds struct Validators by type and scope. validatorMu serializes
// compilation, so a recursive type resolves through compileCtx.inProgress without another
// goroutine seeing a half-built Validator.
var (
	validatorCache sync.Map // structKey -> *Validator
	validatorMu    sync.Mutex
)

// Validate checks v and returns nil, a single NotSatisfiedError, or a batch of them.
func (vr *Validator) Validate(v any) gomerr.Gomerr {
	return vr.ValidateAt("", v)
}

// ValidateAt is Validate with failures targeted at target, for a single value checked
// outside a struct, such as a query parameter named "name".
func (vr *Validator) ValidateAt(target string, v any) gomerr.Gomerr {
	rv := reflect.ValueOf(v)
	if vr.typ != nil && vr.typ.Kind() != reflect.Interface && rv.IsValid() &&
		!rv.Type().AssignableTo(vr.typ) {
		return gomerr.Unprocessable("value type does not match the compiled validator",
			rv.Type().String()).AddAttribute("expected", vr.typ.String())
	}

	vc := ctxPool.Get().(*validationCtx)
	vc.path = append(vc.path[:0], target...)
	vc.errors = gomerr.ErrorBatch{}
	vc.report = true
	vc.reported = 0
	vc.root = reflect.Value{}

	vr.root(rv, vc)
	ge := vc.errors.GomerrOrNil()

	ctxPool.Put(vc)
	return ge
}

// Validate checks v, a struct or pointer to one, against its validate tags in scope. An
// empty scope selects each field's unscoped section.
func Validate(v any, scope string) gomerr.Gomerr {
	vr, ge := NewValidator(reflect.TypeOf(v), scope)
	if ge != nil {
		return ge
	}
	return vr.Validate(v)
}

// TargetNamer renders a field's NotSatisfiedError.Target. The default target is the
// field's name.
type TargetNamer func(reflect.Type, reflect.StructField) string

// targetNamer is read only during compilation, while validatorMu is held.
var targetNamer TargetNamer

// SetTargetNamer sets how failures name a struct's fields in Validators compiled
// afterward, and discards the Validators already cached so none keeps the old names.
func SetTargetNamer(namer TargetNamer) {
	validatorMu.Lock()
	defer validatorMu.Unlock()
	targetNamer = namer
	validatorCache.Clear()
}

// CamelCaseTargetNamer renders targets in camelCase.
var CamelCaseTargetNamer TargetNamer = func(_ reflect.Type, sf reflect.StructField) string {
	return bind.CamelCaseFn(sf.Name)
}

// failureBudget caps how many failures one Validate reports. It matches the error
// batch's own cap.
const failureBudget = 100

// ctxPool reuses validation state across calls, so a passing Validate allocates nothing.
// A pooled validationCtx keeps only its path buffer between uses.
var ctxPool = sync.Pool{New: func() any { return &validationCtx{} }}

// validationCtx carries the state of one Validate: the target of the current check
// (path), the failures so far (errors), and whether failures are being reported. report
// is false inside an or branch, so a failing branch costs nothing.
type validationCtx struct {
	path     []byte
	errors   gomerr.ErrorBatch
	report   bool
	reported int
	// root is the struct whose fields a $.Field operand reads: the struct under
	// validation, or a nested struct while its own fields run. It is invalid when
	// validating a single value.
	root reflect.Value
	// customCtx is the CustomContext a custom check is handed. Keeping it here and
	// passing a pointer means handing it to a check allocates nothing.
	customCtx customCtx
}

// done reports whether the failure budget is spent, so containers can stop iterating.
func (vc *validationCtx) done() bool {
	return vc.reported >= failureBudget
}

// fail records a failure of node against value and returns false, so a check can return
// its result. Inside an or branch it records nothing.
func (vc *validationCtx) fail(node *Node, expected string, value any) bool {
	if !vc.report || vc.done() {
		return false
	}
	nse := NotSatisfied(value)
	nse.Target = string(vc.path)
	nse.Node = *node
	nse.Expected = expected
	vc.errors.Capture(nse)
	vc.reported++
	return false
}

// failValue is fail for a reflect.Value. It boxes the value only when the failure is
// recorded, so a failing check inside an or branch doesn't allocate.
func (vc *validationCtx) failValue(node *Node, expected string, cv reflect.Value) bool {
	if !vc.report || vc.done() {
		return false
	}
	return vc.fail(node, expected, ifaceOf(cv))
}

// push appends a field name or map key to the target path and returns the length to
// truncate back to once the contained check returns.
func (vc *validationCtx) push(name string) int {
	n := len(vc.path)
	if n > 0 {
		vc.path = append(vc.path, '.')
	}
	vc.path = append(vc.path, name...)
	return n
}

func (vc *validationCtx) truncate(n int) {
	vc.path = vc.path[:n]
}

// customCtx is the CustomContext handed to a custom check. reported counts the check's
// Report calls: a check that reported its own failures gets no field-level failure.
type customCtx struct {
	vc       *validationCtx
	node     *Node
	reported int
}

// Enclosing returns the struct that contains the field under test. ok is false when
// validating a single value. The struct is boxed only when Enclosing is called.
func (cx *customCtx) Enclosing() (any, bool) {
	root := cx.vc.root
	if root.IsValid() && root.CanInterface() {
		return root.Interface(), true
	}
	return nil, false
}

// Report records a failure at target, nested under the field's target. Like any failure,
// it is dropped inside an or branch or once the failure budget is spent, but it still
// counts toward reported.
func (cx *customCtx) Report(target, expected string) {
	cx.reported++
	vc := cx.vc
	if !vc.report || vc.done() {
		return
	}
	n := vc.push(target)
	vc.fail(cx.node, expected, nil)
	vc.truncate(n)
}

// deref follows pointers and interfaces to the concrete value. ok is false for an absent
// value: a nil pointer, interface, slice or map.
func deref(v reflect.Value) (reflect.Value, bool) {
	for v.IsValid() {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		case reflect.Slice, reflect.Map:
			if v.IsNil() {
				return reflect.Value{}, false
			}
			return v, true
		default:
			return v, true
		}
	}
	return reflect.Value{}, false
}
