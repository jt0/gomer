package constraint

import (
	"reflect"
	"testing"

	"github.com/jt0/gomer/gomerr"
)

func TestRequiredConstraint(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		wantError bool
	}{
		{"nil value", nil, true},
		{"nil pointer", (*string)(nil), true},
		{"zero int", 0, true},
		{"non-zero int", 42, false},
		{"zero string", "", true},
		{"non-zero string", "hello", false},
		{"false bool", false, true},
		{"true bool", true, false},
		{"empty slice", []string{}, false}, // empty but allocated is not zero
		{"non-empty slice", []string{"a"}, false},
		{"nil slice", []string(nil), true},
		{"empty map", map[string]string{}, false}, // empty but allocated is not zero
		{"non-empty map", map[string]string{"k": "v"}, false},
		{"nil map", map[string]string(nil), true},
		{"zero struct", struct{ Name string }{}, true},
		{"non-zero struct", struct{ Name string }{"test"}, false},
		{"pointer to zero", new(""), true},
		{"pointer to non-zero", new("hello"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := IsRequired.Test(tt.value)
			if tt.wantError && err == nil {
				t.Errorf("Expected error but got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestRequiredFunc(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		wantError bool
	}{
		{"nil value", nil, true},
		{"zero string", "", true},
		{"non-zero string", "hello", false},
		{"zero int", 0, true},
		{"non-zero int", 42, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := tt.value
			c := Required(&v)
			err := c.Test(nil) // Test arg ignored, uses captured value
			if tt.wantError && err == nil {
				t.Errorf("Expected error but got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

// nilCases enumerates the nil-ness of each value under nilConstraint's semantics. Unlike the zero
// constraints, the nil constraints ask about the reference itself and never look through it, so
// every non-nil pointer is not nil regardless of what it points at. Types that cannot be nil are
// Unprocessable rather than satisfying either constraint.
var nilCases = []struct {
	name  string
	value any
	// wantNil is meaningful only when notNilable is false.
	wantNil    bool
	notNilable bool
}{
	{name: "untyped nil", value: nil, wantNil: true},
	{name: "nil interface", value: Constraint(nil), wantNil: true},
	{name: "non-nil interface", value: IsNil, wantNil: false},

	{name: "nil pointer", value: (*struct{})(nil), wantNil: true},
	{name: "non-nil pointer", value: &struct{}{}, wantNil: false},
	// The reference is not nil even though it points at a zero value; contrast with zeroCases.
	{name: "pointer to zero int", value: new(0), wantNil: false},
	{name: "nil pointer to pointer", value: (**string)(nil), wantNil: true},
	{name: "pointer to nil pointer", value: new((*string)(nil)), wantNil: false},

	{name: "nil slice", value: []struct{}(nil), wantNil: true},
	{name: "empty slice", value: []struct{}{}, wantNil: false},
	{name: "nil map", value: map[string]string(nil), wantNil: true},
	{name: "empty map", value: map[string]string{}, wantNil: false},
	{name: "nil chan", value: (chan int)(nil), wantNil: true},
	{name: "non-nil chan", value: make(chan int), wantNil: false},
	{name: "nil func", value: (func())(nil), wantNil: true},
	{name: "non-nil func", value: func() {}, wantNil: false},

	{name: "int is not nil-able", value: 42, notNilable: true},
	{name: "string is not nil-able", value: "hello", notNilable: true},
	{name: "struct is not nil-able", value: struct{}{}, notNilable: true},
}

// TestNilConstraint asserts IsNil and IsNotNil over the same table so the two remain exact mirrors
// of each other, except for non-nil-able types, which satisfy neither.
func TestNilConstraint(t *testing.T) {
	for _, tt := range nilCases {
		t.Run(tt.name, func(t *testing.T) {
			nilErr, notNilErr := IsNil.Test(tt.value), IsNotNil.Test(tt.value)
			if tt.notNilable {
				if nilErr == nil || notNilErr == nil {
					t.Errorf("expected both to error, got IsNil=%v IsNotNil=%v", nilErr, notNilErr)
				}
				return
			}
			if tt.wantNil != (nilErr == nil) {
				t.Errorf("IsNil: wantNil=%v but got err=%v", tt.wantNil, nilErr)
			}
			if tt.wantNil == (notNilErr == nil) {
				t.Errorf("IsNotNil: wantNil=%v but got err=%v", tt.wantNil, notNilErr)
			}
		})
	}
}

// TestNilFuncs covers the Nil and NotNil wrappers, which back the nil($.Other) and notnil($.Other)
// tag forms. These previously tested the *any holding the value rather than the value itself,
// making Nil unsatisfiable and NotNil always satisfied for every input.
func TestNilFuncs(t *testing.T) {
	for _, tt := range nilCases {
		t.Run(tt.name, func(t *testing.T) {
			v := tt.value
			nilErr, notNilErr := Nil(&v).Test(nil), NotNil(&v).Test(nil)
			if tt.notNilable {
				if nilErr == nil || notNilErr == nil {
					t.Errorf("expected both to error, got Nil=%v NotNil=%v", nilErr, notNilErr)
				}
				return
			}
			if tt.wantNil != (nilErr == nil) {
				t.Errorf("Nil: wantNil=%v but got err=%v", tt.wantNil, nilErr)
			}
			if tt.wantNil == (notNilErr == nil) {
				t.Errorf("NotNil: wantNil=%v but got err=%v", tt.wantNil, notNilErr)
			}
		})
	}
}

// TestReportedConstraintName pins the name a violation is reported under, asserting that the
// parameterless form and the dynamic ($.Field) form of each constraint report the *same* name. Two
// things are required for that to hold: the names in value.go carry no "is" prefix, matching the
// tag spelling; and the dynamic forms share a predicate with their counterparts rather than
// delegating to them, since constraint.Test keeps the innermost name it finds and delegating would
// therefore report the delegate's name.
//
// The shared name is what logic.go keys on to recognize the or(nil,...) and or(zero,...)
// optional-field bypass, which must work for both forms.
func TestReportedConstraintName(t *testing.T) {
	var zeroV any = 0
	var notZeroV any = 42
	var nilV any = (*string)(nil)
	var notNilV any = new("x")
	var trueV any = true
	var falseV any = false

	tests := []struct {
		want string
		// parameterless is violated by violates; dynamic is pre-bound to a violating value.
		parameterless Constraint
		violates      any
		dynamic       Constraint
	}{
		{"zero", IsZero, 42, Zero(&notZeroV)},
		{"notZero", IsNotZero, 0, NotZero(&zeroV)},
		{"required", IsRequired, 0, Required(&zeroV)},
		{"nil", IsNil, new("x"), Nil(&notNilV)},
		{"notNil", IsNotNil, (*string)(nil), NotNil(&nilV)},
		{"true", IsTrue, false, True(&falseV)},
		{"false", IsFalse, true, False(&trueV)},
	}

	reportedName := func(t *testing.T, c Constraint, toTest any) string {
		t.Helper()
		ge := c.Test(toTest)
		nse := gomerr.ErrorAs[*NotSatisfiedError](ge)
		if nse == nil {
			t.Fatalf("expected a NotSatisfiedError, got %v", ge)
		}
		return nse.Constraint.Type()
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := reportedName(t, tt.parameterless, tt.violates); got != tt.want {
				t.Errorf("parameterless form reported %q, want %q", got, tt.want)
			}
			if got := reportedName(t, tt.dynamic, nil); got != tt.want {
				t.Errorf("dynamic form reported %q, want %q", got, tt.want)
			}
		})
	}
}

// zeroCases enumerates the zero-ness of each value under zeroConstraint's semantics. Unlike the nil
// constraints, the zero constraints look *through* pointers: a non-nil pointer is zero when the
// value it points at is zero. Values that hold nothing readable at all - an untyped nil or a nil
// pointer - are treated as zero.
var zeroCases = []struct {
	name     string
	value    any
	wantZero bool
}{
	// No readable value at all.
	{"untyped nil", nil, true},
	{"nil pointer", (*string)(nil), true},
	{"nil pointer to pointer", (**string)(nil), true},
	{"pointer to nil pointer", new((*string)(nil)), true},
	{"typed nil in interface", any((*int)(nil)), true},

	// Direct values.
	{"zero int", 0, true},
	{"non-zero int", 42, false},
	{"zero string", "", true},
	{"non-zero string", "hello", false},
	{"false bool", false, true},
	{"true bool", true, false},
	{"zero struct", struct{ Name string }{}, true},
	{"non-zero struct", struct{ Name string }{"test"}, false},

	// Pointers are dereferenced, so pointer-to-zero is zero. This is what distinguishes the zero
	// constraints from the nil constraints, for which every non-nil pointer is not nil.
	{"pointer to zero int", new(0), true},
	{"pointer to non-zero int", new(42), false},
	{"pointer to zero string", new(""), true},
	{"pointer to non-zero string", new("hello"), false},
	{"pointer to false bool", new(false), true},
	{"pointer to true bool", new(true), false},
	{"pointer to zero struct", &struct{ Name string }{}, true},
	{"pointer to non-zero struct", &struct{ Name string }{"test"}, false},
	{"pointer to pointer to non-zero int", new(new(42)), false},

	// Nil-able kinds are zero only when nil; allocated-but-empty is not zero.
	{"nil slice", []string(nil), true},
	{"empty slice", []string{}, false},
	{"non-empty slice", []string{"a"}, false},
	{"nil map", map[string]string(nil), true},
	{"empty map", map[string]string{}, false},
	{"non-empty map", map[string]string{"k": "v"}, false},
	{"nil chan", (chan int)(nil), true},
	{"non-nil chan", make(chan int), false},
	{"nil func", (func())(nil), true},
	{"non-nil func", func() {}, false},

	// A reflect.Value is unwrapped rather than inspected as a struct.
	{"reflect.Value of zero int", reflect.ValueOf(0), true},
	{"reflect.Value of non-zero int", reflect.ValueOf(42), false},
	{"reflect.Value of pointer to zero int", reflect.ValueOf(new(0)), true},
	{"reflect.Value of pointer to non-zero int", reflect.ValueOf(new(42)), false},
}

// TestZeroConstraint asserts IsZero and IsNotZero over the same table so the two remain exact
// mirrors of each other.
func TestZeroConstraint(t *testing.T) {
	for _, tt := range zeroCases {
		t.Run(tt.name, func(t *testing.T) {
			if err := IsZero.Test(tt.value); tt.wantZero != (err == nil) {
				t.Errorf("IsZero: wantZero=%v but got err=%v", tt.wantZero, err)
			}
			if err := IsNotZero.Test(tt.value); tt.wantZero == (err == nil) {
				t.Errorf("IsNotZero: wantZero=%v but got err=%v", tt.wantZero, err)
			}
		})
	}
}

// TestIsRequiredMatchesIsNotZero pins IsRequired to IsNotZero. IsRequired is deliberately a
// zero-check and not a presence check, so a supplied-but-zero value does not satisfy it.
func TestIsRequiredMatchesIsNotZero(t *testing.T) {
	for _, tt := range zeroCases {
		t.Run(tt.name, func(t *testing.T) {
			if err := IsRequired.Test(tt.value); tt.wantZero == (err == nil) {
				t.Errorf("IsRequired: wantZero=%v but got err=%v", tt.wantZero, err)
			}
		})
	}
}

func TestZeroFuncs(t *testing.T) {
	for _, tt := range zeroCases {
		t.Run(tt.name, func(t *testing.T) {
			v := tt.value
			if err := Zero(&v).Test(nil); tt.wantZero != (err == nil) {
				t.Errorf("Zero: wantZero=%v but got err=%v", tt.wantZero, err)
			}
			if err := NotZero(&v).Test(nil); tt.wantZero == (err == nil) {
				t.Errorf("NotZero: wantZero=%v but got err=%v", tt.wantZero, err)
			}
		})
	}
}
