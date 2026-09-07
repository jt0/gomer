// Package constraint provides a struct-tag-driven validation framework.
//
// Constraints are declared in "validate" struct tags and automatically applied when a struct
// is validated. The system supports composable constraints, logical operators, and cross-field
// references.
//
// # Basic Usage
//
//	type User struct {
//	    Name  string `validate:"len(1,64)"`
//	    Email string `validate:"required,regexp(^.+@.+$)"`
//	    Age   int    `validate:"int(gte,0),int(lte,150)"`
//	}
//
//	err := constraint.Validate(&user, constraint.DefaultValidationTool)
//
// # Constraint Types
//
// Simple constraints take no parameters and are looked up by name:
//
//	required, nil, notnil, zero, notzero, true, false
//
// Parameterized constraints are built by functions that accept typed arguments:
//
//	len(1,64)           // length between 1 and 64
//	minlen(1)           // non-empty
//	int(gte,0)          // integer >= 0
//	oneof(a,b,c)        // value is one of a, b, or c
//	regexp(^[a-z]+$)    // matches regex
//	elements(required)  // each element satisfies "required"
//
// # Nil-ness vs. Zero-ness
//
// These ask about different things, and reaching for the wrong one is a common source of bugs.
//
// "nil" and "notnil" ask about pointer types: does it point at anything at all? The answer comes
// from the reference alone - the pointed-at value is never read - so a pointer to 0, or to "",
// is not nil. Types that cannot be nil (int, string, a struct) are Unprocessable rather than
// satisfying either.
//
// "zero" and "notzero" ask about the value: is it the zero value for its type? Reaching it means
// following the reference, so a pointer to 0 is zero even though it is not nil. A reference that
// points at nothing has no value to inspect and is treated as zero.
//
// The two only disagree when a pointer is involved:
//
//	value           nil    notnil  zero  notzero
//	(*int)(nil)     yes    no      yes   no
//	new(0)          no     yes     yes   no      // not nil, but zero
//	new(42)         no     yes     no    yes
//	0               error  error   yes   no
//	42              error  error   no    yes
//
// "required" is an alias for "notzero", so it rejects 0 and false. That makes it the wrong choice
// whenever zero is a legal value. To require that a value was supplied while still accepting zero
// ones, make the field a pointer and ask about the reference instead:
//
//	Count *int `validate:"notnil"`   // 0 accepted, omitted rejected
//	Count int  `validate:"required"` // 0 rejected
//
// A non-pointer field cannot express this distinction at all: an int field holding 0 is
// indistinguishable from one that was never set, so no constraint can tell them apart.
//
// # Logical Operators
//
// Constraints can be composed with and, or, and not:
//
//	or(nil,len(1,100))       // nil OR length between 1 and 100
//	not(zero)                // not zero
//	and(required,len(1,10))  // required AND length 1-10 (same as "required,len(1,10)")
//
// # Dynamic Parameters (Cross-Field References)
//
// Parameters prefixed with "$." reference other fields in the same struct. The referenced
// field's value is resolved at validation time:
//
//	type Range struct {
//	    Min int `validate:"int(gte,0)"`
//	    Max int `validate:"int(gte,$.Min)"`  // Max must be >= Min
//	}
//
// Any builder whose parameter type is a pointer (e.g. *int64, *any) supports dynamic
// references. See the builders map in registry.go for which constraints support this.
//
// # The field() Constraint
//
// For cross-field comparisons that don't fit the pattern of testing the current field,
// use field() to test another field's value directly:
//
//	// CreateMode is required when AccessMode is "read_write"
//	CreateMode *string `validate:"or(field($.AccessMode,neq,read_write),required)"`
//
// # Custom Constraints
//
// Register custom constraints or builders with Register(). Custom names must start with '$':
//
//	constraint.Register("$mycheck", func(value *int64) constraint.Constraint {
//	    return constraint.New("mycheck", value, func(toTest any) gomerr.Gomerr {
//	        // validation logic
//	    })
//	})
package constraint
