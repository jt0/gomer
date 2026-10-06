// Package constraint validates structs against constraints declared in "validate" struct
// tags.
//
// A tag is parsed into a Node, a tree of checks that is plain data, and the Node is
// compiled against the field's type into a Validator. A Validator specializes each check
// for its type once, so validating a value does no tag parsing and, when the value
// passes, allocates nothing. Validators are cached per type and scope.
//
// # Basic Usage
//
//	type User struct {
//	    Name  string `validate:"len(1,64)"`
//	    Email string `validate:"regexp(^.+@.+$)"`
//	    Age   *int   `validate:"required,between(0,150)"`
//	}
//
//	ge := constraint.Validate(&user, "")
//
// A failure is a *NotSatisfiedError naming the field (Target), the failing check (Node)
// and what would have satisfied it (Expected). Several failures come back together as a
// gomerr.BatchError.
//
// # Scopes
//
// A tag can hold a section per scope, such as an API action, separated by semicolons:
//
//	Name string `validate:"create:len(1,64);update:or(zero,len(1,64))"`
//
// A section without a scope applies when no named section matches. A field with no
// matching section is not validated in that scope, so "list:" alone exempts a field from
// list.
//
// # Checks
//
//	required            the value is present: not a nil pointer, slice, map or interface
//	notzero             the value is not its type's zero value
//	nil, notnil         the value is, or is not, absent
//	zero                the value is its type's zero value
//	true, false         a bool's value
//	len(1,64)           length 1 to 64; len(1) is a minimum, len(,64) a maximum
//	gte(0), gt, lte, lt an ordered comparison
//	between(0,150)      an inclusive range
//	eq(x), neq(x)       equality
//	oneof(a,b,c)        one of the listed values
//	regexp(^[a-z]+$)    the string matches the pattern
//	isregexp            the string is itself a valid pattern
//
// # Composition
//
// Comma-separated checks must all pass, and each failure is reported. The structural
// forms are:
//
//	or(a,b)             either passes
//	not(a)              a fails
//	when($.Mode,eq,X,a) a applies only when the Mode field equals X
//	elements(a)         each element of a slice or array passes a
//	mapkeys(a)          each map key passes a
//	mapvalues(a)        each map value passes a
//	struct              the nested struct passes its own tags
//	union               exactly one of the nested struct's fields is set
//
// # Absent Values
//
// A nil pointer, slice, map or interface is absent. Every check except required, notnil
// and notzero passes an absent value, so an optional field needs no guard:
// `validate:"len(1,64)"` on a *string accepts nil. Use or(zero,X) where an empty value is
// meaningful, such as an update that clears a field.
//
// When an or's other branches are only nil or zero, it marks its remaining branch
// optional rather than offering alternatives, and a failure reports that branch's own
// failures.
//
// # Field References
//
// An operand of the form $.Field reads a sibling field of the struct being validated, as
// in gte($.Min). A reference to an absent field skips the comparison.
//
// # Registered Names
//
// A service names reusable checks, which tags then use like built-ins:
//
//	constraint.Register("$identifier", constraint.Length(1, 1011))
//
// Required, NotZero, Pattern, Length, MinLength, MaxLength, Gte, Gt, Lte, Lt, Between,
// OneOf, And, Union and Fail build Nodes in code.
//
// RegisterCustom registers a check written in Go, for logic the built-in kinds can't
// express. A custom check can read its enclosing struct through CustomContext.Enclosing
// and report several failures through CustomContext.Report.
package constraint
