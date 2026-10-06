package constraint

import "testing"

// TestConstructorsPrintAsTags checks that each constructor builds the Node its tag would
// parse to.
func TestConstructorsPrintAsTags(t *testing.T) {
	for want, node := range map[string]Node{
		"required":       Required(),
		"regexp(^a$)":    Pattern("^a$"),
		"len(1,64)":      Length(1, 64),
		"len(1)":         MinLength(1),
		"len(,64)":       MaxLength(64),
		"gte(0)":         Gte(0),
		"lt(10)":         Lt(10),
		"between(0,150)": Between(0, 150),
		"oneof(a,b)":     OneOf("a", "b"),
		"len(1),gte(0)":  And(MinLength(1), Gte(0)),
		"union":          Union(),
	} {
		if got := node.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}
