package constraint

import (
	"testing"
)

func TestRegisterResolves(t *testing.T) {
	if ge := Register("$word", Node{kind: kindPattern, params: []operand{{Value: "^[a-z]+$"}}}); ge != nil {
		t.Fatal(ge)
	}
	node := mustParse(t, "$word")
	vr := mustValidator(t, node, stringType)
	assertPass(t, vr, "abc")
	assertNotSatisfied(t, vr, "ABC")
}

func TestRegisterCustom(t *testing.T) {
	custom := evenCheck()
	if ge := RegisterCustom(custom); ge != nil {
		t.Fatal(ge)
	}
	node := mustParse(t, "$even")
	if node.kind != kindCustom {
		t.Fatalf("want a custom node, got %s", node.kind)
	}
	vr := mustValidator(t, node, intType)
	assertPass(t, vr, 2)
	assertNotSatisfied(t, vr, 1)
}

func TestRegisterNameValidation(t *testing.T) {
	for _, name := range []string{"noDollar", "$", ""} {
		if ge := Register(name, Node{kind: kindFail}); ge == nil {
			t.Errorf("Register(%q): want a validation error", name)
		}
	}
}

func TestUnregisteredNameError(t *testing.T) {
	if _, ge := parseDirective("$neverregistered"); ge == nil {
		t.Error("want an error resolving an unregistered name")
	}
}
