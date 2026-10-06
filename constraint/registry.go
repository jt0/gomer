package constraint

import (
	"strconv"
	"strings"

	"github.com/jt0/gomer/gomerr"
)

// A tag name resolves to a node in one of these ways, checked in order: a built-in
// parameterless check such as required, a built-in builder such as gte or len, struct or
// union, a registered custom check, or a registered node such as $identifier.

// nodeBuilder turns a name's parsed operands into a node, returning a configuration
// error for the wrong number or form of operands.
type nodeBuilder func(operands []operand) (Node, gomerr.Gomerr)

var builtinNodes = map[string]Node{
	"required": {kind: kindPresent, name: "required"},
	"notzero":  {kind: kindNotZero, name: "notzero"},
	"nil":      {kind: kindNil, name: "nil"},
	"notnil":   {kind: kindNotNil, name: "notnil"},
	"zero":     {kind: kindZero, name: "zero"},
	"true":     {kind: kindTrue, name: "true"},
	"false":    {kind: kindFalse, name: "false"},
	"isregexp": {kind: kindIsRegexp, name: "isregexp"},
}

var builtinBuilders = map[string]nodeBuilder{
	"gte":     compareBuilder("gte"),
	"gt":      compareBuilder("gt"),
	"lte":     compareBuilder("lte"),
	"lt":      compareBuilder("lt"),
	"between": fixedLeafBuilder(kindBetween, "between", 2, 2),
	"len":     fixedLeafBuilder(kindLength, "len", 1, 2),
	"regexp":  fixedLeafBuilder(kindPattern, "regexp", 1, 1),
	"eq":      fixedLeafBuilder(kindEquals, "eq", 1, 1),
	"neq":     fixedLeafBuilder(kindNotEquals, "neq", 1, 1),
	"oneof": func(operands []operand) (Node, gomerr.Gomerr) {
		if len(operands) == 0 {
			return Node{}, gomerr.Configuration("oneof takes at least one operand")
		}
		return Node{kind: kindOneOf, name: "oneof", params: operands}, nil
	},
}

var (
	registeredNodes = map[string]Node{}
	customChecks    = map[string]Custom{}
)

func compareBuilder(op string) nodeBuilder {
	return func(operands []operand) (Node, gomerr.Gomerr) {
		if len(operands) != 1 {
			return Node{}, gomerr.Configuration(op + " takes one operand")
		}
		return Node{kind: kindCompare, name: op, params: operands}, nil
	}
}

func fixedLeafBuilder(kind kind, name string, minOperands, maxOperands int) nodeBuilder {
	return func(operands []operand) (Node, gomerr.Gomerr) {
		if got := len(operands); got < minOperands || got > maxOperands {
			want := strconv.Itoa(minOperands)
			if minOperands != maxOperands {
				want += " to " + strconv.Itoa(maxOperands)
			}
			return Node{}, gomerr.Configuration(name + " takes " + want + " operands, found " +
				strconv.Itoa(got))
		}
		return Node{kind: kind, name: name, params: operands}, nil
	}
}

// Register names a reusable node, such as $identifier for a shared pattern. The name must
// start with '$', so it can't collide with a built-in.
func Register(name string, node Node) gomerr.Gomerr {
	if ge := checkRegisteredName(name); ge != nil {
		return ge
	}
	registeredNodes[strings.ToLower(name)] = node
	return nil
}

// RegisterCustom registers check under its name.
func RegisterCustom(check Custom) gomerr.Gomerr {
	name := check.Name()
	if ge := checkRegisteredName(name); ge != nil {
		return ge
	}
	customChecks[strings.ToLower(name)] = check
	return nil
}

func checkRegisteredName(name string) gomerr.Gomerr {
	if len(name) < 2 || len(name) > 64 || name[0] != '$' {
		return gomerr.Configuration("a registered name must start with '$' and be 2 to 64 " +
			"characters, found " + name)
	}
	return nil
}
