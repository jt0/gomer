package constraint

import (
	"strings"

	"github.com/jt0/gomer/gomerr"
)

// parseDirective parses a validate directive into a node. Comma-separated terms at the
// top level are an implicit and. And, or, not, elements, mapkeys and mapvalues take child
// terms; every other name is a leaf whose parenthesized operands are comma-separated, and
// an operand beginning "$." is a field reference. Parsing knows nothing of types; those
// are checked when the node is compiled.
func parseDirective(tag string) (Node, gomerr.Gomerr) {
	return parseAnd(tag)
}

func parseAnd(s string) (Node, gomerr.Gomerr) {
	children, ge := parseTerms(s)
	if ge != nil {
		return Node{}, ge
	}
	switch len(children) {
	case 0:
		return Node{}, gomerr.Configuration("no constraints found: " + s)
	case 1:
		return children[0], nil
	default:
		return Node{kind: kindAnd, name: "and", children: children}, nil
	}
}

func parseTerms(s string) ([]Node, gomerr.Gomerr) {
	var children []Node
	for _, part := range splitTopLevel(s) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		child, ge := parseTerm(part)
		if ge != nil {
			return nil, ge
		}
		children = append(children, child)
	}
	return children, nil
}

func parseTerm(term string) (Node, gomerr.Gomerr) {
	open := strings.Index(term, "(")
	if open < 0 {
		return resolveName(term, nil)
	}
	if !strings.HasSuffix(term, ")") {
		return Node{}, gomerr.Configuration("unbalanced parentheses:" + term)
	}

	name := strings.ToLower(strings.TrimSpace(term[:open]))
	inner := term[open+1 : len(term)-1]

	switch name {
	case "and":
		return parseAnd(inner)
	case "or":
		children, ge := parseTerms(inner)
		if ge != nil {
			return Node{}, ge
		}
		if len(children) == 0 {
			return Node{}, gomerr.Configuration("or takes at least one operand")
		}
		return Node{kind: kindOr, name: "or", children: children}, nil
	case "not":
		child, ge := parseAnd(inner)
		if ge != nil {
			return Node{}, ge
		}
		return Node{kind: kindNot, name: "not", children: []Node{child}}, nil
	case "when":
		return parseWhen(inner)
	case "elements", "mapkeys", "mapvalues":
		child, ge := parseAnd(inner)
		if ge != nil {
			return Node{}, ge
		}
		k := kindElements
		switch name {
		case "mapkeys":
			k = kindMapKeys
		case "mapvalues":
			k = kindMapValues
		}
		return Node{kind: k, name: name, children: []Node{child}}, nil
	default:
		parts := splitTopLevel(inner)
		operands := make([]operand, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "$.") {
				operands = append(operands, operand{Ref: part})
			} else {
				operands = append(operands, operand{Value: part})
			}
		}
		return resolveName(name, operands)
	}
}

// resolveName turns a parsed name and its operands into a node. An unknown name is a
// configuration error.
func resolveName(name string, operands []operand) (Node, gomerr.Gomerr) {
	lower := strings.ToLower(name)
	if node, ok := builtinNodes[lower]; ok {
		if len(operands) == 0 {
			return node, nil
		}
		// A presence check takes one $.Field operand that redirects it to another field,
		// so nil($.Min) tests whether Min is set.
		if k := node.kind; (k == kindPresent || k == kindNil || k == kindNotNil || k == kindZero || k == kindNotZero) &&
			len(operands) == 1 && operands[0].isRef() {
			node.params = operands
			return node, nil
		}
		return Node{}, gomerr.Configuration(name + " takes no operands")
	}
	if builder, ok := builtinBuilders[lower]; ok {
		return builder(operands)
	}
	switch lower {
	case "struct":
		return Node{kind: kindStruct, name: "struct"}, nil
	case "union":
		return Node{kind: kindUnion, name: "union"}, nil
	}
	if cu, ok := customChecks[lower]; ok {
		if len(operands) != 0 {
			return Node{}, gomerr.Configuration(name + " is a custom check and takes no operands")
		}
		return Node{kind: kindCustom, name: cu.Name(), custom: cu}, nil
	}
	if node, ok := registeredNodes[lower]; ok {
		return node, nil
	}
	return Node{}, gomerr.Configuration("unrecognized constraint: " + name)
}

// parseWhen parses a when term's operands: a $.Field condition, an operator, a value, and
// the child constraint, which is the remaining terms as an implicit and.
func parseWhen(inner string) (Node, gomerr.Gomerr) {
	parts := splitTopLevel(inner)
	if len(parts) < 4 {
		return Node{}, gomerr.Configuration("when takes a $.Field condition, an operator, a value and a constraint, found: " + inner)
	}
	condition := strings.TrimSpace(parts[0])
	if !strings.HasPrefix(condition, "$.") {
		return Node{}, gomerr.Configuration("when's condition must be a $.Field reference, found: " + condition)
	}
	operator := strings.TrimSpace(parts[1])
	value := strings.TrimSpace(parts[2])
	then, ge := parseAnd(strings.Join(parts[3:], ","))
	if ge != nil {
		return Node{}, ge
	}
	return Node{
		kind:     kindWhen,
		name:     "when",
		params:   []operand{{Ref: condition}, {Value: operator}, {Value: value}},
		children: []Node{then},
	}, nil
}

// splitTopLevel splits s at the commas that aren't inside parentheses or braces.
func splitTopLevel(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(', '{':
			depth++
		case ')', '}':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}
