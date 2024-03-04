package structs

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/jt0/gomer/gomerr"
)

func ExpressionApplierProvider(_ reflect.Type, sf reflect.StructField, directive string) (Applier, gomerr.Gomerr) {
	if directive == "" {
		return nil, nil
	}

	// special chars: $, [ (if map or slice/array)

	// if directive[0] != '$' {
	//
	// }

	if directive[1] == '.' {
		return StructApplier{directive}, nil
	} else {
		tf := GetToolFunction(directive) // include the '$'
		if tf == nil {
			return nil, gomerr.Configuration("Field function not found: " + directive)
		}
		return tf, nil
	}
}

// ScopeAlias allows the caller to specify an alternative value to use when defining scoped configuration from the
// scope used during the application of a tool. Aliases need to be defined before PrepareTool() is called.
func ScopeAlias(alias, scope string) {
	if scope == "" {
		delete(scopeAliases, alias)
		return
	}

	if current, ok := scopeAliases[alias]; ok && current != scope {
		panic(fmt.Sprintf("%s already aliased tp %s. First delete the existing alias to %s first.", alias, current, scope))
	}

	scopeAliases[alias] = scope
}

func ScopeAliases(aliasToScope map[string]string) {
	for alias, scope := range aliasToScope {
		ScopeAlias(alias, scope)
	}
}

const (
	anyScope = "*"
	scopeKey = "$_gomer_scope"
)

var (
	scopeAliases = make(map[string]string)
	scopeRegexp  = regexp.MustCompile(`(?:([^;:]*[^\\]):)?([^;]*)`)
)

// Format: [<scope>:]<tool_config>[;[<scope>:]<tool_config>]]*
// Note that both ':' and ';' are special chars. Once a scope has been provided, colons are allowed until the
// end of the input or a ';' is found. If a colon should be used for what would otherwise not contain a scope,
// one can use the wildcard scope (e.g. "*:this_colon_:_does_not_indicate_a_scope").
//
// NB: scopes can't be reused within the input. If a scope repeats, the last one wins. This is true for wildcards
//     (implicit, explicit, or both) as well.
func applyScopes(ap ApplierProvider, structType reflect.Type, structField reflect.StructField, directive string) (Applier, gomerr.Gomerr) {
	appliers := make(map[string]Applier)
	for _, match := range scopeRegexp.FindAllStringSubmatch(directive, -1) {
		scope := match[1]
		if scope == "" {
			scope = anyScope
		} else if actualScope, ok := scopeAliases[scope]; ok {
			scope = actualScope
		} // else equals the matched value

		scopedDirective := match[2]
		// TODO: integrate this w/ expressions logic rather than include here...
		if strings.IndexAny(directive, "?&") == -1 {
			scopedDirective = strings.ReplaceAll(scopedDirective, "\\:", ":")
		}

		applier, ge := ap.Applier(structType, structField, scopedDirective, scope)
		if ge != nil {
			return nil, ge.AddAttribute("Scope", scope)
		} else if applier != nil {
			appliers[scope] = applier
		} else if scope != anyScope {
			appliers[scope] = NoApplier{}
		} // else skip
	}

	switch len(appliers) {
	case 0:
		return nil, nil
	case 1:
		// If only an anyScope applier, avoid the wrapper
		if applier, ok := appliers[anyScope]; ok {
			return applier, nil
		}
	}

	return scopeSelect{appliers}, nil
}

type scopeSelect struct {
	appliers map[string]Applier
}

func (s scopeSelect) Apply(sv reflect.Value, fv reflect.Value, tc *ToolContext) gomerr.Gomerr {
	scopedApplier, ok := s.appliers[tc.Scope()]
	if !ok {
		scopedApplier, ok = s.appliers[anyScope]
		if !ok {
			return nil // no applier for scope/any, return
		}
	}

	return scopedApplier.Apply(sv, fv, tc)
}

// Composite checks for a composition directive (either '?' or '&') and if found will create a composed Applier from
// those based on the specified semantic. If there isn't a composition directive, this returns nil for both Applier and
// gomerr.Gomerr.
// TODO:p2 this should perhaps be a default intermediary similar to how the scope applier can be
func Composite(directive string, tool *Tool, st reflect.Type, sf reflect.StructField) (Applier, gomerr.Gomerr) {
	if strings.HasPrefix(directive, "if(") && directive[len(directive)-1] == ')' {
		// TODO:p1
		// Format: if({test},{do}<,{else}>)
		// Example: if($.Enabled,+,-) or if($IsAdmin,+,=*****)
	}

	tIndex := strings.IndexAny(directive, "?&")
	if tIndex == -1 {
		return nil, nil
	}

	var left Applier
	var leftGe gomerr.Gomerr
	if lhs := directive[:tIndex]; len(lhs) > 0 {
		left, leftGe = applyScopes(tool.applierProvider, st, sf, lhs)
		if _, ok := leftGe.(*gomerr.ConfigurationError); leftGe != nil && !ok {
			leftGe = gomerr.Configuration(fmt.Sprintf("Unable to process directive: %s", directive)).Wrap(leftGe)
		}
	}
	var right Applier
	var rightGe gomerr.Gomerr
	if rhs := directive[tIndex+1:]; len(rhs) > 0 {
		right, rightGe = applyScopes(tool.applierProvider, st, sf, rhs)
		if _, ok := rightGe.(*gomerr.ConfigurationError); rightGe != nil && !ok {
			rightGe = gomerr.Configuration(fmt.Sprintf("Unable to process directive: %s", directive)).Wrap(rightGe)
		}
	}
	if ge := gomerr.Batch(leftGe, rightGe); ge != nil || (left == nil && right == nil) {
		return nil, ge
	}

	// TODO:p0 special case "$_b64[encode_type]&[output location]"

	var testFn func(reflect.Value) bool
	if directive[tIndex] == '?' {
		testFn = func(value reflect.Value) bool { return !value.IsZero() }
	} else { // '&'
		testFn = func(reflect.Value) bool { return false }
	}

	return leftTestRightApplier{sf.Name, left, testFn, right}, nil
}

// func (t *Tool) ifApplier(st reflect.Type, sf reflect.StructField, directive string) (Applier, gomerr.Gomerr) {
// 	return nil, nil
// }

type ifThenElseApplier struct {
	name   string
	test   func(value reflect.Value) bool
	then   Applier
	orElse Applier
}

//func (a ifThenElseApplier) Apply(sv reflect.Value, fv reflect.Value, tc *ToolContext) gomerr.Gomerr {
//}

type leftTestRightApplier struct {
	name  string
	left  Applier
	test  func(value reflect.Value) bool
	right Applier
}

func (a leftTestRightApplier) Apply(sv reflect.Value, fv reflect.Value, tc *ToolContext) gomerr.Gomerr {
	var leftGe gomerr.Gomerr

	if a.left != nil {
		leftGe = a.left.Apply(sv, fv, tc)
	}

	if leftGe == nil && a.test(fv) {
		return nil
	}

	if a.right == nil {
		return leftGe
	}

	ge := a.right.Apply(sv, fv, tc)
	if ge != nil {
		return gomerr.Batch(ge, leftGe) // Okay if leftGe is nil
	} else if leftGe != nil {
		// TODO: replace w/ debug-level log statement
		fmt.Println("Left-side applier failed, but right side succeeded. Left error:\n", leftGe.String())
	}

	return nil
}
