package structs

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/log"
)

func ExpressionApplierProvider(directive string) (Applier, gomerr.Gomerr) {
	if len(directive) < 2 || directive[0] != '$' {
		return nil, nil
	}

	if directive[1] != '.' {
		tf := GetToolFunction(directive) // include the '$'
		if tf == nil {
			return nil, gomerr.Configuration("field function not found: " + directive)
		}
		return tf, nil
	}
	return StructApplier{directive}, nil
}

// ScopeAlias allows the caller to specify an alternative value to use when defining scoped configuration from the
// scope used during the application of a tool. Aliases need to be defined before PrepareTool() is called.
func ScopeAlias(alias, scope string) {
	if scope == "" {
		delete(scopeAliases, alias)
		return
	}

	if current, ok := scopeAliases[alias]; ok && current != scope {
		panic(alias + " already aliased tp " + current + ". First delete the existing alias to " + scope)
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
// Note that both ':' and ';' are special chars. Once a scope has been provided, colons
// are allowed until the end of the input or a ';' is found. If a colon should be used
// for what would otherwise not contain a scope, one can use the wildcard scope (e.g.
// "*:this_colon_:_does_not_indicate_a_scope").
//
// NB: Scopes can't be reused within the input. If a scope repeats, the last one wins.
//
//	This is also true for wildcards (explicit or implicit).
func applyScopes(ap ApplierProvider, structType reflect.Type, structField reflect.StructField, directive string) (Applier, gomerr.Gomerr) {
	appliers := make(map[string]Applier)
	for _, match := range scopeRegexp.FindAllStringSubmatch(directive, -1) {
		scope := match[1]
		if scope == "" {
			scope = anyScope
		} else if actualScope, ok := scopeAliases[scope]; ok {
			scope = actualScope
		} // else equals the matched value

		if _, ok := appliers[scope]; ok {
			return nil, gomerr.Configuration("multiple sections define for scope '" + scope + "'")
		}

		scopedDirective := match[2]
		// TODO: integrate this w/ expressions logic rather than include here...
		if strings.IndexAny(directive, "?&") == -1 {
			scopedDirective = strings.ReplaceAll(scopedDirective, "\\:", ":")
		}

		applier, ge := ap.Applier(structType, structField, scopedDirective, scope)
		if ge != nil {
			return nil, ge.AddAttribute("scope", scope)
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

func (s scopeSelect) Apply(sv reflect.Value, fv reflect.Value, tc ToolContext) gomerr.Gomerr {
	scopedApplier, ok := s.appliers[tc.Scope()]
	if !ok {
		scopedApplier, ok = s.appliers[anyScope]
		if !ok {
			return nil // no applier for scope/any, return
		}
	}

	return scopedApplier.Apply(sv, fv, tc)
}

// Composite checks for a composition directive (one of 'if', '?', '&' or '!') and if
// found, creates a composed Applier from the directive. If there isn't a composition
// directive, this returns nil for both Applier and gomerr.Gomerr.
// TODO:p2 this should perhaps be a default intermediary similar to how the scope applier can be
func Composite(directive string, tool *Tool, st reflect.Type, sf reflect.StructField) (Applier, gomerr.Gomerr) {
	if strings.HasPrefix(directive, "if(") && directive[len(directive)-1] == ')' {
		return ifThen(directive, tool, st, sf)
	} else if tIndex := strings.IndexAny(directive, "?&!"); tIndex >= 0 {
		return leftRight(directive, tool, st, sf, tIndex)
	}
	return nil, nil
}

// Format: if({test},{do}<,{else}>), for example if($.Enabled,+,-) or if($IsAdmin,+,=*****)
func ifThen(directive string, tool *Tool, st reflect.Type, sf reflect.StructField) (_ Applier, ge gomerr.Gomerr) {
	parts := strings.Split(directive[len("if("):len(directive)-len(")")], ",")

	ite := ifThenElseApplier{name: sf.Name}
	switch len(parts) {
	case 3:
		if ite.orElse, ge = applyScopes(tool.applierProvider, st, sf, parts[2]); ge != nil {
			return nil, ge
		}
		fallthrough
	case 2:
		if ite.then, ge = applyScopes(tool.applierProvider, st, sf, parts[1]); ge != nil {
			return nil, ge
		}
	default:
		return nil, gomerr.Configuration("malformed 'if({test},{do}<,{else}>)' directive: " + directive)
	}

	ea, ge := ExpressionApplierProvider(parts[0])
	if ge != nil {
		return nil, ge.AddAttribute("if", directive)
	}

	sa, ok := ea.(StructApplier)
	if !ok {
		return nil, gomerr.Configuration("if condition directive must reference a struct field or method; directive: " + directive)
	}

	source := sa.Source
	ite.condition = func(sv reflect.Value, fv reflect.Value, tc ToolContext) bool {
		a, vge := ValueFromStruct(sv, fv, source)
		if vge != nil {
			log.Warn("unable to get struct value", "field", sf.Name, "source", source, "ge", vge.Error())
			return false
		}
		return a != nil && !reflect.ValueOf(a).IsZero()
	}
	return ite, nil
}

type ifThenElseApplier struct {
	name      string
	condition func(sv reflect.Value, fv reflect.Value, tc ToolContext) bool
	then      Applier
	orElse    Applier
}

func (a ifThenElseApplier) Apply(sv reflect.Value, fv reflect.Value, tc ToolContext) gomerr.Gomerr {
	if a.condition(sv, fv, tc) {
		return a.then.Apply(sv, fv, tc)
	} else if a.orElse != nil {
		return a.orElse.Apply(sv, fv, tc)
	}
	return nil
}

func leftRight(directive string, tool *Tool, st reflect.Type, sf reflect.StructField, tIndex int) (Applier, gomerr.Gomerr) {
	var left Applier
	var leftGe gomerr.Gomerr
	if lhs := directive[:tIndex]; len(lhs) > 0 {
		left, leftGe = applyScopes(tool.applierProvider, st, sf, lhs)
		if leftGe != nil && gomerr.ErrorAs[*gomerr.ConfigurationError](leftGe) == nil {
			leftGe = gomerr.Configuration("unable to process directive: " + directive).Wrap(leftGe)
		}
	}
	var right Applier
	var rightGe gomerr.Gomerr
	if rhs := directive[tIndex+1:]; len(rhs) > 0 {
		right, rightGe = applyScopes(tool.applierProvider, st, sf, rhs)
		if rightGe != nil && gomerr.ErrorAs[*gomerr.ConfigurationError](rightGe) == nil {
			rightGe = gomerr.Configuration("unable to process directive: " + directive).Wrap(rightGe)
		}
	}
	if ge := gomerr.Batch(leftGe, rightGe); ge != nil || (left == nil && right == nil) {
		return nil, ge
	}

	// TODO:p0 special case "$_b64[encode_type]&[output location]"

	var testFn func(fv reflect.Value, ge gomerr.Gomerr) (skipRightSide bool)
	switch directive[tIndex] {
	case '?':
		testFn = func(value reflect.Value, _ gomerr.Gomerr) bool { return !value.IsZero() }
	case '&':
		testFn = func(reflect.Value, gomerr.Gomerr) bool { return false }
	case '!':
		testFn = func(_ reflect.Value, ge gomerr.Gomerr) bool { return ge != nil }
	}

	return leftTestRightApplier{sf.Name, left, testFn, right}, nil
}

type leftTestRightApplier struct {
	name  string
	left  Applier
	test  func(reflect.Value, gomerr.Gomerr) bool
	right Applier
}

func (a leftTestRightApplier) Apply(sv reflect.Value, fv reflect.Value, tc ToolContext) gomerr.Gomerr {
	var leftGe gomerr.Gomerr

	if a.left != nil {
		leftGe = a.left.Apply(sv, fv, tc)
	}

	if a.test(fv, leftGe) || a.right == nil {
		return leftGe
	}

	ge := a.right.Apply(sv, fv, tc)
	if ge != nil {
		return gomerr.Batch(ge, leftGe) // Okay if leftGe is nil
	} else if leftGe != nil {
		log.Logger().Debug("left-side applier failed, but right side succeeded", "leftError", leftGe)
	}

	return nil
}
