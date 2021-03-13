package fields

import (
	"reflect"
	"regexp"

	"github.com/jt0/gomer/gomerr"
)

const ScopeKey = "$_scope"

// ScopeAlias allows the caller to specify an alternative value to use when defining scoped configuration from the
// scope used during the application of a tool. Aliases need to be defined before a struct is Process-ed.
func ScopeAlias(alias, scope string) {
	if scope == "" {
		delete(scopeAliases, alias)
		return
	}

	if current, ok := scopeAliases[alias]; ok && current != scope {
		// error!
	}

	scopeAliases[alias] = scope
}

var scopeAliases = make(map[string]string)

type ScopingWrapper struct {
	FieldTool
}

func (w ScopingWrapper) Name() string {
	return w.FieldTool.Name()
}

var scopeRegexp = regexp.MustCompile("(?:([^;:]*):)?([^;]*)")

const anyScope = "*"

// Format: [<scope>:]<tool_config>[;[<scope>:]<tool_config>]]*
// Note that both ':' and ';' are special chars. Once a scope has been provided, colons are allowed until the
// end of the input or a ';' is found. If a colon should be used for what would otherwise not contain a scope,
// one can use the wildcard scope (e.g. "*:this_colon_:_does_not_indicate_a_scope").
//
// NB: scopes can't be reused within the input. If a scope repeats, the last one wins. This is true for wildcards
//     (implicit, explicit, or both) as well.
func (w ScopingWrapper) Applier(structType reflect.Type, structField reflect.StructField, input interface{}) (Applier, gomerr.Gomerr) {
	scopedAppliers := make(map[string]Applier)
	inputString, ok := input.(string)
	if !ok {
		inputString = ""
	}

	for _, match := range scopeRegexp.FindAllStringSubmatch(inputString, -1) {
		scope := match[1]
		if scope == "" {
			scope = anyScope
		} else if unaliased, ok := scopeAliases[scope]; ok {
			scope = unaliased
		}

		var newInput interface{}
		if input == nil {
			newInput = nil
		} else {
			newInput = match[2]
		}

		applier, ge := w.FieldTool.Applier(structType, structField, newInput)
		if ge != nil {
			return nil, ge
		} else if applier == nil {
			continue
		}

		scopedAppliers[scope] = applier
	}

	switch len(scopedAppliers) {
	case 0:
		return nil, nil
	case 1:
		// no point in having the intermediate wrapper - just return the tool directly
		if tool, ok := scopedAppliers[anyScope]; ok {
			return tool, nil
		}
	}

	return scopingApplier{scopedAppliers}, nil
}

type scopingApplier struct {
	scopedAppliers map[string]Applier
}

func (a scopingApplier) Apply(structValue reflect.Value, fieldValue reflect.Value, toolContext ToolContext) gomerr.Gomerr {
	scope, ok := toolContext[ScopeKey].(string)
	if !ok {
		scope = anyScope
	}

	applier, ok := a.scopedAppliers[scope]
	if !ok {
		applier, ok = a.scopedAppliers[anyScope]
		if !ok {
			return nil // no matching tool, return
		}
	}

	return applier.Apply(structValue, fieldValue, toolContext)
}

func AddScopeToContext(scope string, tcs ...ToolContext) ToolContext {
	tc := EnsureContext(tcs...)
	tc[ScopeKey] = scope
	return tc
}
