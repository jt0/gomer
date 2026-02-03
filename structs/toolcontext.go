package structs

import (
	"strings"
)

type ToolContext map[string]any

func (tc ToolContext) Put(key string, value any) any {
	old := tc[key]
	tc[key] = value
	return old
}

func (tc ToolContext) With(key string, value any) ToolContext {
	tc[key] = value
	return tc
}

func (tc ToolContext) Get(key string) any {
	return tc[key]
}

func (tc ToolContext) Lookup(key string) (any, bool) {
	v, ok := tc[key]
	return v, ok
}

func (tc ToolContext) Delete(key string) any {
	old := tc[key]
	delete(tc, key)
	return old
}

func (tc ToolContext) Descend(location string, createIntermediates bool) (ToolContext, bool) {
	if tc == nil {
		return nil, false
	}
	m := tc
	for _, locationPart := range strings.Split(location, ".") {
		if mv, ok := m[locationPart]; ok {
			intermediate, isMap := mv.(map[string]any)
			if !isMap {
				return nil, false
			}
			m = intermediate
		} else if createIntermediates {
			intermediate := make(map[string]any)
			m[locationPart], m = intermediate, intermediate
		} else {
			return nil, false
		}
	}
	return m, true
}

func (tc ToolContext) LookupNested(key string) (ToolContext, bool) {
	if lv, found := tc.Lookup(key); !found {
		return nil, false
	} else if n, ok := lv.(ToolContext); ok {
		return n, true
	} else if m, ok := lv.(map[string]any); ok {
		return m, true
	}
	return nil, false
}

func (tc ToolContext) PutScope(scope string) string {
	return tc.Put(scopeKey, scope).(string)
}

func (tc ToolContext) Scope() string {
	scope, ok := tc.Get(scopeKey).(string)
	if !ok {
		return anyScope
	}
	return scope
}

func EnsureContext(tcs ...ToolContext) ToolContext {
	if len(tcs) > 0 && tcs[0] != nil {
		return tcs[0]
	}
	return ToolContext{}
}

func ToolContextWithScope(scope string) ToolContext {
	return map[string]any{scopeKey: scope}
}
