package structs

import (
	"strings"
)

type ToolContext struct {
	m map[string]interface{}
}

func (tc *ToolContext) Put(key string, value interface{}) *ToolContext {
	if tc == nil {
		//goland:noinspection ALL
		tc = &ToolContext{map[string]interface{}{}}
	} else if tc.m == nil {
		tc.m = map[string]interface{}{}
	}
	tc.m[key] = value
	return tc
}

func (tc *ToolContext) Get(key string) interface{} {
	if tc == nil || tc.m == nil {
		return nil
	}
	return tc.m[key]
}

func (tc *ToolContext) Lookup(key string) (interface{}, bool) {
	if tc == nil || tc.m == nil {
		return nil, false
	}
	v, ok := tc.m[key]
	return v, ok
}

func (tc *ToolContext) Descend(location string, createIntermediates bool) (*ToolContext, bool) {
	if tc == nil || tc.m == nil {
		return nil, false
	}
	m := tc.m
	for _, locationPart := range strings.Split(location, ".") {
		if mv, ok := m[locationPart]; ok {
			intermediate, isMap := mv.(map[string]interface{})
			if !isMap {
				return nil, false
			}
			m = intermediate
		} else if createIntermediates {
			intermediate := make(map[string]interface{})
			m[locationPart], m = intermediate, intermediate
		} else {
			return nil, false
		}
	}
	return &ToolContext{m}, true
}

func (tc *ToolContext) LookupNested(key string) (*ToolContext, bool) {
	if lv, found := tc.Lookup(key); !found {
		return nil, false
	} else if n, ok := lv.(*ToolContext); ok {
		return n, true
	} else if m, ok := lv.(map[string]interface{}); ok {
		return &ToolContext{m}, true
	}
	return nil, false
}

func (tc *ToolContext) PutScope(scope string) *ToolContext {
	return tc.Put(scopeKey, scope)
}

func (tc *ToolContext) Scope() string {
	if tc == nil || tc.m == nil {
		return anyScope
	}
	scope, ok := tc.Get(scopeKey).(string)
	if !ok {
		return anyScope
	}
	return scope
}

func EnsureContext(tcs ...*ToolContext) *ToolContext {
	if len(tcs) > 0 && tcs[0] != nil {
		return tcs[0]
	}
	return &ToolContext{}
}

func ToolContextWithScope(scope string) *ToolContext {
	return &ToolContext{map[string]interface{}{scopeKey: scope}}
}
