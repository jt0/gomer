package dynamodb

import (
	"reflect"
	"testing"
)

type inner struct {
	A string
	B int
}

type mergeTarget struct {
	// Scalars
	Name  string
	Count int
	// Embedded struct
	Embed inner
	// Pointer to struct
	Ptr *inner
	// Pointer to scalar
	PtrStr *string
	PtrInt *int
	// Maps
	Tags   map[string]string
	Scores map[string]int
	PtrMap map[string]*string
	// Pointer to map
	PtrToMap *map[string]string
	// Maps with struct values
	StructMap    map[string]inner
	PtrStructMap map[string]*inner
	// Slices
	Items []string
}

func sp(s string) *string                       { return &s }
func ip(i int) *int                             { return &i }
func mp(m map[string]string) *map[string]string { return &m }

func TestMergeFields(t *testing.T) {
	tests := []struct {
		name   string
		uv, pv mergeTarget
		want   mergeTarget
	}{
		// --- Scalars ---
		{
			name: "scalar/zero uv is skipped",
			uv:   mergeTarget{},
			pv:   mergeTarget{Name: "keep", Count: 5},
			want: mergeTarget{Name: "keep", Count: 5},
		},
		{
			name: "scalar/non-zero uv updates pv",
			uv:   mergeTarget{Name: "new"},
			pv:   mergeTarget{Name: "old"},
			want: mergeTarget{Name: "new"},
		},
		{
			name: "scalar/equal values leave pv unchanged",
			uv:   mergeTarget{Name: "same"},
			pv:   mergeTarget{Name: "same"},
			want: mergeTarget{Name: "same"},
		},
		// --- Embedded struct ---
		{
			name: "struct/recurses into embedded struct",
			uv:   mergeTarget{Embed: inner{A: "changed"}},
			pv:   mergeTarget{Embed: inner{A: "old", B: 1}},
			want: mergeTarget{Embed: inner{A: "changed", B: 1}},
		},
		// --- Pointer to struct ---
		{
			name: "ptr-struct/nil uv leaves pv untouched",
			uv:   mergeTarget{},
			pv:   mergeTarget{Ptr: &inner{A: "original", B: 99}},
			want: mergeTarget{Ptr: &inner{A: "original", B: 99}},
		},
		{
			name: "ptr-struct/zero struct clears pv",
			uv:   mergeTarget{Ptr: &inner{}},
			pv:   mergeTarget{Ptr: &inner{A: "hello", B: 42}},
			want: mergeTarget{},
		},
		{
			name: "ptr-struct/partial merge updates changed fields only",
			uv:   mergeTarget{Ptr: &inner{A: "changed"}},
			pv:   mergeTarget{Ptr: &inner{A: "keep", B: 1}},
			want: mergeTarget{Ptr: &inner{A: "changed", B: 1}},
		},
		{
			name: "ptr-struct/allocates nil pv before merge",
			uv:   mergeTarget{Ptr: &inner{A: "new"}},
			pv:   mergeTarget{},
			want: mergeTarget{Ptr: &inner{A: "new"}},
		},
		// --- Pointer to scalar ---
		{
			name: "ptr-scalar/nil uv leaves pv untouched",
			uv:   mergeTarget{},
			pv:   mergeTarget{PtrStr: sp("keep")},
			want: mergeTarget{PtrStr: sp("keep")},
		},
		{
			name: "ptr-scalar/different value overwrites pv",
			uv:   mergeTarget{PtrStr: sp("new")},
			pv:   mergeTarget{PtrStr: sp("old")},
			want: mergeTarget{PtrStr: sp("new")},
		},
		{
			name: "ptr-scalar/same dereferenced value is noop",
			uv:   mergeTarget{PtrStr: sp("same")},
			pv:   mergeTarget{PtrStr: sp("same")},
			want: mergeTarget{PtrStr: sp("same")},
		},
		{
			name: "ptr-scalar/non-nil uv into nil pv",
			uv:   mergeTarget{PtrInt: ip(42)},
			pv:   mergeTarget{},
			want: mergeTarget{PtrInt: ip(42)},
		},
		// --- Map: string values ---
		{
			name: "map/nil uv leaves pv untouched",
			uv:   mergeTarget{},
			pv:   mergeTarget{Tags: map[string]string{"a": "1"}},
			want: mergeTarget{Tags: map[string]string{"a": "1"}},
		},
		{
			name: "map/empty uv clears pv",
			uv:   mergeTarget{Tags: map[string]string{}},
			pv:   mergeTarget{Tags: map[string]string{"a": "1", "b": "2"}},
			want: mergeTarget{},
		},
		{
			name: "map/add new key",
			uv:   mergeTarget{Tags: map[string]string{"b": "2"}},
			pv:   mergeTarget{Tags: map[string]string{"a": "1"}},
			want: mergeTarget{Tags: map[string]string{"a": "1", "b": "2"}},
		},
		{
			name: "map/update existing key",
			uv:   mergeTarget{Tags: map[string]string{"a": "changed"}},
			pv:   mergeTarget{Tags: map[string]string{"a": "1"}},
			want: mergeTarget{Tags: map[string]string{"a": "changed"}},
		},
		{
			name: "map/delete key via zero value",
			uv:   mergeTarget{Tags: map[string]string{"a": ""}},
			pv:   mergeTarget{Tags: map[string]string{"a": "1", "b": "2"}},
			want: mergeTarget{Tags: map[string]string{"b": "2"}},
		},
		{
			name: "map/same value is noop",
			uv:   mergeTarget{Tags: map[string]string{"a": "1"}},
			pv:   mergeTarget{Tags: map[string]string{"a": "1"}},
			want: mergeTarget{Tags: map[string]string{"a": "1"}},
		},
		{
			name: "map/delete nonexistent key is noop",
			uv:   mergeTarget{Tags: map[string]string{"z": ""}},
			pv:   mergeTarget{Tags: map[string]string{"a": "1"}},
			want: mergeTarget{Tags: map[string]string{"a": "1"}},
		},
		{
			name: "map/add to nil pv map",
			uv:   mergeTarget{Tags: map[string]string{"x": "new"}},
			pv:   mergeTarget{},
			want: mergeTarget{Tags: map[string]string{"x": "new"}},
		},
		{
			name: "map/mixed add update delete",
			uv:   mergeTarget{Tags: map[string]string{"update": "new", "remove": "", "add": "hi"}},
			pv:   mergeTarget{Tags: map[string]string{"keep": "v", "update": "old", "remove": "bye"}},
			want: mergeTarget{Tags: map[string]string{"keep": "v", "update": "new", "add": "hi"}},
		},
		// --- Map: int values ---
		{
			name: "map/zero int deletes key",
			uv:   mergeTarget{Scores: map[string]int{"a": 0}},
			pv:   mergeTarget{Scores: map[string]int{"a": 5}},
			want: mergeTarget{Scores: map[string]int{}},
		},
		{
			name: "map/nonzero int updates key",
			uv:   mergeTarget{Scores: map[string]int{"a": 10}},
			pv:   mergeTarget{Scores: map[string]int{"a": 5}},
			want: mergeTarget{Scores: map[string]int{"a": 10}},
		},
		// --- Map: pointer values ---
		{
			name: "map-ptr/nil value deletes key",
			uv:   mergeTarget{PtrMap: map[string]*string{"a": nil}},
			pv:   mergeTarget{PtrMap: map[string]*string{"a": sp("old")}},
			want: mergeTarget{PtrMap: map[string]*string{}},
		},
		{
			name: "map-ptr/non-nil value adds key",
			uv:   mergeTarget{PtrMap: map[string]*string{"b": sp("new")}},
			pv:   mergeTarget{PtrMap: map[string]*string{"a": sp("keep")}},
			want: mergeTarget{PtrMap: map[string]*string{"a": sp("keep"), "b": sp("new")}},
		},
		// --- Pointer to map ---
		{
			name: "ptr-to-map/nil uv leaves pv untouched",
			uv:   mergeTarget{},
			pv:   mergeTarget{PtrToMap: mp(map[string]string{"a": "1"})},
			want: mergeTarget{PtrToMap: mp(map[string]string{"a": "1"})},
		},
		{
			name: "ptr-to-map/empty map clears pointer",
			uv:   mergeTarget{PtrToMap: mp(map[string]string{})},
			pv:   mergeTarget{PtrToMap: mp(map[string]string{"a": "1", "b": "2"})},
			want: mergeTarget{},
		},
		{
			name: "ptr-to-map/merge keys",
			uv:   mergeTarget{PtrToMap: mp(map[string]string{"b": "new"})},
			pv:   mergeTarget{PtrToMap: mp(map[string]string{"a": "keep", "b": "old"})},
			want: mergeTarget{PtrToMap: mp(map[string]string{"a": "keep", "b": "new"})},
		},
		{
			name: "ptr-to-map/delete key via zero value",
			uv:   mergeTarget{PtrToMap: mp(map[string]string{"a": ""})},
			pv:   mergeTarget{PtrToMap: mp(map[string]string{"a": "1", "b": "2"})},
			want: mergeTarget{PtrToMap: mp(map[string]string{"b": "2"})},
		},
		{
			name: "ptr-to-map/nil pv sets pointer",
			uv:   mergeTarget{PtrToMap: mp(map[string]string{"a": "1"})},
			pv:   mergeTarget{},
			want: mergeTarget{PtrToMap: mp(map[string]string{"a": "1"})},
		},
		// --- Map with struct values ---
		{
			name: "map-struct/merges into existing key",
			uv:   mergeTarget{StructMap: map[string]inner{"x": {A: "new"}}},
			pv:   mergeTarget{StructMap: map[string]inner{"x": {A: "old", B: 5}}},
			want: mergeTarget{StructMap: map[string]inner{"x": {A: "new", B: 5}}},
		},
		{
			name: "map-struct/adds new key",
			uv:   mergeTarget{StructMap: map[string]inner{"y": {A: "added", B: 1}}},
			pv:   mergeTarget{StructMap: map[string]inner{"x": {A: "keep"}}},
			want: mergeTarget{StructMap: map[string]inner{"x": {A: "keep"}, "y": {A: "added", B: 1}}},
		},
		{
			name: "map-struct/zero struct deletes key",
			uv:   mergeTarget{StructMap: map[string]inner{"x": {}}},
			pv:   mergeTarget{StructMap: map[string]inner{"x": {A: "old", B: 5}}},
			want: mergeTarget{StructMap: map[string]inner{}},
		},
		// --- Map with pointer-to-struct values ---
		{
			name: "map-ptr-struct/merges into existing key",
			uv:   mergeTarget{PtrStructMap: map[string]*inner{"x": {A: "new"}}},
			pv:   mergeTarget{PtrStructMap: map[string]*inner{"x": {A: "old", B: 5}}},
			want: mergeTarget{PtrStructMap: map[string]*inner{"x": {A: "new", B: 5}}},
		},
		{
			name: "map-ptr-struct/nil value deletes key",
			uv:   mergeTarget{PtrStructMap: map[string]*inner{"x": nil}},
			pv:   mergeTarget{PtrStructMap: map[string]*inner{"x": {A: "old"}}},
			want: mergeTarget{PtrStructMap: map[string]*inner{}},
		},
		{
			name: "map-ptr-struct/zero struct deletes key",
			uv:   mergeTarget{PtrStructMap: map[string]*inner{"x": {}}},
			pv:   mergeTarget{PtrStructMap: map[string]*inner{"x": {A: "old", B: 5}}},
			want: mergeTarget{PtrStructMap: map[string]*inner{}},
		},
		{
			name: "map-ptr-struct/adds new key when pv missing",
			uv:   mergeTarget{PtrStructMap: map[string]*inner{"y": {A: "new", B: 1}}},
			pv:   mergeTarget{PtrStructMap: map[string]*inner{"x": {A: "keep"}}},
			want: mergeTarget{PtrStructMap: map[string]*inner{"x": {A: "keep"}, "y": {A: "new", B: 1}}},
		},
		// --- Slices ---
		{
			name: "slice/nil uv leaves pv untouched",
			uv:   mergeTarget{},
			pv:   mergeTarget{Items: []string{"a", "b"}},
			want: mergeTarget{Items: []string{"a", "b"}},
		},
		{
			name: "slice/non-nil replaces pv",
			uv:   mergeTarget{Items: []string{"x", "y"}},
			pv:   mergeTarget{Items: []string{"a", "b", "c"}},
			want: mergeTarget{Items: []string{"x", "y"}},
		},
		{
			name: "slice/empty slice replaces pv",
			uv:   mergeTarget{Items: []string{}},
			pv:   mergeTarget{Items: []string{"a", "b"}},
			want: mergeTarget{Items: []string{}},
		},
		{
			name: "slice/non-nil into nil pv",
			uv:   mergeTarget{Items: []string{"new"}},
			pv:   mergeTarget{},
			want: mergeTarget{Items: []string{"new"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uv, pv := tt.uv, tt.pv
			mergeFields(reflect.ValueOf(&pv).Elem(), reflect.ValueOf(&uv).Elem(), nil)
			if !reflect.DeepEqual(pv, tt.want) {
				t.Errorf("got  %+v\nwant %+v", pv, tt.want)
			}
		})
	}
}

func TestMergeFields_ConstraintTracking(t *testing.T) {
	pt := &persistableType{constraintFields: map[string]bool{"Name": true}}

	tests := []struct {
		name           string
		uv, pv         mergeTarget
		wantConstraint bool
	}{
		{
			name:           "returns true when constraint field changes",
			uv:             mergeTarget{Name: "new"},
			pv:             mergeTarget{Name: "old"},
			wantConstraint: true,
		},
		{
			name:           "returns false when non-constraint field changes",
			uv:             mergeTarget{Count: 99},
			pv:             mergeTarget{Count: 1},
			wantConstraint: false,
		},
		{
			name:           "returns false when nothing changes",
			uv:             mergeTarget{},
			pv:             mergeTarget{Name: "same"},
			wantConstraint: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uv, pv := tt.uv, tt.pv
			got := mergeFields(reflect.ValueOf(&pv).Elem(), reflect.ValueOf(&uv).Elem(), pt)
			if got != tt.wantConstraint {
				t.Errorf("validateConstraints = %v, want %v", got, tt.wantConstraint)
			}
		})
	}
}
