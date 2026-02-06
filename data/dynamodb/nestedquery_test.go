package dynamodb

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jt0/gomer/data"
)

// Test types for nested Queryable scenarios

type mockParentQueryable struct {
	data.BaseQueryable
	AccountId   string
	ExtensionId string
	Children    *mockChildQueryableNQ // Nested Queryable
}

func (m *mockParentQueryable) TypeName() string { return "Extension" }

type mockChildQueryableNQ struct {
	data.BaseQueryable
	AccountId   string
	ExtensionId string
	Version     string
}

func (m *mockChildQueryableNQ) TypeName() string { return "ExtensionVersion" }

type mockPermissionQueryableNQ struct {
	data.BaseQueryable
}

func (m *mockPermissionQueryableNQ) TypeName() string { return "Permission" }

// Tests

func TestPaginationContext_CombinedLimit(t *testing.T) {
	child := &mockChildQueryableNQ{}
	child.SetMaximumPageSize(5)

	nested := []nestedQueryableInfo{
		{
			queryable: child,
		},
	}

	parent := &mockParentQueryable{}
	parent.SetMaximumPageSize(10)

	pc := newPaginationContext("Extension", parent, nested)

	// Expected: (10 + 5) * 2 = 30
	expected := int32(30)
	if pc.combinedLimit() != expected {
		t.Errorf("Expected combined limit %d, got %d", expected, pc.combinedLimit())
	}
}

func TestPaginationContext_TryInclude_ParentType(t *testing.T) {
	parent := &mockParentQueryable{}
	parent.SetMaximumPageSize(3)

	pc := newPaginationContext("Extension", parent, nil)

	// Should include first 3
	if !pc.tryInclude("Extension") {
		t.Error("Expected first include to succeed")
	}
	if !pc.tryInclude("Extension") {
		t.Error("Expected second include to succeed")
	}
	if !pc.tryInclude("Extension") {
		t.Error("Expected third include to succeed")
	}

	// Should reject 4th
	if pc.tryInclude("Extension") {
		t.Error("Expected fourth include to fail (limit reached)")
	}
}

func TestPaginationContext_TryInclude_NestedType(t *testing.T) {
	child := &mockChildQueryableNQ{}
	child.SetMaximumPageSize(2)

	nested := []nestedQueryableInfo{
		{
			queryable: child,
		},
	}

	parent := &mockParentQueryable{}
	parent.SetMaximumPageSize(10)

	pc := newPaginationContext("Extension", parent, nested)

	// Should include first 2 children
	if !pc.tryInclude("ExtensionVersion") {
		t.Error("Expected first nested include to succeed")
	}
	if !pc.tryInclude("ExtensionVersion") {
		t.Error("Expected second nested include to succeed")
	}

	// Should reject 3rd
	if pc.tryInclude("ExtensionVersion") {
		t.Error("Expected third nested include to fail (limit reached)")
	}

	// Parent should still work
	if !pc.tryInclude("Extension") {
		t.Error("Parent includes should still work after nested limit reached")
	}
}

func TestPaginationContext_TryInclude_UnknownType(t *testing.T) {
	parent := &mockParentQueryable{}
	parent.SetMaximumPageSize(10)

	pc := newPaginationContext("Extension", parent, nil)

	// Unknown types should be included (no limit)
	if !pc.tryInclude("UnknownType") {
		t.Error("Unknown types should always be included")
	}
}

func TestGetNestedQueryables_NilNestedSkipped(t *testing.T) {
	parent := &mockParentQueryable{
		AccountId:   "acc1",
		ExtensionId: "ext1",
		Children:    nil, // Not initialized - should be skipped
	}

	nested := nestedQueryables(parent)

	if len(nested) != 0 {
		t.Errorf("Expected 0 nested queryables (nil skipped), got %d", len(nested))
	}
}

func TestGetNestedQueryables_NonNilDetected(t *testing.T) {
	parent := &mockParentQueryable{
		AccountId:   "acc1",
		ExtensionId: "ext1",
		Children:    &mockChildQueryableNQ{}, // Initialized - should be detected
	}

	nested := nestedQueryables(parent)

	if len(nested) != 1 {
		t.Fatalf("Expected 1 nested queryable, got %d", len(nested))
	}

	if nested[0].fieldName != "Children" {
		t.Errorf("Expected field name 'Children', got '%s'", nested[0].fieldName)
	}

	if nested[0].queryable.TypeName() != "ExtensionVersion" {
		t.Errorf("Expected persistable type 'ExtensionVersion', got '%s'", nested[0].queryable.TypeName())
	}
}

func TestHasNestedQueryables(t *testing.T) {
	t.Run("no nested", func(t *testing.T) {
		parent := &mockParentQueryable{Children: nil}
		if len(nestedQueryables(parent)) > 0 {
			t.Error("Expected false for nil children")
		}
	})

	t.Run("has nested", func(t *testing.T) {
		parent := &mockParentQueryable{Children: &mockChildQueryableNQ{}}
		if len(nestedQueryables(parent)) == 0 {
			t.Error("Expected true for non-nil children")
		}
	})
}

// Integration test: Full Extension/ExtensionVersion scenario
// This tests the complete flow from detection through pagination context

func TestIntegration_ExtensionVersionScenario(t *testing.T) {
	// Simulate Extension with nested ExtensionVersion Queryable
	// This is the key use case for nested Queryables in STD

	t.Run("detection and pagination context creation", func(t *testing.T) {
		// Create parent with nested queryable initialized
		child := &mockChildQueryableNQ{}
		child.SetMaximumPageSize(5) // Want up to 5 versions

		parent := &mockParentQueryable{
			AccountId:   "acc-123",
			ExtensionId: "ext-456",
			Children:    child,
		}
		parent.SetMaximumPageSize(10) // Want up to 10 extensions

		// Step 1: Detection
		nested := nestedQueryables(parent)
		if len(nested) != 1 {
			t.Fatalf("Expected 1 nested queryable, got %d", len(nested))
		}

		if nested[0].queryable.TypeName() != "ExtensionVersion" {
			t.Errorf("Expected persistable type 'ExtensionVersion', got '%s'", nested[0].queryable.TypeName())
		}

		// Step 2: Pagination context
		pc := newPaginationContext(parent.TypeName(), parent, nested)

		// Verify combined limit includes both types
		// (10 parents + 5 children) * 2 = 30
		expectedLimit := int32(30)
		if pc.combinedLimit() != expectedLimit {
			t.Errorf("Expected combined limit %d, got %d", expectedLimit, pc.combinedLimit())
		}
	})

	t.Run("pagination respects per-type limits", func(t *testing.T) {
		child := &mockChildQueryableNQ{}
		child.SetMaximumPageSize(3)

		nested := []nestedQueryableInfo{
			{
				queryable: child,
			},
		}

		parent := &mockParentQueryable{}
		parent.SetMaximumPageSize(2)

		pc := newPaginationContext("Extension", parent, nested)

		// Simulate mixed result set from DynamoDB
		// E.g., DDB returns: Ext1, ExtVer1, ExtVer2, Ext2, ExtVer3, ExtVer4, ExtVer5

		// Include 2 extensions
		if !pc.tryInclude("Extension") {
			t.Error("Should include first extension")
		}
		if !pc.tryInclude("Extension") {
			t.Error("Should include second extension")
		}
		// Third extension should be rejected (parent limit = 2)
		if pc.tryInclude("Extension") {
			t.Error("Should reject third extension (limit reached)")
		}

		// Include 3 versions
		if !pc.tryInclude("ExtensionVersion") {
			t.Error("Should include first version")
		}
		if !pc.tryInclude("ExtensionVersion") {
			t.Error("Should include second version")
		}
		if !pc.tryInclude("ExtensionVersion") {
			t.Error("Should include third version")
		}
		// Fourth version should be rejected (nested limit = 3)
		if pc.tryInclude("ExtensionVersion") {
			t.Error("Should reject fourth version (limit reached)")
		}

		// Verify final counts
		if pc.parentCount != 2 {
			t.Errorf("Expected parent count 2, got %d", pc.parentCount)
		}
		if pc.nestedCount["ExtensionVersion"] != 3 {
			t.Errorf("Expected nested count 3, got %d", pc.nestedCount["ExtensionVersion"])
		}
	})

	t.Run("multiple nested queryables", func(t *testing.T) {
		// Simulate a parent with multiple nested Queryable types
		// E.g., Extension has both Versions and Permissions

		versions := &mockChildQueryableNQ{}
		versions.SetMaximumPageSize(5)

		permissions := &mockPermissionQueryableNQ{}
		permissions.SetMaximumPageSize(10)

		nested := []nestedQueryableInfo{
			{queryable: versions},
			{queryable: permissions},
		}

		parent := &mockParentQueryable{}
		parent.SetMaximumPageSize(3)

		pc := newPaginationContext("Extension", parent, nested)

		// Combined limit: (3 + 5 + 10) * 2 = 36
		expectedLimit := int32(36)
		if pc.combinedLimit() != expectedLimit {
			t.Errorf("Expected combined limit %d, got %d", expectedLimit, pc.combinedLimit())
		}

		// Each type has its own limit
		for i := 0; i < 5; i++ {
			pc.tryInclude("ExtensionVersion")
		}
		if pc.tryInclude("ExtensionVersion") {
			t.Error("Version limit should be reached")
		}

		// Permissions still have capacity
		for i := 0; i < 10; i++ {
			if !pc.tryInclude("Permission") {
				t.Errorf("Permission %d should be included", i+1)
			}
		}
		if pc.tryInclude("Permission") {
			t.Error("Permission limit should be reached")
		}

		// Parent still has capacity (haven't included any yet)
		if !pc.tryInclude("Extension") {
			t.Error("Parent should still accept items")
		}
	})
}

type mockChildQueryable struct {
	data.BaseQueryable
	ParentId string
}

func (m *mockChildQueryable) TypeName() string { return "ChildItem" }

type mockOtherQueryable struct {
	data.BaseQueryable
	OtherId string
}

func (m *mockOtherQueryable) TypeName() string { return "OtherItem" }

// Test structs

type simpleStruct struct {
	ID   string
	Name string
}

type structWithNilQueryable struct {
	ID    string
	Child *mockChildQueryable // nil
}

type structWithNonNilQueryable struct {
	ID    string
	Child *mockChildQueryable
}

type structWithMultipleQueryables struct {
	ID     string
	ChildA *mockChildQueryable
	ChildB *mockOtherQueryable
}

type structWithMixedQueryables struct {
	ID      string
	Active  *mockChildQueryable // non-nil
	Pending *mockOtherQueryable // nil
	Archive *mockChildQueryable // non-nil
}

type EmbeddedBase struct {
	BaseID string
	Child  *mockChildQueryable
}

type structWithEmbedded struct {
	EmbeddedBase // embedded anonymous struct (exported)
	Name         string
}

type structWithUnexportedQueryable struct {
	ID    string
	child *mockChildQueryable // unexported - should be skipped
}

type structWithValueQueryable struct {
	ID    string
	Child mockChildQueryable // value type, not pointer
}

// Tests

func TestDetectNestedQueryables_NoNestedQueryables(t *testing.T) {
	s := &simpleStruct{ID: "test", Name: "Test Name"}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if len(result) != 0 {
		t.Errorf("Expected 0 nested queryables, got %d", len(result))
	}
}

func TestDetectNestedQueryables_NilQueryableSkipped(t *testing.T) {
	s := &structWithNilQueryable{ID: "test"}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if len(result) != 0 {
		t.Errorf("Expected 0 nested queryables (nil should be skipped), got %d", len(result))
	}
}

func TestDetectNestedQueryables_NonNilQueryableDetected(t *testing.T) {
	s := &structWithNonNilQueryable{
		ID:    "test",
		Child: &mockChildQueryable{ParentId: "parent1"},
	}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if len(result) != 1 {
		t.Fatalf("Expected 1 nested queryable, got %d", len(result))
	}

	if result[0].fieldName != "Child" {
		t.Errorf("Expected fieldName 'Child', got '%s'", result[0].fieldName)
	}

	if result[0].queryable == nil {
		t.Error("Expected queryable to be non-nil")
	}

	if result[0].queryable.TypeName() != "ChildItem" {
		t.Errorf("Expected persistableType 'ChildItem', got '%s'", result[0].queryable.TypeName())
	}
}

func TestDetectNestedQueryables_MultipleQueryables(t *testing.T) {
	s := &structWithMultipleQueryables{
		ID:     "test",
		ChildA: &mockChildQueryable{ParentId: "parent1"},
		ChildB: &mockOtherQueryable{OtherId: "other1"},
	}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if len(result) != 2 {
		t.Fatalf("Expected 2 nested queryables, got %d", len(result))
	}

	// Verify both are detected with correct types
	found := make(map[string]bool)
	for _, r := range result {
		found[r.queryable.TypeName()] = true
	}

	if !found["ChildItem"] {
		t.Error("Expected to find ChildItem queryable")
	}
	if !found["OtherItem"] {
		t.Error("Expected to find OtherItem queryable")
	}
}

func TestDetectNestedQueryables_MixedNilAndNonNil(t *testing.T) {
	s := &structWithMixedQueryables{
		ID:      "test",
		Active:  &mockChildQueryable{ParentId: "active"},
		Pending: nil, // should be skipped
		Archive: &mockChildQueryable{ParentId: "archive"},
	}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if len(result) != 2 {
		t.Fatalf("Expected 2 nested queryables (1 nil skipped), got %d", len(result))
	}

	// Verify correct fields detected
	fieldNames := make(map[string]bool)
	for _, r := range result {
		fieldNames[r.fieldName] = true
	}

	if !fieldNames["Active"] {
		t.Error("Expected to find Active queryable")
	}
	if !fieldNames["Archive"] {
		t.Error("Expected to find Archive queryable")
	}
	if fieldNames["Pending"] {
		t.Error("Pending (nil) should not be detected")
	}
}

func TestDetectNestedQueryables_EmbeddedStruct(t *testing.T) {
	s := &structWithEmbedded{
		EmbeddedBase: EmbeddedBase{
			BaseID: "base1",
			Child:  &mockChildQueryable{ParentId: "embedded"},
		},
		Name: "Test",
	}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if len(result) != 1 {
		t.Fatalf("Expected 1 nested queryable from embedded struct, got %d", len(result))
	}

	if result[0].fieldName != "Child" {
		t.Errorf("Expected fieldName 'Child' from embedded struct, got '%s'", result[0].fieldName)
	}
}

func TestDetectNestedQueryables_UnexportedFieldSkipped(t *testing.T) {
	s := &structWithUnexportedQueryable{
		ID:    "test",
		child: &mockChildQueryable{ParentId: "unexported"},
	}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if len(result) != 0 {
		t.Errorf("Expected 0 nested queryables (unexported should be skipped), got %d", len(result))
	}
}

func TestDetectNestedQueryables_NilInput(t *testing.T) {
	var s *simpleStruct = nil
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if result != nil {
		t.Errorf("Expected nil result for nil input, got %v", result)
	}
}

func TestDetectNestedQueryables_NonStructInput(t *testing.T) {
	s := "not a struct"
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if result != nil {
		t.Errorf("Expected nil result for non-struct input, got %v", result)
	}
}

func TestDetectNestedQueryables_ValueTypeQueryable(t *testing.T) {
	s := &structWithValueQueryable{
		ID:    "test",
		Child: mockChildQueryable{ParentId: "value-type"},
	}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	// Value type Queryables should be detected if addressable
	if len(result) != 1 {
		t.Fatalf("Expected 1 nested queryable for value type, got %d", len(result))
	}

	if result[0].fieldName != "Child" {
		t.Errorf("Expected fieldName 'Child', got '%s'", result[0].fieldName)
	}
}

func TestDetectNestedQueryables_PreservesFieldIndex(t *testing.T) {
	s := &structWithMultipleQueryables{
		ID:     "test",
		ChildA: &mockChildQueryable{ParentId: "a"},
		ChildB: &mockOtherQueryable{OtherId: "b"},
	}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	// Verify field indices are correct
	st := reflect.TypeOf(*s)
	for _, r := range result {
		expectedField := st.Field(r.fieldIndex)
		if expectedField.Name != r.fieldName {
			t.Errorf("Field index mismatch: index %d is %s, expected %s",
				r.fieldIndex, expectedField.Name, r.fieldName)
		}
	}
}

func TestDetectNestedQueryables_QueryableInstanceAccessible(t *testing.T) {
	child := &mockChildQueryable{ParentId: "parent1"}
	child.SetMaximumPageSize(5)

	s := &structWithNonNilQueryable{
		ID:    "test",
		Child: child,
	}
	v := reflect.ValueOf(s)

	result := nestedQueryables(v)

	if len(result) != 1 {
		t.Fatalf("Expected 1 nested queryable, got %d", len(result))
	}

	// Verify we can access the Queryable's properties
	if result[0].queryable.MaximumPageSize() != 5 {
		t.Errorf("Expected MaximumPageSize 5, got %d", result[0].queryable.MaximumPageSize())
	}
}

func TestIndexSupportsAllTypes(t *testing.T) {
	// Setup test index with key fields for different types
	idx := &index{
		pk: &keyAttribute{
			name: "PK",
			keyFieldsByPersistable: map[string][]*keyField{
				"TypeA": {{name: "AccountId"}},
				"TypeB": {{name: "AccountId"}},
			},
		},
		sk: &keyAttribute{
			name: "SK",
			keyFieldsByPersistable: map[string][]*keyField{
				"TypeA": {{name: "'A'"}, {name: "Id"}},
				"TypeB": {{name: "'B'"}, {name: "Id"}},
			},
		},
	}

	tests := []struct {
		name     string
		types    []string
		expected bool
	}{
		{
			name:     "single type supported",
			types:    []string{"TypeA"},
			expected: true,
		},
		{
			name:     "both types supported",
			types:    []string{"TypeA", "TypeB"},
			expected: true,
		},
		{
			name:     "unknown type not supported",
			types:    []string{"TypeA", "TypeC"},
			expected: false,
		},
		{
			name:     "empty types list",
			types:    []string{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := indexSupportsAllTypes(idx, tt.types)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIndexSupportsAllTypes_NoSK(t *testing.T) {
	// Index without sort key
	idx := &index{
		pk: &keyAttribute{
			name: "PK",
			keyFieldsByPersistable: map[string][]*keyField{
				"TypeA": {{name: "Id"}},
			},
		},
		sk: nil, // No sort key
	}

	if !indexSupportsAllTypes(idx, []string{"TypeA"}) {
		t.Error("Expected index without SK to support type with only PK")
	}
}

func TestVerifyPKCompatibility(t *testing.T) {
	tests := []struct {
		name     string
		idx      *index
		types    []string
		expected bool
	}{
		{
			name: "compatible dynamic PK fields",
			idx: &index{
				pk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"TypeA": {{name: "AccountId"}},
						"TypeB": {{name: "AccountId"}},
					},
				},
			},
			types:    []string{"TypeA", "TypeB"},
			expected: true,
		},
		{
			name: "incompatible PK field names",
			idx: &index{
				pk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"TypeA": {{name: "AccountId"}},
						"TypeB": {{name: "TenantId"}}, // Different field name
					},
				},
			},
			types:    []string{"TypeA", "TypeB"},
			expected: false,
		},
		{
			name: "incompatible PK lengths",
			idx: &index{
				pk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"TypeA": {{name: "AccountId"}},
						"TypeB": {{name: "AccountId"}, {name: "Extra"}},
					},
				},
			},
			types:    []string{"TypeA", "TypeB"},
			expected: false,
		},
		{
			name: "compatible static PK fields",
			idx: &index{
				pk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"TypeA": {{name: "'PREFIX'"}, {name: "Id"}},
						"TypeB": {{name: "'PREFIX'"}, {name: "Id"}},
					},
				},
			},
			types:    []string{"TypeA", "TypeB"},
			expected: true,
		},
		{
			name: "incompatible static PK fields",
			idx: &index{
				pk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"TypeA": {{name: "'PREFIX_A'"}, {name: "Id"}},
						"TypeB": {{name: "'PREFIX_B'"}, {name: "Id"}}, // Different static prefix
					},
				},
			},
			types:    []string{"TypeA", "TypeB"},
			expected: false,
		},
		{
			name: "single type always compatible",
			idx: &index{
				pk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"TypeA": {{name: "Id"}},
					},
				},
			},
			types:    []string{"TypeA"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := verifyPKCompatibility(tt.idx, tt.types)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestEscapeAndJoin(t *testing.T) {
	tests := []struct {
		name      string
		segments  []string
		separator byte
		escape    byte
		expected  string
	}{
		{
			name:      "simple segments",
			segments:  []string{"E", "abc123"},
			separator: '#',
			escape:    '$',
			expected:  "E#abc123",
		},
		{
			name:      "single segment",
			segments:  []string{"value"},
			separator: '#',
			escape:    '$',
			expected:  "value",
		},
		{
			name:      "empty segments list",
			segments:  []string{},
			separator: '#',
			escape:    '$',
			expected:  "",
		},
		{
			name:      "segment with separator needs escaping",
			segments:  []string{"a#b", "c"},
			separator: '#',
			escape:    '$',
			expected:  "a$#b#c", // First segment has escaped #
		},
		{
			name:      "segment with escape char needs escaping",
			segments:  []string{"a$b", "c"},
			separator: '#',
			escape:    '$',
			expected:  "a$$b#c", // First segment has escaped $
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeAndJoin(tt.segments, tt.separator, tt.escape)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestCommonKeyPrefix(t *testing.T) {
	// Create test struct type for reflection
	type TestQuery struct {
		AccountId   string
		ExtensionId string
	}

	tests := []struct {
		name       string
		idx        *index
		parentType string
		query      interface{}
		separator  byte
		escape     byte
		expected   string
	}{
		{
			name: "static then dynamic field",
			idx: &index{
				sk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"Extension": {{name: "'E'"}, {name: "ExtensionId"}},
					},
				},
			},
			parentType: "Extension",
			query:      &TestQuery{AccountId: "acc1", ExtensionId: "ext1"},
			separator:  '#',
			escape:     '$',
			expected:   "E#ext1#",
		},
		{
			name: "only static field",
			idx: &index{
				sk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"Entity": {{name: "'E'"}},
					},
				},
			},
			parentType: "Entity",
			query:      &TestQuery{AccountId: "acc1"},
			separator:  '#',
			escape:     '$',
			expected:   "E#",
		},
		{
			name: "missing dynamic field stops prefix",
			idx: &index{
				sk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"Entity": {{name: "'E'"}, {name: "ExtensionId"}, {name: "'V'"}},
					},
				},
			},
			parentType: "Entity",
			query:      &TestQuery{AccountId: "acc1"}, // ExtensionId is zero value
			separator:  '#',
			escape:     '$',
			expected:   "E#",
		},
		{
			name: "no SK returns empty",
			idx: &index{
				sk: nil,
			},
			parentType: "Entity",
			query:      &TestQuery{AccountId: "acc1"},
			separator:  '#',
			escape:     '$',
			expected:   "",
		},
		{
			name: "unknown type returns empty",
			idx: &index{
				sk: &keyAttribute{
					keyFieldsByPersistable: map[string][]*keyField{
						"OtherType": {{name: "'E'"}},
					},
				},
			},
			parentType: "UnknownType",
			query:      &TestQuery{AccountId: "acc1"},
			separator:  '#',
			escape:     '$',
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qv := reflect.ValueOf(tt.query).Elem()
			result := commonKeyPrefix(tt.idx, tt.parentType, qv, tt.separator, tt.escape)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestBuildStaticPrefix(t *testing.T) {
	tests := []struct {
		name      string
		keyFields []*keyField
		separator byte
		expected  string
	}{
		{
			name: "single static value",
			keyFields: []*keyField{
				{name: "'E'"},
			},
			separator: '#',
			expected:  "E",
		},
		{
			name: "static then dynamic",
			keyFields: []*keyField{
				{name: "'E'"},
				{name: "ExtensionId"},
			},
			separator: '#',
			expected:  "E",
		},
		{
			name: "multiple static values",
			keyFields: []*keyField{
				{name: "'E'"},
				{name: "'V'"},
			},
			separator: '#',
			expected:  "E#V",
		},
		{
			name: "dynamic field first",
			keyFields: []*keyField{
				{name: "Id"},
				{name: "'STATUS'"},
			},
			separator: '#',
			expected:  "", // Stops at first dynamic field
		},
		{
			name:      "empty key fields",
			keyFields: []*keyField{},
			separator: '#',
			expected:  "",
		},
		{
			name: "all dynamic fields",
			keyFields: []*keyField{
				{name: "Field1"},
				{name: "Field2"},
			},
			separator: '#',
			expected:  "",
		},
		{
			name: "static-dynamic-static pattern",
			keyFields: []*keyField{
				{name: "'PREFIX'"},
				{name: "Id"},
				{name: "'SUFFIX'"},
			},
			separator: '#',
			expected:  "PREFIX", // Stops at first dynamic
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildStaticPrefix(tt.keyFields, tt.separator)
			if result != tt.expected {
				t.Errorf("Expected prefix '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestSortPatternsBySpecificity(t *testing.T) {
	patterns := []typePattern{
		{prefix: "E", skLength: 2, typeName: "Extension"},
		{prefix: "E", skLength: 7, typeName: "ExtensionVersion"},
		{prefix: "U", skLength: 3, typeName: "User"},
	}

	sortPatternsBySpecificity(patterns)

	// Should be sorted by segment count descending
	if patterns[0].typeName != "ExtensionVersion" {
		t.Errorf("Expected first pattern to be ExtensionVersion, got %s", patterns[0].typeName)
	}
	if patterns[1].typeName != "User" {
		t.Errorf("Expected second pattern to be User, got %s", patterns[1].typeName)
	}
	if patterns[2].typeName != "Extension" {
		t.Errorf("Expected third pattern to be Extension, got %s", patterns[2].typeName)
	}
}

func TestTypeDiscriminator_Discriminate_BySegmentCount(t *testing.T) {
	td := &typeDiscriminator{
		patternsByIndex: map[string][]typePattern{
			"": { // base table
				{prefix: "E", skLength: 7, typeName: "ExtensionVersion"},
				{prefix: "E", skLength: 2, typeName: "Extension"},
			},
		},
		separator: '#',
	}

	tests := []struct {
		name         string
		skValue      string
		expectedType string
		expectError  bool
	}{
		{
			name:         "Extension with 2 segments",
			skValue:      "E#abc123",
			expectedType: "Extension",
		},
		{
			name:         "ExtensionVersion with 7 segments",
			skValue:      "E#abc123#V#1#0#0#",
			expectedType: "ExtensionVersion",
		},
		{
			name:         "Unknown segment count falls back to prefix",
			skValue:      "E#abc123#V#1",
			expectedType: "ExtensionVersion", // Falls back to prefix match (E prefix)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := map[string]types.AttributeValue{
				"SK": &types.AttributeValueMemberS{Value: tt.skValue},
			}

			typeName, err := td.discriminate(item, "", "SK")

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if typeName != tt.expectedType {
					t.Errorf("Expected type '%s', got '%s'", tt.expectedType, typeName)
				}
			}
		})
	}
}

func TestTypeDiscriminator_Discriminate_SameSegmentCount_PrefixFallback(t *testing.T) {
	// Two types with same segment count but different prefixes
	td := &typeDiscriminator{
		patternsByIndex: map[string][]typePattern{
			"": {
				{prefix: "USER", skLength: 2, typeName: "User"},
				{prefix: "ITEM", skLength: 2, typeName: "Item"},
			},
		},
		separator: '#',
	}

	tests := []struct {
		name         string
		skValue      string
		expectedType string
	}{
		{
			name:         "User prefix",
			skValue:      "USER#abc123",
			expectedType: "User",
		},
		{
			name:         "Item prefix",
			skValue:      "ITEM#xyz789",
			expectedType: "Item",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := map[string]types.AttributeValue{
				"SK": &types.AttributeValueMemberS{Value: tt.skValue},
			}

			typeName, err := td.discriminate(item, "", "SK")
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if typeName != tt.expectedType {
				t.Errorf("Expected type '%s', got '%s'", tt.expectedType, typeName)
			}
		})
	}
}

func TestTypeDiscriminator_Discriminate_MissingSK(t *testing.T) {
	td := &typeDiscriminator{
		patternsByIndex: map[string][]typePattern{
			"": {{prefix: "E", skLength: 2, typeName: "Extension"}},
		},
		separator: '#',
	}

	item := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: "partition"},
		// SK is missing
	}

	_, err := td.discriminate(item, "", "SK")
	if err == nil {
		t.Error("Expected error for missing SK attribute")
	}
}

func TestTypeDiscriminator_Discriminate_NoPatterns(t *testing.T) {
	td := &typeDiscriminator{
		patternsByIndex: map[string][]typePattern{},
		separator:       '#',
	}

	item := map[string]types.AttributeValue{
		"SK": &types.AttributeValueMemberS{Value: "E#abc123"},
	}

	_, err := td.discriminate(item, "", "SK")
	if err == nil {
		t.Error("Expected error for no patterns")
	}
}

func TestTypeDiscriminator_Discriminate_NumericSK(t *testing.T) {
	td := &typeDiscriminator{
		patternsByIndex: map[string][]typePattern{
			"": {{prefix: "", skLength: 1, typeName: "NumericEntity"}},
		},
		separator: '#',
	}

	item := map[string]types.AttributeValue{
		"SK": &types.AttributeValueMemberN{Value: "12345"},
	}

	typeName, err := td.discriminate(item, "", "SK")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if typeName != "NumericEntity" {
		t.Errorf("Expected type 'NumericEntity', got '%s'", typeName)
	}
}

func TestTypeDiscriminator_Discriminate_GSI(t *testing.T) {
	td := &typeDiscriminator{
		patternsByIndex: map[string][]typePattern{
			"":      {{prefix: "E", skLength: 2, typeName: "Extension"}},
			"gsi_1": {{prefix: "EMAIL", skLength: 2, typeName: "User"}},
		},
		separator: '#',
	}

	item := map[string]types.AttributeValue{
		"GSI1SK": &types.AttributeValueMemberS{Value: "EMAIL#user@example.com"},
	}

	typeName, err := td.discriminate(item, "gsi_1", "GSI1SK")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if typeName != "User" {
		t.Errorf("Expected type 'User', got '%s'", typeName)
	}
}

func TestTypeDiscriminator_HasPatterns(t *testing.T) {
	t.Run("has patterns", func(t *testing.T) {
		td := &typeDiscriminator{
			patternsByIndex: map[string][]typePattern{
				"": {{prefix: "E", skLength: 2, typeName: "Extension"}},
			},
		}
		if !td.hasPatterns() {
			t.Error("Expected hasPatterns to return true")
		}
	})

	t.Run("no patterns", func(t *testing.T) {
		td := &typeDiscriminator{
			patternsByIndex: map[string][]typePattern{},
		}
		if td.hasPatterns() {
			t.Error("Expected hasPatterns to return false")
		}
	})

	t.Run("empty patterns list", func(t *testing.T) {
		td := &typeDiscriminator{
			patternsByIndex: map[string][]typePattern{
				"": {},
			},
		}
		if td.hasPatterns() {
			t.Error("Expected hasPatterns to return false for empty list")
		}
	})
}

func TestTypeDiscriminator_TypesForIndex(t *testing.T) {
	td := &typeDiscriminator{
		patternsByIndex: map[string][]typePattern{
			"": {
				{prefix: "E", skLength: 7, typeName: "ExtensionVersion"},
				{prefix: "E", skLength: 2, typeName: "Extension"},
			},
			"gsi_1": {
				{prefix: "USER", skLength: 2, typeName: "User"},
			},
		},
	}

	t.Run("base table types", func(t *testing.T) {
		types := td.typesForIndex("")
		if len(types) != 2 {
			t.Errorf("Expected 2 types, got %d", len(types))
		}
	})

	t.Run("gsi types", func(t *testing.T) {
		types := td.typesForIndex("gsi_1")
		if len(types) != 1 || types[0] != "User" {
			t.Errorf("Expected [User], got %v", types)
		}
	})

	t.Run("unknown index", func(t *testing.T) {
		types := td.typesForIndex("unknown")
		if types != nil {
			t.Errorf("Expected nil for unknown index, got %v", types)
		}
	})
}

func TestTypeDiscriminator_Discriminate_EmptyPrefix(t *testing.T) {
	// Test case where a type has no static prefix (all dynamic fields)
	td := &typeDiscriminator{
		patternsByIndex: map[string][]typePattern{
			"": {
				{prefix: "", skLength: 3, typeName: "DynamicEntity"},
				{prefix: "STATIC", skLength: 3, typeName: "StaticEntity"},
			},
		},
		separator: '#',
	}

	tests := []struct {
		name         string
		skValue      string
		expectedType string
	}{
		{
			name:         "Matches static prefix",
			skValue:      "STATIC#a#b",
			expectedType: "StaticEntity",
		},
		{
			name:         "No prefix match falls to empty prefix",
			skValue:      "OTHER#a#b",
			expectedType: "DynamicEntity", // Falls back to empty prefix
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := map[string]types.AttributeValue{
				"SK": &types.AttributeValueMemberS{Value: tt.skValue},
			}

			typeName, err := td.discriminate(item, "", "SK")
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if typeName != tt.expectedType {
				t.Errorf("Expected type '%s', got '%s'", tt.expectedType, typeName)
			}
		})
	}
}
