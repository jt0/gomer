package dynamodb

import (
	"testing"

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

	nested := getNestedQueryables(parent)

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

	nested := getNestedQueryables(parent)

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
		if hasNestedQueryables(parent) {
			t.Error("Expected false for nil children")
		}
	})

	t.Run("has nested", func(t *testing.T) {
		parent := &mockParentQueryable{Children: &mockChildQueryableNQ{}}
		if !hasNestedQueryables(parent) {
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
		nested := getNestedQueryables(parent)
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
