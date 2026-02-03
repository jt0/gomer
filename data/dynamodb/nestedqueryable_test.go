package dynamodb

import (
	"reflect"
	"testing"

	"github.com/jt0/gomer/data"
)

// Mock Queryable implementations for testing

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

	result := detectNestedQueryables(v)

	if len(result) != 0 {
		t.Errorf("Expected 0 nested queryables, got %d", len(result))
	}
}

func TestDetectNestedQueryables_NilQueryableSkipped(t *testing.T) {
	s := &structWithNilQueryable{ID: "test"}
	v := reflect.ValueOf(s)

	result := detectNestedQueryables(v)

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

	result := detectNestedQueryables(v)

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

	result := detectNestedQueryables(v)

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

	result := detectNestedQueryables(v)

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

	result := detectNestedQueryables(v)

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

	result := detectNestedQueryables(v)

	if len(result) != 0 {
		t.Errorf("Expected 0 nested queryables (unexported should be skipped), got %d", len(result))
	}
}

func TestDetectNestedQueryables_NilInput(t *testing.T) {
	var s *simpleStruct = nil
	v := reflect.ValueOf(s)

	result := detectNestedQueryables(v)

	if result != nil {
		t.Errorf("Expected nil result for nil input, got %v", result)
	}
}

func TestDetectNestedQueryables_NonStructInput(t *testing.T) {
	s := "not a struct"
	v := reflect.ValueOf(s)

	result := detectNestedQueryables(v)

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

	result := detectNestedQueryables(v)

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

	result := detectNestedQueryables(v)

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

	result := detectNestedQueryables(v)

	if len(result) != 1 {
		t.Fatalf("Expected 1 nested queryable, got %d", len(result))
	}

	// Verify we can access the Queryable's properties
	if result[0].queryable.MaximumPageSize() != 5 {
		t.Errorf("Expected MaximumPageSize 5, got %d", result[0].queryable.MaximumPageSize())
	}
}
