package rest

import (
	"reflect"
	"testing"

	"github.com/jt0/gomer/_test/assert"
)

// Test types for pathName tests
type Extension struct{}
type Extensions struct{}
type ExtensionVersion struct{}
type ExtensionVersions struct{}
type ExtensionVersionArtifact struct{}
type ExtensionArtifact struct{}
type VersionArtifact struct{}
type FooBat struct{}
type Foo struct{}
type Bar struct{}

// CustomPathResource implements PathNamer to provide an explicit path name
type CustomPathResource struct{}

func (CustomPathResource) PathName() string {
	return "custom"
}

// EmptyPathResource implements PathNamer but returns empty string
type EmptyPathResource struct{}

func (EmptyPathResource) PathName() string {
	return ""
}

func TestPathName_NoAncestors(t *testing.T) {
	tests := []struct {
		name         string
		resourceType reflect.Type
		expected     string
	}{
		{"simple type", reflect.TypeOf(&Extension{}), "Extension"},
		{"plural type", reflect.TypeOf(&Extensions{}), "Extensions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pathName(tt.resourceType, nil)
			assert.Equals(t, tt.expected, result)
		})
	}
}

func TestPathName_TrimsParentTypeName(t *testing.T) {
	ancestors := []ancestorContext{
		{typeName: "Extension", pathName: "Extension"},
	}

	tests := []struct {
		name         string
		resourceType reflect.Type
		expected     string
	}{
		{"ExtensionVersion trims Extension", reflect.TypeOf(&ExtensionVersion{}), "Version"},
		{"ExtensionVersions trims Extension", reflect.TypeOf(&ExtensionVersions{}), "Versions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pathName(tt.resourceType, ancestors)
			assert.Equals(t, tt.expected, result)
		})
	}
}

func TestPathName_TrimsParentPathName(t *testing.T) {
	// Parent is ExtensionVersion, but its path name is already trimmed to "Version"
	ancestors := []ancestorContext{
		{typeName: "ExtensionVersion", pathName: "Version"},
	}

	tests := []struct {
		name         string
		resourceType reflect.Type
		expected     string
	}{
		// ExtensionVersionArtifact matches parent type name "ExtensionVersion" → "Artifact"
		{"ExtensionVersionArtifact trims ExtensionVersion", reflect.TypeOf(&ExtensionVersionArtifact{}), "Artifact"},
		// VersionArtifact matches parent path name "Version" → "Artifact"
		{"VersionArtifact trims Version", reflect.TypeOf(&VersionArtifact{}), "Artifact"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pathName(tt.resourceType, ancestors)
			assert.Equals(t, tt.expected, result)
		})
	}
}

func TestPathName_TrimsGrandparentPrefix(t *testing.T) {
	// Extension -> ExtensionVersion -> ExtensionArtifact
	// ExtensionArtifact should trim "Extension" from grandparent
	ancestors := []ancestorContext{
		{typeName: "ExtensionVersion", pathName: "Version"}, // immediate parent (closest)
		{typeName: "Extension", pathName: "Extension"},      // grandparent
	}

	result := pathName(reflect.TypeOf(&ExtensionArtifact{}), ancestors)
	assert.Equals(t, "Artifact", result)
}

func TestPathName_CloserAncestorTakesPrecedence(t *testing.T) {
	// If both parent and grandparent could match, parent wins (checked first)
	ancestors := []ancestorContext{
		{typeName: "ExtensionVersion", pathName: "Version"},
		{typeName: "Extension", pathName: "Extension"},
	}

	// ExtensionVersionArtifact matches parent "ExtensionVersion", not grandparent "Extension"
	result := pathName(reflect.TypeOf(&ExtensionVersionArtifact{}), ancestors)
	assert.Equals(t, "Artifact", result)
}

func TestPathName_NoMatchingPrefix(t *testing.T) {
	// Foo -> Bar -> FooBat
	// With ancestor checking, FooBat DOES match grandparent "Foo", so it becomes "Bat"
	ancestors := []ancestorContext{
		{typeName: "Bar", pathName: "Bar"},
		{typeName: "Foo", pathName: "Foo"},
	}

	result := pathName(reflect.TypeOf(&FooBat{}), ancestors)
	assert.Equals(t, "Bat", result) // Now trims against grandparent "Foo"
}

func TestPathName_NoMatchingPrefixAtAll(t *testing.T) {
	// When no ancestor matches, return the original name
	ancestors := []ancestorContext{
		{typeName: "Unrelated", pathName: "Unrelated"},
		{typeName: "Other", pathName: "Other"},
	}

	result := pathName(reflect.TypeOf(&Extension{}), ancestors)
	assert.Equals(t, "Extension", result)
}

func TestPathName_ExplicitOverride(t *testing.T) {
	ancestors := []ancestorContext{
		{typeName: "Extension", pathName: "Extension"},
	}

	// Even with a matching ancestor, explicit PathName() should be used
	result := pathName(reflect.TypeOf(&CustomPathResource{}), ancestors)
	assert.Equals(t, "custom", result)
}

func TestPathName_EmptyOverrideUsesAutomatic(t *testing.T) {
	// EmptyPathResource returns "" from PathName(), so automatic trimming should apply
	// Since "EmptyPathResource" starts with "Empty", it gets trimmed to "PathResource"
	ancestors := []ancestorContext{
		{typeName: "Empty", pathName: "Empty"},
	}

	result := pathName(reflect.TypeOf(&EmptyPathResource{}), ancestors)
	assert.Equals(t, "PathResource", result)
}

func TestPathName_CaseInsensitive(t *testing.T) {
	ancestors := []ancestorContext{
		{typeName: "extension", pathName: "extension"},
	}

	// Type name is "ExtensionVersion" but ancestor is "extension" (lowercase)
	result := pathName(reflect.TypeOf(&ExtensionVersion{}), ancestors)
	assert.Equals(t, "Version", result)
}

func TestPathName_TypeNameTakesPrecedence(t *testing.T) {
	// When both ancestor type name and path name could match, type name wins
	// because it's checked first (and is typically longer or equal)
	ancestors := []ancestorContext{
		{typeName: "ExtensionVersion", pathName: "Version"},
	}

	// ExtensionVersionArtifact starts with "ExtensionVersion" (ancestor type), so it uses that
	result := pathName(reflect.TypeOf(&ExtensionVersionArtifact{}), ancestors)
	assert.Equals(t, "Artifact", result)
}

func TestPathName_RequiresLongerName(t *testing.T) {
	// pathName should not trim if the child name is not longer than the prefix
	ancestors := []ancestorContext{
		{typeName: "ExtensionVersionArtifact", pathName: "ExtensionVersionArtifact"},
	}

	// Extension does NOT start with ExtensionVersionArtifact, and is shorter anyway
	result := pathName(reflect.TypeOf(&Extension{}), ancestors)
	assert.Equals(t, "Extension", result)
}

func TestPathName_DeepHierarchy(t *testing.T) {
	// Simulate: Tenant -> TenantWorkspace -> WorkspaceProject -> TenantAuditLog
	// TenantAuditLog should trim "Tenant" from great-grandparent
	type TenantAuditLog struct{}

	ancestors := []ancestorContext{
		{typeName: "WorkspaceProject", pathName: "Project"},
		{typeName: "TenantWorkspace", pathName: "Workspace"},
		{typeName: "Tenant", pathName: "Tenant"},
	}

	result := pathName(reflect.TypeOf(&TenantAuditLog{}), ancestors)
	assert.Equals(t, "AuditLog", result)
}
