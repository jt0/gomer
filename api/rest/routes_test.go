package rest

import (
	"testing"

	"github.com/jt0/gomer/_test/assert"
)

func TestPathName_NoAncestors(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple type", "Extension", "Extension"},
		{"plural type", "Extensions", "Extensions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pathName(tt.input, nil)
			assert.Equals(t, tt.expected, result)
		})
	}
}

func TestPathName_TrimsParentTypeName(t *testing.T) {
	ancestors := []ancestorContext{
		{typeName: "Extension", pathName: "Extension"},
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"ExtensionVersion trims Extension", "ExtensionVersion", "Version"},
		{"ExtensionVersions trims Extension", "ExtensionVersions", "Versions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pathName(tt.input, ancestors)
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
		name     string
		input    string
		expected string
	}{
		// ExtensionVersionArtifact matches parent type name "ExtensionVersion" → "Artifact"
		{"ExtensionVersionArtifact trims ExtensionVersion", "ExtensionVersionArtifact", "Artifact"},
		// VersionArtifact matches parent path name "Version" → "Artifact"
		{"VersionArtifact trims Version", "VersionArtifact", "Artifact"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pathName(tt.input, ancestors)
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

	result := pathName("ExtensionArtifact", ancestors)
	assert.Equals(t, "Artifact", result)
}

func TestPathName_CloserAncestorTakesPrecedence(t *testing.T) {
	// If both parent and grandparent could match, parent wins (checked first)
	ancestors := []ancestorContext{
		{typeName: "ExtensionVersion", pathName: "Version"},
		{typeName: "Extension", pathName: "Extension"},
	}

	// ExtensionVersionArtifact matches parent "ExtensionVersion", not grandparent "Extension"
	result := pathName("ExtensionVersionArtifact", ancestors)
	assert.Equals(t, "Artifact", result)
}

func TestPathName_NoMatchingPrefix(t *testing.T) {
	// Foo -> Bar -> FooBat
	// With ancestor checking, FooBat DOES match grandparent "Foo", so it becomes "Bat"
	ancestors := []ancestorContext{
		{typeName: "Bar", pathName: "Bar"},
		{typeName: "Foo", pathName: "Foo"},
	}

	result := pathName("FooBat", ancestors)
	assert.Equals(t, "Bat", result) // Now trims against grandparent "Foo"
}

func TestPathName_NoMatchingPrefixAtAll(t *testing.T) {
	// When no ancestor matches, return the original name
	ancestors := []ancestorContext{
		{typeName: "Unrelated", pathName: "Unrelated"},
		{typeName: "Other", pathName: "Other"},
	}

	result := pathName("Extension", ancestors)
	assert.Equals(t, "Extension", result)
}

func TestPathName_CaseInsensitive(t *testing.T) {
	ancestors := []ancestorContext{
		{typeName: "extension", pathName: "extension"},
	}

	// Type name is "ExtensionVersion" but ancestor is "extension" (lowercase)
	result := pathName("ExtensionVersion", ancestors)
	assert.Equals(t, "Version", result)
}

func TestPathName_TypeNameTakesPrecedence(t *testing.T) {
	// When both ancestor type name and path name could match, type name wins
	// because it's checked first (and is typically longer or equal)
	ancestors := []ancestorContext{
		{typeName: "ExtensionVersion", pathName: "Version"},
	}

	// ExtensionVersionArtifact starts with "ExtensionVersion" (ancestor type), so it uses that
	result := pathName("ExtensionVersionArtifact", ancestors)
	assert.Equals(t, "Artifact", result)
}

func TestPathName_RequiresLongerName(t *testing.T) {
	// pathName should not trim if the child name is not longer than the prefix
	ancestors := []ancestorContext{
		{typeName: "ExtensionVersionArtifact", pathName: "ExtensionVersionArtifact"},
	}

	// Extension does NOT start with ExtensionVersionArtifact, and is shorter anyway
	result := pathName("Extension", ancestors)
	assert.Equals(t, "Extension", result)
}

func TestPathName_DeepHierarchy(t *testing.T) {
	// Simulate: Tenant -> TenantWorkspace -> WorkspaceProject -> TenantAuditLog
	// TenantAuditLog should trim "Tenant" from great-grandparent
	ancestors := []ancestorContext{
		{typeName: "WorkspaceProject", pathName: "Project"},
		{typeName: "TenantWorkspace", pathName: "Workspace"},
		{typeName: "Tenant", pathName: "Tenant"},
	}

	result := pathName("TenantAuditLog", ancestors)
	assert.Equals(t, "AuditLog", result)
}
