package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

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
			name: "empty key fields",
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
			"":       {{prefix: "E", skLength: 2, typeName: "Extension"}},
			"gsi_1":  {{prefix: "EMAIL", skLength: 2, typeName: "User"}},
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
