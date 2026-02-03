package dynamodb

import (
	"reflect"
	"testing"
)

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
