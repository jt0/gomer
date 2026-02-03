package dynamodb

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// typeDiscriminator holds pre-computed patterns for type identification.
// When a query returns items of multiple types (e.g., Extension and ExtensionVersion),
// this is used to determine which persistableType each item belongs to for correct unmarshaling.
type typeDiscriminator struct {
	// patternsByIndex maps index name to type patterns for that index.
	// Empty string key represents the base table (no index name).
	// Patterns are sorted by segment count descending for efficient matching.
	patternsByIndex map[string][]typePattern
	separator       byte
}

// typePattern holds the discrimination criteria for a single type.
type typePattern struct {
	prefix    string // Static prefix from key fields (e.g., "E#" for Extension)
	skLength  int    // Expected number of SK segments for this type
	typeName  string // The persistableType name
}

// buildTypeDiscriminator creates a typeDiscriminator from the table's registered types.
// This should be called after all Persistables are registered with the table.
func (t *table) buildTypeDiscriminator() *typeDiscriminator {
	td := &typeDiscriminator{
		patternsByIndex: make(map[string][]typePattern),
		separator:       t.valueSeparatorChar,
	}

	for _, idx := range t.indexes {
		indexName := ""
		if idx.name != nil {
			indexName = *idx.name
		}

		// Only process if index has a sort key (SK needed for discrimination)
		if idx.sk == nil {
			continue
		}

		var patterns []typePattern

		for typeName, keyFields := range idx.sk.keyFieldsByPersistable {
			// Build static prefix from key fields
			prefix := buildStaticPrefix(keyFields, t.valueSeparatorChar)

			patterns = append(patterns, typePattern{
				prefix:   prefix,
				skLength: len(keyFields),
				typeName: typeName,
			})
		}

		// Sort by segment count descending (more specific types first)
		// This helps when multiple types have same prefix but different lengths
		sortPatternsBySpecificity(patterns)

		td.patternsByIndex[indexName] = patterns
	}

	return td
}

// sortPatternsBySpecificity sorts patterns so that more specific patterns come first.
// Specificity is determined by: 1) segment count descending, 2) prefix length descending.
func sortPatternsBySpecificity(patterns []typePattern) {
	for i := 0; i < len(patterns); i++ {
		for j := i + 1; j < len(patterns); j++ {
			// Sort by segment count descending
			if patterns[j].skLength > patterns[i].skLength {
				patterns[i], patterns[j] = patterns[j], patterns[i]
			} else if patterns[j].skLength == patterns[i].skLength {
				// Same segment count: sort by prefix length descending
				if len(patterns[j].prefix) > len(patterns[i].prefix) {
					patterns[i], patterns[j] = patterns[j], patterns[i]
				}
			}
		}
	}
}

// buildStaticPrefix extracts the static prefix from key fields.
// Static values in key fields are quoted (e.g., "'E'").
// The prefix includes only static values and their separators.
// Does NOT include a trailing separator when stopping at a dynamic field.
func buildStaticPrefix(keyFields []*keyField, separator byte) string {
	var parts []string

	for _, kf := range keyFields {
		// Static values start with a single quote
		if len(kf.name) > 0 && kf.name[0] == '\'' {
			// Extract value between quotes
			if len(kf.name) >= 2 {
				parts = append(parts, kf.name[1:len(kf.name)-1])
			}
		} else {
			// Dynamic field - stop building prefix here
			// We can only reliably match up to the first dynamic segment
			break
		}
	}

	return strings.Join(parts, string(separator))
}

// discriminate determines the type of an item based on its SK value.
// Uses segment count as primary discriminator (most reliable for STD patterns).
// Falls back to prefix matching for edge cases.
//
// Parameters:
//   - item: The DynamoDB item to discriminate
//   - indexName: The index being queried ("" for base table)
//   - skAttrName: The name of the SK attribute
//
// Returns:
//   - typeName: The identified persistableType name
//   - error: If SK not found or no matching type
func (td *typeDiscriminator) discriminate(item map[string]types.AttributeValue, indexName string, skAttrName string) (string, error) {
	patterns := td.patternsByIndex[indexName]
	if len(patterns) == 0 {
		return "", fmt.Errorf("no patterns for index: %s", indexName)
	}

	// Get SK value from item
	skAttr, ok := item[skAttrName]
	if !ok {
		return "", fmt.Errorf("SK attribute not found: %s", skAttrName)
	}

	var skValue string
	switch v := skAttr.(type) {
	case *types.AttributeValueMemberS:
		skValue = v.Value
	case *types.AttributeValueMemberN:
		skValue = v.Value
	default:
		return "", fmt.Errorf("unexpected SK type: %T", skAttr)
	}

	// Count segments in the SK value
	segments := strings.Split(skValue, string(td.separator))
	segmentCount := len(segments)

	// PRIMARY: Match by segment count (most reliable for STD)
	var candidates []typePattern
	for _, p := range patterns {
		if p.skLength == segmentCount {
			candidates = append(candidates, p)
		}
	}

	// If unique match by segment count, done
	if len(candidates) == 1 {
		return candidates[0].typeName, nil
	}

	// SECONDARY: If multiple types have same segment count, use prefix matching
	if len(candidates) > 1 {
		// Sort candidates by prefix length descending (longest match wins)
		for i := 0; i < len(candidates); i++ {
			for j := i + 1; j < len(candidates); j++ {
				if len(candidates[j].prefix) > len(candidates[i].prefix) {
					candidates[i], candidates[j] = candidates[j], candidates[i]
				}
			}
		}

		for _, p := range candidates {
			if p.prefix == "" || strings.HasPrefix(skValue, p.prefix) {
				return p.typeName, nil
			}
		}
	}

	// FALLBACK: prefix-only match for edge cases (partial data)
	// Try matching any pattern by prefix if segment count didn't work
	for _, p := range patterns {
		if p.prefix != "" && strings.HasPrefix(skValue, p.prefix) {
			return p.typeName, nil
		}
	}

	// Last resort: if there's a pattern with empty prefix, use it
	for _, p := range patterns {
		if p.prefix == "" {
			return p.typeName, nil
		}
	}

	return "", fmt.Errorf("no matching type for SK: %s (segments: %d)", skValue, segmentCount)
}

// hasPatterns returns true if the discriminator has patterns for any index.
func (td *typeDiscriminator) hasPatterns() bool {
	for _, patterns := range td.patternsByIndex {
		if len(patterns) > 0 {
			return true
		}
	}
	return false
}

// typesForIndex returns the type names registered for a given index.
func (td *typeDiscriminator) typesForIndex(indexName string) []string {
	patterns := td.patternsByIndex[indexName]
	if patterns == nil {
		return nil
	}

	types := make([]string, len(patterns))
	for i, p := range patterns {
		types[i] = p.typeName
	}
	return types
}
