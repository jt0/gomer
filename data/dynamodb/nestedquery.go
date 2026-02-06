package dynamodb

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jt0/gomer/flect"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

// paginationContext tracks item counts for nested Queryable pagination.
// It ensures we respect MaxPageSize limits on both parent and nested Queryables.
type paginationContext struct {
	parentType    string
	parentMax     int
	parentCount   int
	nestedMax     map[string]int // typeName -> max items
	nestedCount   map[string]int // typeName -> current count
	moreAvailable bool
}

// newPaginationContext creates a pagination context from parent Queryable and nested infos.
func newPaginationContext(parentType string, parentQ data.Queryable, nested []nestedQueryableInfo) *paginationContext {
	pc := &paginationContext{
		parentType:  parentType,
		parentMax:   parentQ.MaximumPageSize(),
		nestedMax:   make(map[string]int),
		nestedCount: make(map[string]int),
	}

	for _, n := range nested {
		pc.nestedMax[n.queryable.TypeName()] = n.queryable.MaximumPageSize()
		pc.nestedCount[n.queryable.TypeName()] = 0
	}

	return pc
}

// combinedLimit calculates a reasonable limit for the DynamoDB query.
// Uses a multiplier to reduce round-trips when interleaved data is expected.
func (pc *paginationContext) combinedLimit() int32 {
	total := pc.parentMax
	for _, nMax := range pc.nestedMax {
		total += nMax
	}
	// Apply multiplier to account for interleaving
	// This is a heuristic - may need tuning based on actual usage patterns
	return int32(total * 2)
}

// tryInclude attempts to include an item in results, respecting limits.
// Returns true if item should be included, false if limit reached.
func (pc *paginationContext) tryInclude(typeName string) bool {
	if typeName == pc.parentType {
		if pc.parentCount >= pc.parentMax {
			return false
		}
		pc.parentCount++
		return true
	}

	nMax, hasLimit := pc.nestedMax[typeName]
	if !hasLimit {
		// Unknown nested type - include it (no limit)
		return true
	}

	count := pc.nestedCount[typeName]
	if count >= nMax {
		return false
	}
	pc.nestedCount[typeName] = count + 1
	return true
}

// queryWithNested handles queries when the Queryable has nested Queryables.
// It attempts to use a shared index for a single DynamoDB query, falling back
// to separate queries if no shared index exists.
func (t *table) queryWithNested(ctx context.Context, q data.Queryable, nested []nestedQueryableInfo) gomerr.Gomerr {
	parentType := q.TypeName()
	childTypes := make([]string, len(nested))
	for i, n := range nested {
		childTypes[i] = n.queryable.TypeName()
	}

	// Try to find a shared index
	idx, ascending, consistent, ge := indexForMultiple(t, parentType, childTypes, q)
	if ge != nil {
		return ge
	}

	if idx == nil {
		// No shared index - fall back to separate queries
		return t.queryWithNestedFallback(ctx, q, nested)
	}

	// Build query input for multi-type query
	input, ge := t.buildMultiTypeQueryInput(ctx, q, nested, idx, ascending, consistent)
	if ge != nil {
		return ge
	}

	// Execute query
	output, ge := t.runQuery(ctx, input)
	if ge != nil {
		return ge
	}

	// Route items to correct Queryables
	return t.routeQueryResults(ctx, q, nested, idx, output)
}

// queryWithNestedFallback executes separate queries when no shared index exists.
// First queries the parent, then queries each nested type separately.
func (t *table) queryWithNestedFallback(ctx context.Context, q data.Queryable, nested []nestedQueryableInfo) gomerr.Gomerr {
	// Query parent type normally
	if ge := t.querySingleType(ctx, q); ge != nil {
		return ge
	}

	// For each nested Queryable, query separately
	// Note: This requires the nested Queryable to have its key fields populated
	// from the parent - this is a limitation of the fallback approach
	for _, n := range nested {
		if ge := t.querySingleType(ctx, n.queryable); ge != nil {
			return ge
		}
	}

	return nil
}

// querySingleType performs a standard single-type query.
// This is extracted from the original Query method for reuse.
func (t *table) querySingleType(ctx context.Context, q data.Queryable) gomerr.Gomerr {
	input, ge := t.buildQueryInput(ctx, q)
	if ge != nil {
		return ge
	}

	output, ge := t.runQuery(ctx, input)
	if ge != nil {
		return ge
	}

	nt, ge := t.nextTokenizer.tokenize(ctx, q, output.LastEvaluatedKey)
	if ge != nil {
		return gomerr.Internal("Unable to generate nextToken").Wrap(ge)
	}

	items := make([]interface{}, len(output.Items))
	for i, item := range output.Items {
		pt := t.persistableTypes[q.TypeName()]

		var resolvedItem interface{}
		if resolvedItem, ge = pt.resolver(item); ge != nil {
			return ge
		}

		if p, ok := resolvedItem.(data.Persistable); ok {
			if ge = pt.populateKeyFieldsFromAttributes(p, item, t.indexes, t.valueSeparatorChar, t.validateKeyFieldConsistency); ge != nil {
				return ge
			}
		}

		items[i] = resolvedItem
	}

	q.SetItems(items)
	q.SetNextPageToken(nt)

	return nil
}

// buildMultiTypeQueryInput builds a QueryInput that can return multiple types.
// Uses begins_with on the parent's SK prefix to fetch parent and children together.
func (t *table) buildMultiTypeQueryInput(ctx context.Context, q data.Queryable, nested []nestedQueryableInfo, idx *index, ascending bool, consistent *bool) (*dynamodb.QueryInput, gomerr.Gomerr) {
	expressionAttributeNames := make(map[string]string, 2)
	expressionAttributeValues := make(map[string]types.AttributeValue, 2)

	qElem := reflect.ValueOf(q).Elem()

	// Build PK condition (same as regular query)
	keyConditionExpression := safeName(idx.pk.name, expressionAttributeNames) + "=:pk"
	expressionAttributeValues[":pk"] = idx.pk.attributeValue(qElem, q.TypeName(), t.valueSeparatorChar, 0)

	// Build SK condition using common prefix for multi-type matching
	if idx.sk != nil {
		prefix := commonKeyPrefix(idx, q.TypeName(), qElem, t.valueSeparatorChar, t.escapeChar)
		if prefix != "" {
			// Remove trailing separator for begins_with (it's already included for matching)
			if prefix[len(prefix)-1] == t.valueSeparatorChar {
				prefix = prefix[:len(prefix)-1]
			}
			if prefix != "" {
				keyConditionExpression += " AND begins_with(" + safeName(idx.sk.name, expressionAttributeNames) + ",:sk)"
				expressionAttributeValues[":sk"] = &types.AttributeValueMemberS{Value: prefix}
			}
		}
	}

	if len(expressionAttributeNames) == 0 {
		expressionAttributeNames = nil
	}

	exclusiveStartKey, ge := t.nextTokenizer.untokenize(ctx, q)
	if ge != nil {
		return nil, ge
	}

	// Calculate combined limit based on parent and nested maximums
	pc := newPaginationContext(q.TypeName(), q, nested)
	limit := pc.combinedLimit()

	input := &dynamodb.QueryInput{
		TableName:                 t.tableName,
		IndexName:                 idx.name,
		ConsistentRead:            consistent,
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		KeyConditionExpression:    &keyConditionExpression,
		ExclusiveStartKey:         exclusiveStartKey,
		Limit:                     &limit,
		ScanIndexForward:          &ascending,
	}

	return input, nil
}

// routeQueryResults routes query results to the parent Queryable and nested Queryables.
// Uses type discrimination to determine which items belong to which Queryable.
func (t *table) routeQueryResults(ctx context.Context, q data.Queryable, nested []nestedQueryableInfo, idx *index, output *dynamodb.QueryOutput) gomerr.Gomerr {
	// Build type discriminator if not already built
	if t.typeDiscriminator == nil {
		t.typeDiscriminator = t.buildTypeDiscriminator()
	}

	// Create pagination context
	pc := newPaginationContext(q.TypeName(), q, nested)

	// Prepare item slices for each Queryable
	parentItems := make([]interface{}, 0)
	nestedItems := make(map[string][]interface{})
	for _, n := range nested {
		nestedItems[n.queryable.TypeName()] = make([]interface{}, 0)
	}

	// Get index name and SK attribute name for discrimination
	indexName := ""
	if idx.name != nil {
		indexName = *idx.name
	}
	skAttrName := ""
	if idx.sk != nil {
		skAttrName = idx.sk.name
	}

	// Process each item
	for _, item := range output.Items {
		// Discriminate type
		typeName, err := t.typeDiscriminator.discriminate(item, indexName, skAttrName)
		if err != nil {
			// If discrimination fails, skip item (could log warning)
			continue
		}

		// Check if we should include this item (respects limits)
		if !pc.tryInclude(typeName) {
			pc.moreAvailable = true
			continue
		}

		// Get persistable type and resolve item
		pt := t.persistableTypes[typeName]
		if pt == nil {
			continue
		}

		resolvedItem, ge := pt.resolver(item)
		if ge != nil {
			return ge
		}

		// Populate key fields
		if p, ok := resolvedItem.(data.Persistable); ok {
			if ge = pt.populateKeyFieldsFromAttributes(p, item, t.indexes, t.valueSeparatorChar, t.validateKeyFieldConsistency); ge != nil {
				return ge
			}
		}

		// Route to correct slice
		if typeName == q.TypeName() {
			parentItems = append(parentItems, resolvedItem)
		} else {
			nestedItems[typeName] = append(nestedItems[typeName], resolvedItem)
		}
	}

	// Set items on parent Queryable
	q.SetItems(parentItems)

	// Set items on nested Queryables
	for _, n := range nested {
		items := nestedItems[n.queryable.TypeName()]
		n.queryable.SetItems(items)
	}

	// Handle pagination token
	nt, ge := t.nextTokenizer.tokenize(ctx, q, output.LastEvaluatedKey)
	if ge != nil {
		return gomerr.Internal("Unable to generate nextToken").Wrap(ge)
	}
	q.SetNextPageToken(nt)

	return nil
}

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
	prefix   string // Static prefix from key fields (e.g., "E#" for Extension)
	skLength int    // Expected number of SK segments for this type
	typeName string // The persistableType name
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

// nestedQueryableInfo holds information about a nested Queryable field within a struct.
// This is used to detect and process child Queryables in Single Table Design patterns.
type nestedQueryableInfo struct {
	fieldName  string         // Name of the field in parent struct
	fieldIndex int            // Index for efficient field access via reflection
	queryable  data.Queryable // The nested Queryable instance (always non-nil)
}

// nestedQueryables scans a struct for non-nil Queryable fields. Returns a slice of nestedQueryableInfo for
// fields that should be included in the query. Nil Queryable fields are skipped (the user can set a Queryable
// field to non-nil to opt in to nested querying, typically in a PreRead hook).
func nestedQueryables(a any) []nestedQueryableInfo { // TODO: tighten parameter type?
	v, ge := flect.IndirectValue(a, false)
	if ge != nil {
		// TODO: add error
		return nil
	}

	var result []nestedQueryableInfo

	// Get underlying struct (handle pointer)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil
	}

	t := v.Type()
	queryableType := reflect.TypeOf((*data.Queryable)(nil)).Elem()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fv := v.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Handle embedded anonymous structs recursively
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			embedded := nestedQueryables(fv)
			result = append(result, embedded...)
			continue
		}

		// Handle embedded pointer to struct
		if field.Anonymous && field.Type.Kind() == reflect.Pointer && field.Type.Elem().Kind() == reflect.Struct {
			if !fv.IsNil() {
				embedded := nestedQueryables(fv)
				result = append(result, embedded...)
			}
			continue
		}

		// Check if field type implements Queryable
		fieldType := field.Type
		isPointer := fieldType.Kind() == reflect.Pointer
		if isPointer {
			fieldType = fieldType.Elem()
		}

		// Check both direct and pointer implementation
		implementsQueryable := fieldType.Implements(queryableType) ||
			reflect.PointerTo(fieldType).Implements(queryableType)
		if !implementsQueryable {
			continue
		}

		// Check if non-nil (should be included)
		if isPointer && fv.IsNil() {
			continue // Skip nil Queryables - not opted-in
		}

		// Get the Queryable instance
		var q data.Queryable
		if isPointer {
			q = fv.Interface().(data.Queryable)
		} else {
			// Value type - need to take address
			if fv.CanAddr() {
				q = fv.Addr().Interface().(data.Queryable)
			} else {
				// Cannot take address of field - skip
				continue
			}
		}

		result = append(result, nestedQueryableInfo{
			fieldName:  field.Name,
			fieldIndex: i,
			queryable:  q,
		})
	}

	return result
}
