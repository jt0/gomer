package dynamodb

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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
	for _, max := range pc.nestedMax {
		total += max
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

	max, hasLimit := pc.nestedMax[typeName]
	if !hasLimit {
		// Unknown nested type - include it (no limit)
		return true
	}

	count := pc.nestedCount[typeName]
	if count >= max {
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
			if ge := pt.populateKeyFieldsFromAttributes(p, item, t.indexes, t.valueSeparatorChar, t.validateKeyFieldConsistency); ge != nil {
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

// hasNestedQueryables checks if a Queryable has any non-nil nested Queryable fields.
func hasNestedQueryables(q data.Queryable) bool {
	nested := detectNestedQueryables(reflect.ValueOf(q))
	return len(nested) > 0
}

// getNestedQueryables returns the nested Queryable infos for a Queryable.
func getNestedQueryables(q data.Queryable) []nestedQueryableInfo {
	return detectNestedQueryables(reflect.ValueOf(q))
}
