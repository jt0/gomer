package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/crypto"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/data/dataerr"
	"github.com/jt0/gomer/flect"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/limit"
	"github.com/jt0/gomer/structs"
)

type table struct {
	index
	tableName                   *string
	ddb                         *dynamodb.Client
	defaultLimit                *int64
	maxLimit                    *int64
	defaultConsistencyType      ConsistencyType
	indexes                     map[string]*index
	persistableTypes            map[string]*persistableType
	nextTokenizer               nextTokenizer
	valueSeparatorChar          byte
	escapeChar                  byte
	failDeleteIfNotPresent      bool
	validateKeyFieldConsistency bool
	constraintTool              *structs.Tool
}

type Configuration struct {
	DynamoDb                    *dynamodb.Client
	MaxResultsDefault           int64
	MaxResultsMax               int64
	ConsistencyDefault          ConsistencyType
	NextTokenCipher             crypto.Cipher
	ValueSeparatorChar          byte
	QueryWildcardChar           byte
	FailDeleteIfNotPresent      bool
	ValidateKeyFieldConsistency bool
}

var tables = make(map[string]data.Store)

type ConsistencyType int

const (
	Indifferent ConsistencyType = iota
	Required
	Preferred

	SymbolChars                    = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`"
	ValueSeparatorCharDefault      = '#'
	QueryWildcardCharDefault  byte = 0
)

const maxItemSize = limit.DataSize(400 * 1024)

type ConsistencyTyper interface {
	ConsistencyType() ConsistencyType
	SetConsistencyType(consistencyType ConsistencyType)
}

type ItemResolver func(interface{}) (interface{}, gomerr.Gomerr)

func Store(tableName string, config *Configuration /* resolver data.ItemResolver,*/, persistables ...data.Persistable) (store data.Store, ge gomerr.Gomerr) {
	t := &table{
		tableName:                   &tableName,
		index:                       index{canReadConsistently: true},
		ddb:                         config.DynamoDb,
		defaultLimit:                &config.MaxResultsDefault,
		maxLimit:                    &config.MaxResultsMax,
		defaultConsistencyType:      config.ConsistencyDefault,
		indexes:                     make(map[string]*index),
		persistableTypes:            make(map[string]*persistableType),
		nextTokenizer:               nextTokenizer{cipher: config.NextTokenCipher},
		failDeleteIfNotPresent:      config.FailDeleteIfNotPresent,
		validateKeyFieldConsistency: config.ValidateKeyFieldConsistency,
	}

	if t.valueSeparatorChar, ge = validOrDefaultChar(config.ValueSeparatorChar, ValueSeparatorCharDefault); ge != nil {
		return nil, ge
	}

	// Validate separator is in printable range and can have an escape character
	if t.valueSeparatorChar <= 32 || t.valueSeparatorChar >= 126 {
		return nil, gomerr.Configuration("ValueSeparatorChar must be > 32 (space) and < 126 (tilde)").
			AddAttribute("ValueSeparatorChar", t.valueSeparatorChar)
	}

	// Escape character is the next ASCII character after the separator
	t.escapeChar = t.valueSeparatorChar + 1

	if t.queryWildcardChar, ge = validOrDefaultChar(config.QueryWildcardChar, QueryWildcardCharDefault); ge != nil {
		return nil, ge
	}

	if ge = t.prepare(persistables); ge != nil {
		return nil, ge
	}

	// Initialize constraint tool and preprocess persistable types
	t.constraintTool = NewConstraintTool(t)
	for _, p := range persistables {
		if ge := structs.Preprocess(p, t.constraintTool); ge != nil {
			return nil, ge.AddAttribute("TypeName", p.TypeName())
		}
	}

	tables[tableName] = t

	return t, nil
}

func validOrDefaultChar(ch byte, _default byte) (byte, gomerr.Gomerr) {
	if ch != 0 {
		s := string(ch)
		if strings.Contains(SymbolChars, s) {
			return ch, nil
		} else {
			return 0, gomerr.Configuration("QueryWildcardChar " + s + " not in the valid set: " + SymbolChars)
		}
	} else {
		return _default, nil
	}
}

func Stores() map[string]data.Store {
	return tables
}

func (t *table) prepare(persistables []data.Persistable) gomerr.Gomerr {
	input := &dynamodb.DescribeTableInput{TableName: t.tableName}
	output, err := t.ddb.DescribeTable(context.Background(), input)
	if err != nil {
		var notFoundErr *types.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			return gomerr.Unprocessable("Table", *t.tableName).Wrap(err)
		}

		return gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	attributeTypes := make(map[string]string)
	for _, at := range output.Table.AttributeDefinitions {
		attributeTypes[*at.AttributeName] = string(at.AttributeType)
	}

	if ge := t.index.processKeySchema(output.Table.KeySchema, attributeTypes); ge != nil {
		return ge
	}

	t.indexes[""] = &t.index

	for _, lsid := range output.Table.LocalSecondaryIndexes {
		lsi := &index{
			name:                lsid.IndexName,
			canReadConsistently: true,
			queryWildcardChar:   t.queryWildcardChar,
		}

		if ge := lsi.processKeySchema(lsid.KeySchema, attributeTypes); ge != nil {
			return ge
		}

		lsi.pk = t.pk // Overwrite w/ t.pk

		t.indexes[*lsid.IndexName] = lsi
	}

	for _, gsid := range output.Table.GlobalSecondaryIndexes {
		gsi := &index{
			name:                gsid.IndexName,
			canReadConsistently: false,
			queryWildcardChar:   t.queryWildcardChar,
		}

		if ge := gsi.processKeySchema(gsid.KeySchema, attributeTypes); ge != nil {
			return ge
		}

		t.indexes[*gsid.IndexName] = gsi
	}

	for _, persistable := range persistables {
		pType := reflect.TypeOf(persistable)
		pElem := pType.Elem()

		unqualifiedPersistableName := pElem.String()
		unqualifiedPersistableName = unqualifiedPersistableName[strings.Index(unqualifiedPersistableName, ".")+1:]

		pt, ge := newPersistableType(t, unqualifiedPersistableName, pElem)
		if ge != nil {
			return ge
		}

		// Validate that each key in each index has fully defined key fields for this persistable
		for _, idx := range t.indexes {
			for _, attribute := range idx.keyAttributes() {
				if keyFields := attribute.keyFieldsByPersistable[unqualifiedPersistableName]; keyFields != nil {
					for i, kf := range keyFields {
						if kf == nil {
							return gomerr.Configuration(
								fmt.Sprintf("Index %s is missing a key field: %s[%s][%d]", idx.friendlyName(), attribute.name, unqualifiedPersistableName, i),
							).AddAttribute("keyFields", keyFields)
						}
					}
				} else {
					attribute.keyFieldsByPersistable[unqualifiedPersistableName] = []*keyField{{name: pt.dbNameToFieldName(attribute.name), ascending: true}}
				}
			}
		}

		t.persistableTypes[unqualifiedPersistableName] = pt
	}

	return nil
}

func (t *table) Name() string {
	return *t.tableName
}

func (t *table) Create(ctx context.Context, p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			// Todo: is this needed or should this just be added to the attributes?
			ge = dataerr.Store("Create", p).Wrap(ge)
		}
	}()

	// Always validate constraints on create
	ge = t.put(ctx, p, true, true)

	return
}

func (t *table) Update(ctx context.Context, p data.Persistable, update data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = dataerr.Store("Update", p).Wrap(ge)
		}
	}()

	// TODO:p1 support partial update vs put()

	validateConstraints := false
	pt := t.persistableTypes[p.TypeName()]

	if update != nil {
		uv := reflect.ValueOf(update).Elem()
		pv := reflect.ValueOf(p).Elem()

		for i := 0; i < uv.NumField(); i++ {
			uField := uv.Field(i)
			// TODO:p0 Support structs. Will want to recurse through and not bother w/ CanSet() checks until we know
			//         we're dealing w/ a scalar.
			if !uField.CanSet() || uField.Kind() == reflect.Struct || (uField.Kind() == reflect.Ptr && uField.Elem().Kind() == reflect.Struct) {
				continue
			}

			pField := pv.Field(i)
			fieldName := uv.Type().Field(i).Name

			if reflect.DeepEqual(uField.Interface(), pField.Interface()) {
				uField.Set(reflect.Zero(uField.Type()))
			} else if uField.Kind() == reflect.Ptr {
				if uField.IsNil() {
					continue
				}
				if !pField.IsNil() && reflect.DeepEqual(uField.Elem().Interface(), pField.Elem().Interface()) {
					uField.Set(reflect.Zero(uField.Type()))
				} else {
					pField.Set(uField)
					// Check if this field participates in any constraint
					if pt.constraintFields[fieldName] {
						validateConstraints = true
					}
				}
			} else {
				if uField.IsZero() {
					continue
				}
				pField.Set(uField)
				// Check if this field participates in any constraint
				if pt.constraintFields[fieldName] {
					validateConstraints = true
				}
			}
		}
	}

	ge = t.put(ctx, p, validateConstraints, false)

	return
}

func (t *table) put(ctx context.Context, p data.Persistable, validateConstraints bool, ensureUniqueId bool) gomerr.Gomerr {
	// Validate constraints using tool framework
	if validateConstraints {
		tc := structs.EnsureContext(nil).Put("ctx", ctx)
		if ge := structs.ApplyTools(p, tc, t.constraintTool); ge != nil {
			return ge
		}
	}

	av, err := attributevalue.MarshalMap(p)
	if err != nil {
		return gomerr.Marshal(p.TypeName(), p).Wrap(err)
	}

	pt := t.persistableTypes[p.TypeName()]
	pt.convertFieldNamesToDbNames(&av)

	for _, i := range t.indexes {
		_ = i.populateKeyValues(av, p, t.valueSeparatorChar, false)
	}

	// Remove key fields from attributes - they're stored in composite keys only
	pt.removeKeyFieldsFromAttributes(&av)

	// TODO: here we could compare the current av map w/ one we stashed into the object somewhere

	var uniqueIdConditionExpression *string
	if ensureUniqueId {
		expression := fmt.Sprintf("attribute_not_exists(%s)", t.pk.name)
		if t.sk != nil {
			expression += fmt.Sprintf(" AND attribute_not_exists(%s)", t.sk.name)
		}
		uniqueIdConditionExpression = &expression
	}

	// TODO:p1 optimistic locking

	input := &dynamodb.PutItemInput{
		Item:                av,
		TableName:           t.tableName,
		ConditionExpression: uniqueIdConditionExpression,
	}
	_, err = t.ddb.PutItem(ctx, input) // TODO:p3 look at result data to track capacity or other info?
	if err != nil {
		var condCheckErr *types.ConditionalCheckFailedException
		if errors.As(err, &condCheckErr) {
			if ensureUniqueId {
				return gomerr.Internal("Unique id check failed, retry with a new id value").Wrap(err)
			} else {
				return gomerr.Dependency("DynamoDB", input).Wrap(err)
			}
		}

		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "RequestLimitExceeded", "ProvisionedThroughputExceededException":
				return limit.UnquantifiedExcess("DynamoDB", "throughput").Wrap(err)
			case "ItemCollectionSizeLimitExceededException":
				return limit.Exceeded("DynamoDB", "item.size()", maxItemSize, limit.NotApplicable, limit.Unknown)
			}
		}

		return gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	return nil
}

func (t *table) Read(ctx context.Context, p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = dataerr.Store("Read", p).Wrap(ge)
		}
	}()

	key := make(map[string]types.AttributeValue, 2)
	ge = t.populateKeyValues(key, p, t.valueSeparatorChar, true)
	if ge != nil {
		return ge
	}

	input := &dynamodb.GetItemInput{
		Key:            key,
		ConsistentRead: consistentRead(t.consistencyType(p), true),
		TableName:      t.tableName,
	}
	output, err := t.ddb.GetItem(ctx, input)
	if err != nil {
		var notFoundErr *types.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			return dataerr.PersistableNotFound(p.TypeName(), key).Wrap(err)
		}

		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "RequestLimitExceeded", "ProvisionedThroughputExceededException":
				return limit.UnquantifiedExcess("DynamoDB", "throughput").Wrap(err)
			}
		}

		return gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	if output.Item == nil {
		return dataerr.PersistableNotFound(p.TypeName(), key)
	}

	err = attributevalue.UnmarshalMap(output.Item, p)
	if err != nil {
		return gomerr.Unmarshal(p.TypeName(), output.Item, p).Wrap(err)
	}

	// Populate key fields from composite keys
	pt := t.persistableTypes[p.TypeName()]
	if ge = pt.populateKeyFieldsFromAttributes(p, output.Item, t.indexes, t.valueSeparatorChar, t.validateKeyFieldConsistency); ge != nil {
		return ge
	}

	return nil
}

func (t *table) Delete(ctx context.Context, p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = dataerr.Store("Delete", p).Wrap(ge)
		}
	}()

	// TODO:p2 support a soft-delete option

	key := make(map[string]types.AttributeValue, 2)
	ge = t.populateKeyValues(key, p, t.valueSeparatorChar, true)
	if ge != nil {
		return ge
	}

	var existenceCheckExpression *string
	if t.failDeleteIfNotPresent {
		expression := fmt.Sprintf("attribute_exists(%s)", t.pk.name)
		if t.sk != nil {
			expression += fmt.Sprintf(" AND attribute_exists(%s)", t.sk.name)
		}
		existenceCheckExpression = &expression
	}

	input := &dynamodb.DeleteItemInput{
		Key:                 key,
		TableName:           t.tableName,
		ConditionExpression: existenceCheckExpression,
	}
	_, err := t.ddb.DeleteItem(ctx, input)
	if err != nil {
		var notFoundErr *types.ResourceNotFoundException
		var condCheckErr *types.ConditionalCheckFailedException
		if errors.As(err, &notFoundErr) || errors.As(err, &condCheckErr) {
			return dataerr.PersistableNotFound(p.TypeName(), key).Wrap(err)
		}

		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "RequestLimitExceeded", "ProvisionedThroughputExceededException":
				return limit.UnquantifiedExcess("DynamoDB", "throughput").Wrap(err)
			}
		}

		return gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	return nil
}

func (t *table) Query(ctx context.Context, q data.Queryable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = dataerr.Store("Query", q).Wrap(ge)
		}
	}()

	var input *dynamodb.QueryInput
	input, ge = t.buildQueryInput(ctx, q, q.TypeNames()[0]) // TODO:p2 Fix when query supports multiple types
	if ge != nil {
		return ge
	}

	var output *dynamodb.QueryOutput
	output, ge = t.runQuery(ctx, input)
	if ge != nil {
		return ge
	}

	nt, ge := t.nextTokenizer.tokenize(ctx, q, output.LastEvaluatedKey)
	if ge != nil {
		return gomerr.Internal("Unable to generate nextToken").Wrap(ge)
	}

	items := make([]interface{}, len(output.Items))
	for i, item := range output.Items {
		typeName := q.TypeOf(item)
		pt := t.persistableTypes[typeName]

		var resolvedItem interface{}
		if resolvedItem, ge = pt.resolver(item); ge != nil {
			return ge
		}

		// Populate key fields from composite keys
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

func (t *table) isFieldTupleUnique(fields []string) func(pi interface{}) gomerr.Gomerr {
	return func(pi interface{}) gomerr.Gomerr {
		p, ok := pi.(data.Persistable)
		if !ok {
			return gomerr.Unprocessable("Test value is not a data.Persistable", pi)
		}

		q := p.NewQueryable()
		if ct, ok := q.(ConsistencyTyper); ok {
			ct.SetConsistencyType(Preferred)
		}

		qv := reflect.ValueOf(q).Elem()
		pv := reflect.ValueOf(p).Elem()
		for _, field := range fields {
			qv.FieldByName(field).Set(pv.FieldByName(field))
		}

		ctx := context.Background() // Use background context for constraint validation
		input, ge := t.buildQueryInput(ctx, q, p.TypeName())
		if ge != nil {
			return ge
		}

		for queryLimit := int32(1); queryLimit <= 300; queryLimit += 100 { // Bump limit up each time
			input.Limit = &queryLimit

			output, queryErr := t.runQuery(ctx, input)
			if queryErr != nil {
				return queryErr
			}

			if len(output.Items) > 0 {
				newP := reflect.New(pv.Type()).Elem()
				err := attributevalue.UnmarshalMap(output.Items[0], newP)
				return constraint.NotSatisfied(pi).AddAttribute("Existing", newP).Wrap(err)
			}

			if output.LastEvaluatedKey == nil {
				return nil
			}

			input.ExclusiveStartKey = output.LastEvaluatedKey
		}

		return gomerr.Unprocessable("Too many db checks to verify uniqueness constraint", pi)
	}
}

// checkFieldTupleUnique validates that the given field tuple is unique by querying DynamoDB.
// This is called from the constraint tool during put() operations.
func (t *table) checkFieldTupleUnique(ctx context.Context, p data.Persistable, fields []string) gomerr.Gomerr {
	// Create queryable from persistable
	q := p.NewQueryable()
	if q == nil {
		return gomerr.Configuration("unable to create queryable for uniqueness check").
			AddAttribute("Type", p.TypeName())
	}

	// Set consistency preference
	if ct, ok := q.(ConsistencyTyper); ok {
		ct.SetConsistencyType(Preferred)
	}

	// Copy field values from persistable to queryable
	qv := reflect.ValueOf(q).Elem()
	pv := reflect.ValueOf(p).Elem()
	for _, field := range fields {
		fv := pv.FieldByName(field)
		if fv.IsValid() {
			qv.FieldByName(field).Set(fv)
		}
	}

	// Build query
	input, ge := t.buildQueryInput(ctx, q, p.TypeName())
	if ge != nil {
		return ge
	}

	// Query with progressive limit increases
	for queryLimit := int32(1); queryLimit <= 300; queryLimit += 100 {
		input.Limit = &queryLimit
		output, err := t.runQuery(ctx, input)
		if err != nil {
			return err
		}

		// If any results found, uniqueness violated
		if len(output.Items) > 0 {
			existing := reflect.New(pv.Type()).Interface()
			if unmarshalErr := attributevalue.UnmarshalMap(output.Items[0], existing); unmarshalErr != nil {
				return gomerr.Configuration("unable to unmarshal existing record").Wrap(unmarshalErr)
			}
			return constraint.NotSatisfied(p).AddAttribute("Existing", existing)
		}

		// No more pages, confirmed unique
		if output.LastEvaluatedKey == nil {
			return nil
		}

		input.ExclusiveStartKey = output.LastEvaluatedKey
	}

	return gomerr.Unprocessable("too many database checks to verify uniqueness constraint", p)
}

type UniqueConstraint struct {
	constraint.Constraint
}

// buildQueryInput Builds the DynamoDB QueryInput types based on the provided queryable. See indexFor and
// nextTokenizer.untokenize for possible error types.
func (t *table) buildQueryInput(ctx context.Context, q data.Queryable, persistableTypeName string) (*dynamodb.QueryInput, gomerr.Gomerr) {
	idx, ascending, consistent, ge := indexFor(t, q)
	if ge != nil {
		return nil, ge
	}

	expressionAttributeNames := make(map[string]string, 2)
	expressionAttributeValues := make(map[string]types.AttributeValue, 2)

	// TODO: any reason Elem() would be incorrect?
	qElem := reflect.ValueOf(q).Elem()

	keyConditionExpression := safeName(idx.pk.name, expressionAttributeNames) + "=:pk"
	expressionAttributeValues[":pk"] = idx.pk.attributeValue(qElem, persistableTypeName, t.valueSeparatorChar, 0) // Non-null because indexFor succeeded

	// TODO: customers should opt-in to wildcard matches on a field-by-field basis
	// TODO: need to provide a way to sanitize, both when saving and querying data, the delimiter char
	if idx.sk != nil {
		if eav := idx.sk.attributeValue(qElem, persistableTypeName, t.valueSeparatorChar, t.queryWildcardChar); eav != nil {
			if s, ok := eav.(*types.AttributeValueMemberS); ok {
				if len(s.Value) > 0 && (s.Value[len(s.Value)-1] == t.queryWildcardChar || s.Value[len(s.Value)-1] == t.valueSeparatorChar) {
					s.Value = s.Value[:len(s.Value)-1] // remove the last char
					keyConditionExpression += " AND begins_with(" + safeName(idx.sk.name, expressionAttributeNames) + ",:sk)"
				} else {
					keyConditionExpression += " AND " + safeName(idx.sk.name, expressionAttributeNames) + "=:sk"
				}
			}
			expressionAttributeValues[":sk"] = eav
		}
	}

	var filterExpression *string
	if fe, ge := t.filterExpression(q, idx, persistableTypeName, expressionAttributeNames, expressionAttributeValues); ge != nil {
		return nil, ge
	} else if fe != "" {
		filterExpression = &fe
	}

	// for _, attribute := range q.ResponseFields() {
	// 	safeName(attribute, expressionAttributeNames)
	// }

	if len(expressionAttributeNames) == 0 {
		expressionAttributeNames = nil
	}

	// TODO:p2 projectionExpression
	// var projectionExpressionPtr *string
	// projectionExpression := strings.Join(attributes, ",") // Join() returns "" if len(attributes) == 0
	// if projectionExpression != "" {
	// 	projectionExpressionPtr = &projectionExpression
	// }

	exclusiveStartKey, ge := t.nextTokenizer.untokenize(ctx, q)
	if ge != nil {
		return nil, ge
	}

	input := &dynamodb.QueryInput{
		TableName:                 t.tableName,
		IndexName:                 idx.name,
		ConsistentRead:            consistent,
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		KeyConditionExpression:    &keyConditionExpression,
		FilterExpression:          filterExpression,
		ExclusiveStartKey:         exclusiveStartKey,
		Limit:                     t.limit(q.MaximumPageSize()),
		// ProjectionExpression:      projectionExpressionPtr,
		ScanIndexForward: &ascending,
	}

	return input, nil
}

func (t *table) filterExpression(q data.Queryable, idx *index, persistableTypeName string, expressionAttributeNames map[string]string, expressionAttributeValues map[string]types.AttributeValue) (string, gomerr.Gomerr) {
	qv, ge := flect.IndirectValue(q, false)
	if ge != nil {
		return "", ge
	}

	keyFields := map[string]bool{}
	for _, ka := range idx.keyAttributes() {
		for _, kf := range ka.keyFieldsByPersistable[persistableTypeName] {
			keyFields[kf.name] = true
		}
	}

	var exp string
	qt := qv.Type()
	for i := 0; i < qt.NumField(); i++ {
		var qfv reflect.Value
		var sf reflect.StructField
		if sf = qt.Field(i); keyFields[sf.Name] {
			continue
		} else if qfv = qv.Field(i); qfv.IsZero() {
			continue
		}
		if qfv.Kind() == reflect.Ptr {
			qfv = qfv.Elem()
		}
		if qfv.Kind() == reflect.Struct {
			continue
		}
		s := fmt.Sprint(qfv.Interface())
		if len(s) == 0 {
			continue
		}
		if len(exp) > 0 {
			exp += " AND "
		}
		filterAlias := ":f" + strconv.Itoa(i)
		if s[len(s)-1] == t.queryWildcardChar {
			s = s[:len(s)-1]
			exp += "begins_with(" + safeName(sf.Name, expressionAttributeNames) + "," + filterAlias + ")"
		} else {
			exp += safeName(sf.Name, expressionAttributeNames) + "=" + filterAlias
		}
		expressionAttributeValues[filterAlias] = &types.AttributeValueMemberS{Value: s}
	}

	return exp, nil
}

func (t *table) runQuery(ctx context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, gomerr.Gomerr) {
	output, err := t.ddb.Query(ctx, input)
	if err != nil {
		var notFoundErr *types.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			if input.IndexName != nil {
				return nil, gomerr.Unprocessable("Table Index", *input.IndexName).Wrap(err)
			} else {
				return nil, gomerr.Unprocessable("Table", *t.tableName).Wrap(err)
			}
		}

		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "RequestLimitExceeded", "ProvisionedThroughputExceededException":
				return nil, limit.UnquantifiedExcess("DynamoDB", "throughput").Wrap(err)
			}
		}

		return nil, gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	return output, nil
}

func (t *table) consistencyType(p data.Persistable) ConsistencyType {
	if ct, ok := p.(ConsistencyTyper); ok {
		return ct.ConsistencyType()
	} else {
		return t.defaultConsistencyType
	}
}

func (t *table) limit(maximumPageSize int) *int32 {
	if maximumPageSize > 0 && t.maxLimit != nil {
		mps32 := int32(maximumPageSize)
		maxLimit32 := int32(*t.maxLimit)
		if mps32 <= maxLimit32 {
			return &mps32
		} else {
			return &maxLimit32
		}
	} else if t.defaultLimit != nil {
		defaultLimit32 := int32(*t.defaultLimit)
		return &defaultLimit32
	}
	return nil
}

func safeName(attributeName string, expressionAttributeNames map[string]string) string {
	// TODO: calculate once and store in persistableType
	if reservedWords[strings.ToUpper(attributeName)] || strings.ContainsAny(attributeName, ". ") || attributeName[0] >= '0' || attributeName[0] <= '9' {
		replacement := "#a" + strconv.Itoa(len(expressionAttributeNames))
		expressionAttributeNames[replacement] = attributeName
		return replacement
	}
	return attributeName
}

var (
	trueVal  = true
	falseVal = false
)

func consistentRead(consistencyType ConsistencyType, canReadConsistently bool) *bool {
	switch consistencyType {
	case Indifferent:
		return &falseVal
	case Required:
		return &trueVal
	case Preferred:
		return &canReadConsistently
	default:
		return nil
	}
}
