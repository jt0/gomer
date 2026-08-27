package dynamodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

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
	"github.com/jt0/gomer/log"
	"github.com/jt0/gomer/structs"
)

func Stores() map[string]data.Store {
	stores := make(map[string]data.Store, len(tables))
	for k, t := range tables {
		stores[k] = t
	}
	return stores
}

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
	typeDiscriminator           *typeDiscriminator // For multi-type queries with nested Queryables
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

var tables = make(map[string]*table)

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

type ItemResolver func(any) (any, gomerr.Gomerr)

func Store(tableName string, config *Configuration /* resolver data.ItemResolver,*/) (store data.Store, ge gomerr.Gomerr) {
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

	var validOrDefaultChar = func(ch byte, _default byte) (byte, gomerr.Gomerr) {
		if ch != 0 {
			for _, sch := range []byte(SymbolChars) {
				if ch == sch {
					return ch, nil
				}
			}
			return 0, gomerr.Configuration("character " + string(ch) + " not in the valid set: " + SymbolChars)
		}
		return _default, nil
	}

	if t.valueSeparatorChar, ge = validOrDefaultChar(config.ValueSeparatorChar, ValueSeparatorCharDefault); ge != nil {
		return nil, ge
	}

	// Validate separator is in printable range and can have an escape character
	if t.valueSeparatorChar <= 32 || t.valueSeparatorChar >= 126 {
		return nil, gomerr.Configuration("ValueSeparatorChar must be > 32 (space) and < 126 (tilde)").
			AddAttribute("char", t.valueSeparatorChar)
	}

	// Escape character is the next ASCII character after the separator
	t.escapeChar = t.valueSeparatorChar + 1

	if t.queryWildcardChar, ge = validOrDefaultChar(config.QueryWildcardChar, QueryWildcardCharDefault); ge != nil {
		return nil, ge
	}

	if ge = t.prepare(); ge != nil {
		return nil, ge
	}
	tables[tableName] = t

	return t, nil
}

func (t *table) prepare() gomerr.Gomerr {
	input := &dynamodb.DescribeTableInput{TableName: t.tableName}
	output, err := t.ddb.DescribeTable(context.Background(), input)
	if err != nil {
		if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
			return gomerr.Unprocessable("table", *t.tableName).Wrap(err)
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

	t.constraintTool = NewConstraintTool(t)

	return nil
}

func (t *table) AddPersistables(persistables ...data.Persistable) gomerr.Gomerr {
	for _, p := range persistables {
		pType := reflect.TypeOf(p)
		pElem := pType.Elem()

		pt, ge := newPersistableType(t, p.TypeName(), pElem)
		if ge != nil {
			return ge
		}

		// Validate that each key in each index has fully defined key fields for this persistable
		for _, idx := range t.indexes {
			for _, attribute := range idx.keyAttributes() {
				// NB: require types to declare all key fields, even if name matches what's defined by ddb
				// TODO: support 'KeyField string `db.keys=""`' to avoid re-typing field name
				if keyFields := attribute.keyFieldsByPersistable[p.TypeName()]; keyFields != nil {
					for i, kf := range keyFields {
						if kf == nil {
							return gomerr.Configuration(fmt.Sprintf("index %s: key field %s.%d missing for type %s ",
								idx.friendlyName(), attribute.name, i, p.TypeName()),
							).AddAttribute("keyFields", toNames(keyFields))
						}
					}
				}
			}
		}

		if ge = structs.Preprocess(p, t.constraintTool); ge != nil {
			return ge.AddAttribute("typeName", p.TypeName())
		}

		t.persistableTypes[p.TypeName()] = pt
	}
	return nil
}

func toNames(keyFields []*keyField) []string {
	var kfNames []string
	for _, k := range keyFields {
		if k == nil {
			kfNames = append(kfNames, "?")
		} else {
			kfNames = append(kfNames, k.name)
		}
	}
	return kfNames
}

func (t *table) Name() string {
	return *t.tableName
}

func (t *table) Create(ctx context.Context, p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			// Todo: is this needed or should this just be added to the attributes?
			ge = dataerr.Store("Create", p, ge)
		}
	}()

	// Always validate constraints on create
	ge = t.put(ctx, p, true, true)

	return
}

func (t *table) Update(ctx context.Context, p data.Persistable, u data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = dataerr.Store("Update", p, ge)
		}
	}()

	// TODO:p1 support partial update vs put()

	validate := false
	if p != u && u != nil {
		validate, ge = t.mergeFields(reflect.ValueOf(p).Elem(), reflect.ValueOf(u).Elem(), t.persistableTypes[p.TypeName()])
		if ge != nil {
			return
		}
	}

	return t.put(ctx, p, validate, false)
}

var conditionalCheckFailure = constraint.New("uniqueKeys", nil, func(toTest any) gomerr.Gomerr {
	if ccf := gomerr.ErrorAs[*types.ConditionalCheckFailedException](toTest.(error)); ccf != nil {
		return constraint.NotSatisfied(ccf)
	}
	return nil
})

func (t *table) put(ctx context.Context, p data.Persistable, validateConstraints bool, ensureUniqueId bool) gomerr.Gomerr {
	// Validate constraints using tool framework
	if validateConstraints {
		if ge := structs.ApplyTools(p, structs.EnsureContext().With("ctx", ctx), t.constraintTool); ge != nil {
			return ge
		}
	}

	avm, err := attributevalue.MarshalMap(p)
	if err != nil {
		return gomerr.Marshal(p.TypeName(), p).Wrap(err)
	}

	pt := t.persistableTypes[p.TypeName()]
	pt.convertFieldNamesToDbNames(&avm)

	for _, i := range t.indexes {
		_ = i.populateKeyValues(avm, p, t.valueSeparatorChar, false)
	}

	// Remove key fields from attributes - they're stored in composite keys only
	// TODO:p1 this should be configurable or performed only if STD is being used with names not present in the persistable
	pt.removeKeyFieldsFromAttributes(&avm)

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

	if log.DebugEnabled() {
		attrs := append([]any{}, "pk", avToStr(avm[t.pk.name]))
		if t.sk != nil {
			attrs = append(attrs, "sk", avToStr(avm[t.sk.name]))
		}
		for an, av := range avm {
			if strings.HasSuffix(an, "_sk") {
				attrs = append(attrs, "an", avToStr(av))
			}
		}
		if ensureUniqueId {
			attrs = append(attrs, "condition", *uniqueIdConditionExpression)
			log.Debug("[gomer.ddb] create", attrs...)
		} else {
			log.Debug("[gomer.ddb] update", attrs...)
		}
	}

	input := &dynamodb.PutItemInput{
		Item:                avm,
		TableName:           t.tableName,
		ConditionExpression: uniqueIdConditionExpression,
	}
	_, err = t.ddb.PutItem(ctx, input) // TODO:p3 look at result data to track capacity or other info?
	if err != nil {
		if ge := conditionalCheckFailure.Test(err); ge != nil {
			return ge
		}

		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
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
			ge = dataerr.Store("Read", p, ge)
		}
	}()

	if key := t.itemKey(p); key == nil {
		ge = t.queryOne(ctx, p)
	} else {
		ge = t.readOne(ctx, p, key)
	}

	if ge != nil {
		return ge
	}

	// Check for nested Queryables (including autopopulated) and execute queries for each
	for _, nested := range t.prepareNestedQueryables(p) {
		if ge = t.querySingleType(ctx, nested.queryable); ge != nil {
			return ge
		}
	}

	return nil
}

func (t *table) itemKey(p data.Persistable) map[string]types.AttributeValue {
	key := make(map[string]types.AttributeValue, 2)
	_ = t.populateKeyValues(key, p, t.valueSeparatorChar, false)

	pk, ok := key[t.pk.name].(*types.AttributeValueMemberS)
	if !ok || keyPartIncomplete(pk.Value, t.valueSeparatorChar) {
		return nil
	}

	if t.sk == nil {
		return key
	}

	sk, ok := key[t.sk.name].(*types.AttributeValueMemberS)
	if !ok || keyPartIncomplete(sk.Value, t.valueSeparatorChar) {
		return nil
	}

	return key
}

func keyPartIncomplete(s string, sep byte) bool {
	return len(s) == 0 || s[len(s)-1] == sep || bytes.Contains([]byte(s), []byte{sep, sep})
}

func (t *table) queryOne(ctx context.Context, p data.Persistable) gomerr.Gomerr {
	pt, ok := t.persistableTypes[p.TypeName()]
	if !ok {
		return gomerr.Configuration("no persistable type for " + p.TypeName())
	}

	fields := make([]string, 0, len(pt.keyFields))
	for field := range pt.keyFields {
		fields = append(fields, field)
	}

	q, ge := toQueryable(p, fields)
	if ge != nil {
		return ge
	}

	if ge = t.Query(ctx, q); ge != nil {
		return ge
	}

	results := q.Results()
	if len(results) == 0 {
		return dataerr.PersistableNotFound(p)
	} else if len(results) > 1 {
		return gomerr.Conflict(p.TypeName(), "", "multiple_matches")
	}

	copyFields(reflect.ValueOf(p).Elem(), reflect.ValueOf(results[0]).Elem())

	return nil
}

func (t *table) readOne(ctx context.Context, p data.Persistable, key map[string]types.AttributeValue) gomerr.Gomerr {
	input := &dynamodb.GetItemInput{
		Key:            key,
		ConsistentRead: consistentRead(t.consistencyType(p), true),
		TableName:      t.tableName,
	}

	if log.DebugEnabled() {
		attrs := append([]any{}, t.pk.name, avToStr(key[t.pk.name]))
		if t.sk != nil {
			attrs = append(attrs, t.sk.name, avToStr(key[t.sk.name]))
		}
		log.Debug("[gomer.ddb] get", attrs...)
	}

	output, err := t.ddb.GetItem(ctx, input)
	if err != nil {
		if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
			return dataerr.PersistableNotFound(p).Wrap(err)
		}
		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
			switch apiErr.ErrorCode() {
			case "RequestLimitExceeded", "ProvisionedThroughputExceededException":
				return limit.UnquantifiedExcess("DynamoDB", "throughput").Wrap(err)
			}
		}
		return gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	if output.Item == nil {
		return dataerr.PersistableNotFound(p)
	}

	err = attributevalue.UnmarshalMap(output.Item, p)
	if err != nil {
		return gomerr.Unmarshal(p.TypeName(), output.Item, p).Wrap(err)
	}

	// Populate key fields from composite keys
	pt := t.persistableTypes[p.TypeName()]
	if ge := pt.populateKeyFieldsFromAttributes(p, output.Item, t.indexes, t.valueSeparatorChar, t.validateKeyFieldConsistency); ge != nil {
		return ge
	}
	return nil
}

var queryableType = reflect.TypeFor[data.Queryable]()
var timeType = reflect.TypeFor[time.Time]()

// copyFields copies field values from src to dst, skipping fields that implement Queryable or are tagged with
// `structs:"ignore"`. It recurses into struct and pointer-to-struct fields to handle embedded types that may
// contain skippable fields. Slices, maps, and interfaces are copied as-is without inspecting their contents.
func copyFields(dst, src reflect.Value) {
	for i := 0; i < dst.NumField(); i++ {
		f := dst.Type().Field(i)
		df := dst.Field(i)
		sf := src.FieldByName(f.Name)

		if !f.IsExported() || sf.IsZero() || f.Tag.Get("structs") == "ignore" || f.Type.Implements(queryableType) {
			continue
		}

		switch f.Type.Kind() {
		case reflect.Struct:
			if f.Type == timeType {
				df.Set(sf)
			} else {
				copyFields(df, sf)
			}
		case reflect.Ptr:
			if f.Type.Elem().Kind() == reflect.Struct && !df.IsNil() && !sf.IsNil() && f.Type.Elem() != timeType {
				copyFields(df.Elem(), sf.Elem())
			} else {
				df.Set(sf)
			}
		default:
			df.Set(sf)
		}
	}
}

func (t *table) Delete(ctx context.Context, p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = dataerr.Store("Delete", p, ge)
		}
	}()

	// TODO:p2 support a soft-delete option

	key := make(map[string]types.AttributeValue, 2)
	if ge = t.populateKeyValues(key, p, t.valueSeparatorChar, true); ge != nil {
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

	if log.DebugEnabled() {
		attrs := append([]any{}, "pk", avToStr(key["pk"]))
		if av, ok := key["sk"]; ok {
			attrs = append(attrs, "sk", avToStr(av))
		}
		log.Debug("[gomer.ddb] delete", attrs...)
	}

	_, err := t.ddb.DeleteItem(ctx, input)
	if err != nil {
		if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
			return dataerr.PersistableNotFound(p).Wrap(err)
		} else if _, ok = errors.AsType[*types.ConditionalCheckFailedException](err); ok {
			return dataerr.PersistableNotFound(p).Wrap(err)
		}

		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
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
			ge = dataerr.Store("Query", q, ge)
		}
	}()

	// Check for nested Queryables (enables multi-type STD queries)
	nested := nestedQueryables(q)
	if len(nested) > 0 {
		return t.queryWithNested(ctx, q, nested)
	}

	// Standard single-type query path
	var input *dynamodb.QueryInput
	input, ge = t.buildQueryInput(ctx, q)
	if ge != nil {
		return ge
	}

	var output *dynamodb.QueryOutput
	output, ge = t.runQuery(ctx, input)
	if ge != nil {
		return ge
	}

	nt, ge := t.nextTokenizer.Tokenize(ctx, q, output.LastEvaluatedKey)
	if ge != nil {
		return gomerr.Internal("unable to generate nextToken").Wrap(ge)
	}

	items := make([]any, len(output.Items))
	for i, item := range output.Items {
		name := q.TypeName()
		pt := t.persistableTypes[name]

		var resolvedItem any
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

	q.SetResults(items)
	q.SetNextPageToken(nt)

	return nil
}

// checkFieldTupleUnique validates that the given field tuple is unique by querying DynamoDB.
// This is called from the constraint tool during put() operations.
func (t *table) checkFieldTupleUnique(ctx context.Context, p data.Persistable, fields []string) gomerr.Gomerr {
	// Create queryable from persistable
	q, ge := toQueryable(p, fields)
	if ge != nil {
		return ge
	}

	// Build query
	input, ge := t.buildQueryInput(ctx, q)
	if ge != nil {
		return ge
	}

	// Query with progressive limit increases
	for queryLimit := int32(1); queryLimit <= 300; queryLimit += 100 {
		input.Limit = &queryLimit

		var output *dynamodb.QueryOutput
		if output, ge = t.runQuery(ctx, input); ge != nil {
			return ge
		}

		// If any results found, uniqueness violated
		if len(output.Items) > 0 {
			output.Items = output.Items[:1] // only want the first
			item := output.Items[0]
			pt := t.persistableTypes[p.TypeName()]

			var existing any
			if existing, ge = pt.resolver(item); ge != nil {
				return ge
			}

			// Populate key fields from composite keys
			if pe, ok := existing.(data.Persistable); ok {
				if ge = pt.populateKeyFieldsFromAttributes(pe, item, t.indexes, t.valueSeparatorChar, t.validateKeyFieldConsistency); ge != nil {
					return ge
				}
			}

			q.SetResults([]any{existing})

			return constraint.NotSatisfied(p).AddAttribute("existing", existing)
		}

		// No more pages, confirmed unique
		if output.LastEvaluatedKey == nil {
			return nil
		}

		input.ExclusiveStartKey = output.LastEvaluatedKey
	}

	return gomerr.Unprocessable("too many database checks to verify uniqueness constraint", p)
}

func toQueryable(p data.Persistable, fields []string) (data.Queryable, gomerr.Gomerr) {
	q := p.NewQueryable()
	if q == nil {
		return nil, gomerr.Configuration("unable to create queryable for uniqueness check").AddAttribute("type", p.TypeName())
	}

	// Set consistency preference
	if ct, ok := q.(ConsistencyTyper); ok {
		ct.SetConsistencyType(Preferred)
	}

	// Copy field values from persistable to queryable
	qv := reflect.ValueOf(q.ItemTemplate()).Elem()
	pv := reflect.ValueOf(p).Elem()
	for _, field := range fields {
		qfv := qv.FieldByName(field)
		pfv := pv.FieldByName(field)
		if qfv.IsValid() && pfv.IsValid() {
			if ge := flect.SetValue(qfv, pfv); ge != nil {
				return nil, gomerr.Configuration("unable to populate uniqueness").Wrap(ge)
			}
		}
	}
	return q, nil
}

// buildQueryInput Builds the DynamoDB QueryInput types based on the provided queryable. See indexFor and
// nextTokenizer.Untokenize for possible error types.
func (t *table) buildQueryInput(ctx context.Context, q data.Queryable) (*dynamodb.QueryInput, gomerr.Gomerr) {
	idx, ascending, consistent, ge := indexFor(t, q)
	if ge != nil {
		return nil, ge
	}

	expressionAttributeNames := make(map[string]string, 2)
	expressionAttributeValues := make(map[string]types.AttributeValue, 2)

	// TODO: any reason Elem() would be incorrect?
	qElem := reflect.ValueOf(q.ItemTemplate()).Elem()

	keyConditionExpression := safeName(idx.pk.name, expressionAttributeNames) + "=:pk"
	expressionAttributeValues[":pk"] = idx.pk.attributeValue(qElem, q.TypeName(), t.valueSeparatorChar, 0) // Non-null because indexFor succeeded

	// TODO: customers should opt-in to wildcard matches on a field-by-field basis
	// TODO: need to provide a way to sanitize, both when saving and querying data, the delimiter char
	if idx.sk != nil {
		if eav := idx.sk.attributeValue(qElem, q.TypeName(), t.valueSeparatorChar, t.queryWildcardChar); eav != nil {
			switch av := eav.(type) {
			case *types.AttributeValueMemberS:
				if v := av.Value; v[len(v)-1] == t.queryWildcardChar || v[len(v)-1] == t.valueSeparatorChar {
					// Remove the last char and apply begins_with if non-zero. If it is zero, then exclude
					// the sort key altogether.
					if v = v[:len(v)-1]; v != "" {
						keyConditionExpression += " AND begins_with(" + safeName(idx.sk.name, expressionAttributeNames) + ",:sk)"
						av.Value = v
						expressionAttributeValues[":sk"] = av
					}
				} else {
					keyConditionExpression += " AND " + safeName(idx.sk.name, expressionAttributeNames) + "=:sk"
					expressionAttributeValues[":sk"] = av
				}
			default:
				keyConditionExpression += " AND " + safeName(idx.sk.name, expressionAttributeNames) + "=:sk"
				expressionAttributeValues[":sk"] = eav
			}
		}
	}

	var fe string
	if fe, ge = t.filterExpression(q, idx, expressionAttributeNames, expressionAttributeValues); ge != nil {
		return nil, ge
	}

	if log.DebugEnabled() {
		idxName := "table"
		if idx.name != nil {
			idxName = *idx.name
		}

		attrs := make([]any, 0, 2*len(expressionAttributeValues)+2)
		attrs = append(attrs, "idx", idxName, "exp", keyConditionExpression, ":pk", avToStr(expressionAttributeValues[":pk"]))
		if av, ok := expressionAttributeValues[":sk"]; ok {
			attrs = append(attrs, ":sk", avToStr(av))
		}
		if fe != "" {
			attrs = append(attrs, "filter", fe)
			for k, av := range expressionAttributeValues {
				if k == ":pk" || k == ":sk" {
					continue
				}
				attrs = append(attrs, k, avToStr(av))
			}
		}
		log.Debug("[gomer.ddb] query", attrs...)
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

	exclusiveStartKey, ge := t.nextTokenizer.Untokenize(ctx, q)
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
		FilterExpression:          ptrOrNil(fe),
		ExclusiveStartKey:         exclusiveStartKey,
		Limit:                     t.limit(q.MaximumPageSize()),
		// ProjectionExpression:      projectionExpressionPtr,
		ScanIndexForward: &ascending,
	}

	return input, nil
}

func ptrOrNil[T any](t T) *T {
	if reflect.ValueOf(t).IsZero() {
		return nil
	}
	return &t
}

func (t *table) filterExpression(q data.Queryable, idx *index, expressionAttributeNames map[string]string, expressionAttributeValues map[string]types.AttributeValue) (string, gomerr.Gomerr) {
	qv, ge := flect.IndirectValue(q.ItemTemplate(), false)
	if ge != nil {
		return "", ge
	}

	keyFields := map[string]bool{}
	for _, ka := range idx.keyAttributes() {
		for _, kf := range ka.keyFieldsByPersistable[q.TypeName()] {
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
		if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
			if input.IndexName != nil {
				return nil, gomerr.Unprocessable("table Index", *input.IndexName).Wrap(err)
			}
			return nil, gomerr.Unprocessable("table", *t.tableName).Wrap(err)
		}

		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
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
	}
	return t.defaultConsistencyType
}

func (t *table) limit(maximumPageSize int) *int32 {
	if maximumPageSize > 0 && t.maxLimit != nil {
		mps32 := int32(maximumPageSize)
		maxLimit32 := int32(*t.maxLimit)
		if mps32 <= maxLimit32 {
			return &mps32
		}
		return &maxLimit32
	} else if t.defaultLimit != nil {
		return new(int32(*t.defaultLimit))
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

func avToStr(v types.AttributeValue) string {
	switch tv := v.(type) {
	case *types.AttributeValueMemberS:
		return tv.Value
	case *types.AttributeValueMemberN:
		return tv.Value
	default:
		return ""
	}
}
