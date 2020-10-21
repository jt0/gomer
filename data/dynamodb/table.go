package dynamodb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/jt0/gomer/crypto"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	. "github.com/jt0/gomer/gomerr/because"
	"github.com/jt0/gomer/limit"
	"github.com/jt0/gomer/util"
)

type table struct {
	index

	tableName              *string
	ddb                    dynamodbiface.DynamoDBAPI
	defaultLimit           *int64
	maxLimit               *int64
	defaultConsistencyType ConsistencyType
	indexes                map[string]*index
	persistableTypes       map[string]*persistableType
	valueSeparator         string
	nextTokenizer          nextTokenizer
	failDeleteIfNotPresent bool
}

type Configuration struct {
	DynamoDb               dynamodbiface.DynamoDBAPI
	MaxResultsDefault      int64
	MaxResultsMax          int64
	ConsistencyDefault     ConsistencyType
	ValueSeparator         string
	NextTokenCipher        crypto.Cipher
	FailDeleteIfNotPresent bool
}

var tables = make(map[string]data.Store)

type ConsistencyType int

const (
	Indifferent ConsistencyType = iota
	Required
	Preferred

	maxItemSize = limit.DataSize(400 * 1024)
)

type ConsistencyTyper interface {
	ConsistencyType() ConsistencyType
	SetConsistencyType(consistencyType ConsistencyType)
}

type ItemResolver func(interface{}) (interface{}, gomerr.Gomerr)

func Store(
	tableName string,
	config *Configuration,
	/* resolver data.ItemResolver,*/
	persistables ...data.Persistable,
) (data.Store, gomerr.Gomerr) {
	table := &table{
		tableName:              &tableName,
		index:                  index{canReadConsistently: true},
		ddb:                    config.DynamoDb,
		defaultLimit:           &config.MaxResultsDefault,
		maxLimit:               &config.MaxResultsMax,
		defaultConsistencyType: config.ConsistencyDefault,
		indexes:                make(map[string]*index),
		persistableTypes:       make(map[string]*persistableType),
		valueSeparator:         config.ValueSeparator,
		nextTokenizer:          nextTokenizer{cipher: config.NextTokenCipher},
		failDeleteIfNotPresent: config.FailDeleteIfNotPresent,
	}

	if ge := table.prepare(persistables); ge != nil {
		return nil, ge
	}

	tables[tableName] = table

	return table, nil
}

func Stores() map[string]data.Store {
	return tables
}

func (t *table) prepare(persistables []data.Persistable) gomerr.Gomerr {
	input := &dynamodb.DescribeTableInput{TableName: t.tableName}
	output, err := t.ddb.DescribeTable(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				return gomerr.NotFound("ddb.Table", *t.tableName).Wrap(awsErr)
			}
		}

		return gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	attributeTypes := make(map[string]string)
	for _, at := range output.Table.AttributeDefinitions {
		attributeTypes[*at.AttributeName] = *at.AttributeType
	}

	if ge := t.index.processKeySchema(output.Table.KeySchema, attributeTypes); ge != nil {
		return ge
	}

	t.indexes[""] = &t.index

	for _, lsid := range output.Table.LocalSecondaryIndexes {
		lsi := &index{
			name:                lsid.IndexName,
			canReadConsistently: true,
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
		}

		if ge := gsi.processKeySchema(gsid.KeySchema, attributeTypes); ge != nil {
			return ge
		}

		t.indexes[*gsid.IndexName] = gsi
	}

	for _, persistable := range persistables {
		pType := reflect.TypeOf(persistable)
		pElem := pType.Elem()
		pName := util.UnqualifiedTypeName(pElem)

		pt, ge := newPersistableType(pName, pElem, t.indexes)
		if ge != nil {
			return ge
		}

		// Validate that each key in each index has fully defined key fields for this persistable
		for _, index := range t.indexes {
			for _, keyAttribute := range index.keyAttributes() {
				if keyParts := keyAttribute.keyFieldsByPersistable[pName]; keyParts != nil {
					for i, keyPart := range keyParts {
						if keyPart == "" {
							return gomerr.Missing(keyAttribute.name+"["+pName+"]["+strconv.Itoa(i)+"]", keyParts)
						}
					}
				} else {
					keyAttribute.keyFieldsByPersistable[pName] = []string{pt.dbNameToFieldName(keyAttribute.name)}
				}
			}
		}

		t.persistableTypes[pName] = pt
	}

	return nil
}

func (t *table) Name() string {
	return *t.tableName
}

func (t *table) Create(p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			// XXX: is this needed or should this just be added to the attributes
			ge = gomerr.Data("Create", p).Wrap(ge)
		}
	}()

	ge = t.put(p, t.persistableTypes[p.PersistableTypeName()].uniqueFields, true)

	return
}

var zeroValue = reflect.Value{}

func (t *table) Update(p data.Persistable, update data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = gomerr.Data("Update", p).Wrap(ge)
		}
	}()

	// TODO:p1 support partial update vs put()

	updatedUniqueFields := make(map[string][]string)

	if update != nil {
		uv := reflect.ValueOf(update).Elem()
		pv := reflect.ValueOf(p).Elem()

		for i := 0; i < uv.NumField(); i++ {
			uField := uv.Field(i)
			if !uField.CanSet() || uField.Kind() == reflect.Struct || (uField.Kind() == reflect.Ptr && uField.Elem().Kind() == reflect.Struct) { // TODO: structs
				continue
			}

			pField := pv.Field(i)
			if uField.Interface() == pField.Interface() {
				uField.Set(reflect.Zero(uField.Type()))
			} else if uField.Kind() != reflect.Ptr || !uField.IsNil() { // Only update if there's a different (first clause == false) and non-nil value
				pField.Set(uField)
			}
		}

		for fieldName, additionalFields := range t.persistableTypes[p.PersistableTypeName()].uniqueFields {
			if !uv.FieldByName(fieldName).IsZero() {
				updatedUniqueFields[fieldName] = additionalFields
			}
		}
	}

	ge = t.put(p, updatedUniqueFields, false)

	return
}

func (t *table) put(p data.Persistable, fieldsToCheckForUniqueness map[string][]string, ensureUniqueId bool) gomerr.Gomerr {
	pt := t.persistableTypes[p.PersistableTypeName()]

	for fieldName, additionalFields := range fieldsToCheckForUniqueness {
		if ge := t.constraintsCheck(p, fieldName, additionalFields); ge != nil {
			return ge
		}
	}

	av, err := dynamodbattribute.MarshalMap(p)
	if err != nil {
		return gomerr.Marshal(p.PersistableTypeName(), p).Wrap(err)
	}

	pt.convertFieldNamesToDbNames(&av)

	for _, index := range t.indexes {
		_ = index.populateKeyValues(av, p, t.valueSeparator, false)
	}

	// TODO: here we could compare the current av map w/ one we stashed into the object somewhere

	var uniqueIdConditionExpresion *string
	if ensureUniqueId {
		expression := fmt.Sprintf("attribute_not_exists(%s)", t.pk.name)
		if t.sk != nil {
			expression += fmt.Sprintf(" AND attribute_not_exists(%s)", t.sk.name)
		}
		uniqueIdConditionExpresion = &expression
	}

	// TODO:p1 optimistic locking

	input := &dynamodb.PutItemInput{
		Item:                av,
		TableName:           t.tableName,
		ConditionExpression: uniqueIdConditionExpresion,
	}
	_, err = t.ddb.PutItem(input) // TODO:p3 look at result data to track capacity or other info?
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				if ensureUniqueId {
					return gomerr.Internal("Unique id check failed, retry with a new id value").Wrap(err)
				} else {
					return gomerr.Dependency("DynamoDB", input).Wrap(err)
				}
			case dynamodb.ErrCodeRequestLimitExceeded, dynamodb.ErrCodeProvisionedThroughputExceededException:
				return limit.UnquantifiedExcess("DynamoDB", "throughput").Wrap(awsErr)
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				return limit.Exceeded("DynamoDB", "item.size()", maxItemSize, limit.NotApplicable, limit.Unknown)
			}
		}

		return gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	return nil
}

func (t *table) Read(p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = gomerr.Data("Read", p).Wrap(ge)
		}
	}()

	keys := make(map[string]*dynamodb.AttributeValue, 2)
	ge = t.populateKeyValues(keys, p, t.valueSeparator, true)
	if ge != nil {
		return ge
	}

	input := &dynamodb.GetItemInput{
		Key:            keys,
		ConsistentRead: consistentRead(t.consistencyType(p), true),
		TableName:      t.tableName,
	}
	output, err := t.ddb.GetItem(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				return gomerr.NotFound(p.PersistableTypeName(), p.Id()).Wrap(err)
			case dynamodb.ErrCodeRequestLimitExceeded, dynamodb.ErrCodeProvisionedThroughputExceededException:
				return limit.UnquantifiedExcess("DynamoDB", "throughput").Wrap(awsErr)
			}
		}

		return gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	if output.Item == nil {
		return gomerr.NotFound(p.PersistableTypeName(), p.Id())
	}

	err = dynamodbattribute.UnmarshalMap(output.Item, p)
	if err != nil {
		return gomerr.Unmarshal(p.PersistableTypeName(), output.Item, p).Wrap(err)
	}

	return nil
}

func (t *table) Delete(p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = gomerr.Data("Delete", p).Wrap(ge)
		}
	}()

	// TODO:p2 support a soft-delete option

	keys := make(map[string]*dynamodb.AttributeValue, 2)
	ge = t.populateKeyValues(keys, p, t.valueSeparator, true)
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
		Key:                 keys,
		TableName:           t.tableName,
		ConditionExpression: existenceCheckExpression,
	}
	_, err := t.ddb.DeleteItem(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				return gomerr.NotFound(p.PersistableTypeName(), p.Id()).Wrap(err)
			case dynamodb.ErrCodeRequestLimitExceeded, dynamodb.ErrCodeProvisionedThroughputExceededException:
				return limit.UnquantifiedExcess("DynamoDB", "throughput").Wrap(awsErr)
			}
		}

		return gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	return nil
}

func (t *table) Query(q data.Queryable) (items []interface{}, nextToken *string, ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = gomerr.Data("Query", q).Wrap(ge)
		}
	}()

	var input *dynamodb.QueryInput
	input, ge = t.buildQueryInput(q)
	if ge != nil {
		return nil, nil, ge
	}

	var output *dynamodb.QueryOutput
	output, ge = t.runQuery(input)
	if ge != nil {
		return nil, nil, ge
	}

	nextToken, ge = t.nextTokenizer.tokenize(q, output.LastEvaluatedKey)
	if ge != nil {
		return nil, nil, gomerr.Internal("Unable to generate nextToken").Wrap(ge)
	}

	items = make([]interface{}, len(output.Items))
	for i, item := range output.Items {
		if items[i], ge = t.persistableTypes[q.PersistableTypeName()].resolver(item); ge != nil {
			return nil, nil, ge
		}
	}

	return items, nextToken, nil
}

func (t *table) constraintsCheck(p data.Persistable, fieldName string, additionalFields []string) gomerr.Gomerr {
	q := p.NewQueryable()
	if ct, ok := p.(ConsistencyTyper); ok {
		ct.SetConsistencyType(Preferred)
	}

	qv := reflect.ValueOf(q).Elem()
	pv := reflect.ValueOf(p).Elem()

	qv.FieldByName(fieldName).Set(pv.FieldByName(fieldName))
	for _, additionalField := range additionalFields {
		qv.FieldByName(additionalField).Set(pv.FieldByName(additionalField))
	}

	input, ge := t.buildQueryInput(q)
	if ge != nil {
		return gomerr.Internal("Unable to build constraint query").Wrap(ge)
	}

	for queryLimit := int64(1); queryLimit <= 300; queryLimit += 100 { // Bump limit up each time
		input.Limit = &queryLimit

		output, ge := t.runQuery(input)
		if ge != nil {
			return ge
		}

		if len(output.Items) > 0 {
			var existingItem interface{}

			outSlice := util.EmptySliceForType(reflect.TypeOf(p))
			err := dynamodbattribute.UnmarshalListOfMaps(output.Items, outSlice)
			if err != nil {
				existingItem = output.Items[0]
			} else {
				slice := reflect.ValueOf(outSlice).Elem()
				pOther, ok := slice.Index(0).Interface().(data.Persistable)
				if !ok {
					existingItem = output.Items[0]
				} else {
					existingItem = pOther.Id()
				}
			}

			return gomerr.ConflictBetween(p, existingItem).Because(NotUnique).On(fieldName)
		}

		if output.LastEvaluatedKey == nil {
			return nil
		}

		input.ExclusiveStartKey = output.LastEvaluatedKey
	}

	return gomerr.Internal("Too many db checks to verify field constraint").AddAttribute("Field", fieldName)
}

// buildQueryInput Builds the DynamoDB QueryInput types based on the provided queryable. See indexFor and
// nextTokenizer.untokenize for possible error types.
func (t *table) buildQueryInput(q data.Queryable) (*dynamodb.QueryInput, gomerr.Gomerr) {
	index, consistent, ge := indexFor(t, q)
	if ge != nil {
		return nil, ge
	}

	expressionAttributeNames := make(map[string]*string, 2)
	expressionAttributeValues := make(map[string]*dynamodb.AttributeValue, 2)

	keyConditionExpresion := safeName(index.pk.name, expressionAttributeNames) + "=:pk"
	expressionAttributeValues[":pk"] = index.pk.attributeValue(q, t.valueSeparator) // Non-null because indexFor succeeded

	if index.sk != nil {
		if eav := index.sk.attributeValue(q, t.valueSeparator); eav != nil {
			if eav.S != nil && strings.HasSuffix(*eav.S, ":") {
				trimmed := strings.Trim(*eav.S, ":")
				eav.S = &trimmed
				keyConditionExpresion += " AND begins_with(" + safeName(index.sk.name, expressionAttributeNames) + ",:sk)"
			} else {
				keyConditionExpresion += " AND " + safeName(index.sk.name, expressionAttributeNames) + "=:sk"
			}
			expressionAttributeValues[":sk"] = eav
		}
	}

	//for _, attribute := range q.ResponseFields() {
	//	safeName(attribute, expressionAttributeNames)
	//}

	if len(expressionAttributeNames) == 0 {
		expressionAttributeNames = nil
	}

	// TODO: projectionExpression
	//var projectionExpressionPtr *string
	//projectionExpression := strings.Join(attributes, ",") // Join() returns "" if len(attributes) == 0
	//if projectionExpression != "" {
	//	projectionExpressionPtr = &projectionExpression
	//}

	exclusiveStartKey, ge := t.nextTokenizer.untokenize(q)
	if ge != nil {
		return nil, ge
	}

	input := &dynamodb.QueryInput{
		TableName:                 t.tableName,
		IndexName:                 index.name,
		ConsistentRead:            consistent,
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		KeyConditionExpression:    &keyConditionExpresion,
		FilterExpression:          nil,
		ExclusiveStartKey:         exclusiveStartKey,
		Limit:                     t.limit(q.MaximumPageSize()),
		//ProjectionExpression:      projectionExpressionPtr,
		//ScanIndexForward:          &scanIndexForward,
	}

	return input, nil
}

func (t *table) runQuery(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, gomerr.Gomerr) {
	output, err := t.ddb.Query(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// TODO: improve exceptions
			switch awsErr.Code() {
			case dynamodb.ErrCodeRequestLimitExceeded, dynamodb.ErrCodeProvisionedThroughputExceededException:
				return nil, limit.UnquantifiedExcess("DynamoDB", "throughput").Wrap(awsErr)
			case dynamodb.ErrCodeResourceNotFoundException:
				return nil, gomerr.Missing("Table (or table index)", input).Wrap(awsErr)
			}
		}

		return nil, gomerr.Dependency("DynamoDB", input).Wrap(err)
	}

	return output, nil
}

func (t *table) consistencyType(s data.Storable) ConsistencyType {
	if ct, ok := s.(ConsistencyTyper); ok {
		return ct.ConsistencyType()
	} else {
		return t.defaultConsistencyType
	}
}

func (t *table) limit(maximumPageSize int) *int64 {
	if maximumPageSize > 0 && t.maxLimit != nil {
		mps64 := int64(maximumPageSize)
		if mps64 <= *t.maxLimit {
			return &mps64
		} else {
			return t.maxLimit
		}
	} else {
		return t.defaultLimit
	}
}

func safeName(attributeName string, expressionAttributeNames map[string]*string) string {
	// TODO: calculate once and store in persistableType
	if _, reserved := reservedWords[strings.ToUpper(attributeName)]; reserved || strings.ContainsAny(attributeName, ". ") || attributeName[0] >= '0' || attributeName[0] <= '9' {
		replacement := "#a" + strconv.Itoa(len(expressionAttributeNames))
		expressionAttributeNames[replacement] = &attributeName

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
