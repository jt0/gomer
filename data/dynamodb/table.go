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

	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/crypto"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
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
)

type ConsistencyTyper interface {
	ConsistencyType() ConsistencyType
	SetConsistencyType(consistencyType ConsistencyType)
}

func Store(tableName string, config *Configuration, persistables ...data.Persistable) (data.Store, gomerr.Gomerr) {
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
				return gomerr.NotFound("ddb.Table", *t.tableName).WithCause(awsErr).AddCulprit(gomerr.Configuration)
			}
		}

		return gomerr.Dependency(err, input)
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
		pType := reflect.TypeOf(persistable).Elem()
		pName := strings.ToLower(util.UnqualifiedTypeName(pType))

		pt, ge := newPersistableType(pName, pType, t.indexes)
		if ge != nil {
			return ge
		}

		// Validate that each key in each index has fully defined key fields for this persistable
		for _, index := range t.indexes {
			for _, keyAttribute := range index.keyAttributes() {
				if keyParts := keyAttribute.keyFieldsByPersistable[pName]; keyParts != nil {
					for i, keyPart := range keyParts {
						if keyPart == "" {
							return gomerr.BadValue(keyAttribute.name+"["+pName+"]["+strconv.Itoa(i)+"]", keyParts, constraint.NonZero())
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

func (t *table) Create(p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = data.CreateFailed(ge, p)
		}
	}()

	ge = t.put(p, t.persistableTypes[p.PersistableTypeName()].uniqueFields, true)

	return
}

var zeroValue = reflect.Value{}

func (t *table) Update(p data.Persistable, update data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = data.UpdateFailed(ge, p, update)
		}
	}()

	// TODO:p1 support partial update vs put()

	updatedUniqueFields := make(map[string][]string)

	if update != nil {
		pv := reflect.ValueOf(p).Elem()
		uv := reflect.ValueOf(update).Elem()

		for i := 0; i < uv.NumField(); i++ {
			uField := uv.Field(i)
			if !uField.CanSet() || uField.IsZero() {
				continue
			}

			if pv.Field(i).Interface() == uv.Field(i).Interface() { // TODO: deal w/ pointers and structs
				uField.Set(reflect.Zero(uField.Type()))
			} else {
				pv.Field(i).Set(uField)
			}
		}

		for fieldName, additionalFields := range t.persistableTypes[p.PersistableTypeName()].uniqueFields {
			uField := uv.FieldByName(fieldName)
			if uField != zeroValue && !uField.IsZero() {
				updatedUniqueFields[fieldName] = additionalFields
			}
		}
	}

	ge = t.put(p, updatedUniqueFields, false)

	return
}

func (t *table) put(p data.Persistable, uniqueFields map[string][]string, ensureUniqueId bool) gomerr.Gomerr {
	pt := t.persistableTypes[p.PersistableTypeName()]

	for fieldName, additionalFields := range uniqueFields {
		if ge := t.constraintsCheck(p, fieldName, additionalFields); ge != nil {
			return ge
		}
	}

	av, err := dynamodbattribute.MarshalMap(p)
	if err != nil {
		return gomerr.Marshal(err, p)
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
					return gomerr.InternalServer(err).AddNotes("unique id check failed. retry with a new id").AddCulprit(gomerr.Internal)
				}
			case dynamodb.ErrCodeRequestLimitExceeded:
				fallthrough
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				fallthrough
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fallthrough
			case dynamodb.ErrCodeResourceNotFoundException:
				return gomerr.Dependency(err, input).AddCulprit(gomerr.Configuration)
			}
		}

		return gomerr.Dependency(err, input)
	}

	return nil
}

func (t *table) Read(p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = data.ReadFailed(ge, p)
		}
	}()

	keys := make(map[string]*dynamodb.AttributeValue, 2)
	ge = t.populateKeyValues(keys, p, t.valueSeparator, true)
	if ge != nil {
		return ge.AddNotes("cannot read persistable without key value(s)")
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
				return gomerr.NotFound(p.PersistableTypeName(), p.Id()).WithCause(err)
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fallthrough
			case dynamodb.ErrCodeRequestLimitExceeded:
				return gomerr.Dependency(err, input).AddCulprit(gomerr.Configuration)
			}
		}

		return gomerr.Dependency(err, input)
	}

	if output.Item == nil {
		return gomerr.NotFound(p.PersistableTypeName(), p.Id()).AddCulprit(gomerr.Client)
	}

	err = dynamodbattribute.UnmarshalMap(output.Item, p)
	if err != nil {
		return gomerr.Unmarshal(err, output.Item, p).AddCulprit(gomerr.Internal)
	}

	return nil
}

func (t *table) Delete(p data.Persistable) (ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = data.DeleteFailed(ge, p)
		}
	}()

	// TODO:p2 support a soft-delete option

	keys := make(map[string]*dynamodb.AttributeValue, 2)
	ge = t.populateKeyValues(keys, p, t.valueSeparator, true)
	if ge != nil {
		return ge.AddNotes("cannot delete persistable without key value(s)")
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
			case dynamodb.ErrCodeResourceNotFoundException, dynamodb.ErrCodeConditionalCheckFailedException:
				return gomerr.NotFound(p.PersistableTypeName(), p.Id()).WithCause(err)
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException, dynamodb.ErrCodeProvisionedThroughputExceededException, dynamodb.ErrCodeRequestLimitExceeded:
				return gomerr.Dependency(err, input).AddCulprit(gomerr.Configuration)
			}
		}

		return gomerr.Dependency(err, input)
	}

	return nil
}

func (t *table) Query(q data.Queryable, arrayOfPersistable interface{}) (nextToken *string, ge gomerr.Gomerr) {
	defer func() {
		if ge != nil {
			ge = data.QueryFailed(ge, q)
		}
	}()

	var input *dynamodb.QueryInput
	input, ge = t.buildQueryInput(q)
	if ge != nil {
		return nil, ge.AddCulprit(gomerr.Internal)
	}

	var output *dynamodb.QueryOutput
	output, ge = t.runQuery(input)
	if ge != nil {
		return nil, ge
	}

	nextToken, ge = t.nextTokenizer.tokenize(q, output.LastEvaluatedKey)
	if ge != nil {
		return nil, ge.AddCulprit(gomerr.Internal)
	}

	err := dynamodbattribute.UnmarshalListOfMaps(output.Items, arrayOfPersistable)
	if err != nil {
		return nil, gomerr.Unmarshal(err, output.Items, arrayOfPersistable).AddCulprit(gomerr.Internal)
	}

	return nextToken, nil
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

	qi, ge := t.buildQueryInput(q)
	if ge != nil {
		return ge.AddCulprit(gomerr.Internal)
	}

	for queryLimit := int64(1); queryLimit <= 300; queryLimit += 100 { // Bump limit up each time
		qi.Limit = &queryLimit

		qo, ge := t.runQuery(qi)
		if ge != nil {
			return ge
		}

		if len(qo.Items) > 0 {
			var by interface{}

			arrayOfPersistable := util.EmptySliceForType(reflect.TypeOf(p))
			err := dynamodbattribute.UnmarshalListOfMaps(qo.Items, arrayOfPersistable)
			if err != nil {
				by = qo.Items[0]
			} else {
				slice := reflect.ValueOf(arrayOfPersistable).Elem()
				pOther, ok := slice.Index(0).Interface().(data.Persistable)
				if !ok {
					by = qo.Items[0]
				} else {
					by = pOther.Id()
				}
			}

			return data.ConstraintViolation(data.Unique, fieldName, by).AddCulprit(gomerr.Client)
		}

		if qo.LastEvaluatedKey == nil {
			return nil
		}

		qi.ExclusiveStartKey = qo.LastEvaluatedKey
	}

	return data.QueryFailed(nil, q).AddNotes("unable to verify constraint", "too many paginated calls to DDB").AddCulprit(gomerr.Configuration)
}

func (t *table) buildQueryInput(q data.Queryable) (*dynamodb.QueryInput, gomerr.Gomerr) {
	index, consistent, ge := indexFor(t, q)
	if ge != nil {
		return nil, ge
	}

	expressionAttributeNames := make(map[string]*string, 2)
	expressionAttributeValues := make(map[string]*dynamodb.AttributeValue, 2)

	keyConditionExpresion := safeName(index.pk.name, expressionAttributeNames) + "=:pk"
	expressionAttributeValues[":pk"], ge = index.pk.attributeValue(q, t.valueSeparator, true)
	if ge != nil {
		return nil, ge.AddNotes("cannot perform query without partition key")
	}

	if index.sk != nil {
		if av, _ := index.sk.attributeValue(q, t.valueSeparator, false); av != nil {
			if av.S != nil && strings.HasSuffix(*av.S, ":") {
				trimmed := strings.Trim(*av.S, ":")
				av.S = &trimmed
				keyConditionExpresion += " AND begins_with(" + safeName(index.sk.name, expressionAttributeNames) + ",:sk)"
			} else {
				keyConditionExpresion += " AND " + safeName(index.sk.name, expressionAttributeNames) + "=:sk"
			}
			expressionAttributeValues[":sk"] = av
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
			case dynamodb.ErrCodeConditionalCheckFailedException:
				fallthrough
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				fallthrough
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fallthrough
			case dynamodb.ErrCodeRequestLimitExceeded:
				fallthrough
			case dynamodb.ErrCodeResourceNotFoundException:
				return nil, gomerr.Dependency(err, input).AddCulprit(gomerr.Configuration)
			}
		}

		return nil, gomerr.Dependency(err, input)
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

func (t *table) limit(maximumPageSize *int) *int64 {
	if maximumPageSize != nil && *maximumPageSize > 0 {
		mps64 := int64(*maximumPageSize)
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
