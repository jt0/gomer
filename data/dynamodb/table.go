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
	"github.com/jt0/gomer/logs"
	"github.com/jt0/gomer/util"
)

type table struct {
	index

	ddb                    dynamodbiface.DynamoDBAPI
	defaultLimit           *int64
	maxLimit               *int64
	defaultConsistencyType ConsistencyType
	indexes                map[string]*index
	persistableTypes       map[string]*persistableType
	valueSeparator         string
	nextTokenizer          nextTokenizer
}

type Configuration struct {
	DynamoDb           dynamodbiface.DynamoDBAPI
	MaxResultsDefault  int64
	MaxResultsMax      int64
	ConsistencyDefault ConsistencyType
	ValueSeparator     string
	NextTokenCipher    crypto.Cipher
}

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

func Store(tableName string, config *Configuration, persistables ...data.Persistable) data.Store {
	tableIndexName := ""

	table := &table{
		index:                  index{tableName: &tableName, name: &tableIndexName, canReadConsistently: true},
		ddb:                    config.DynamoDb,
		defaultLimit:           &config.MaxResultsDefault,
		maxLimit:               &config.MaxResultsMax,
		defaultConsistencyType: config.ConsistencyDefault,
		indexes:                make(map[string]*index),
		persistableTypes:       make(map[string]*persistableType),
		valueSeparator:         config.ValueSeparator,
		nextTokenizer:          nextTokenizer{cipher: config.NextTokenCipher},
	}

	table.prepare(persistables)

	return table
}

func (t *table) prepare(persistables []data.Persistable) {
	input := &dynamodb.DescribeTableInput{TableName: t.tableName}
	output, err := t.ddb.DescribeTable(input)
	if err != nil {
		panic("Table inspection error")
	}

	attributeTypes := make(map[string]string)
	for _, at := range output.Table.AttributeDefinitions {
		attributeTypes[*at.AttributeName] = *at.AttributeType
	}

	t.index.processKeySchema(output.Table.KeySchema, attributeTypes)
	t.indexes[""] = &t.index

	for _, lsid := range output.Table.LocalSecondaryIndexes {
		lsi := &index{
			tableName:           t.tableName,
			name:                lsid.IndexName,
			canReadConsistently: true,
		}
		lsi.processKeySchema(lsid.KeySchema, attributeTypes)
		lsi.pk = t.pk // Overwrite w/ t.pk

		t.indexes[*lsid.IndexName] = lsi
	}

	for _, gsid := range output.Table.GlobalSecondaryIndexes {
		gsi := &index{
			tableName:           t.tableName,
			name:                gsid.IndexName,
			canReadConsistently: false,
		}
		gsi.processKeySchema(gsid.KeySchema, attributeTypes)

		t.indexes[*gsid.IndexName] = gsi
	}

	for _, persistable := range persistables {
		pType := reflect.TypeOf(persistable).Elem()
		pName := strings.ToLower(util.UnqualifiedTypeName(pType))

		pt := &persistableType{
			name:         pName,
			uniqueFields: make(map[string][]string, 0),
			dbNames:      make(map[string]string),
		}

		pt.processFields(pType, "", t.indexes)

		// Validate that each key in each index has fully defined key fields for this persistable
		for _, index := range t.indexes {
			for _, keyAttribute := range index.keyAttributes() {
				if keyParts := keyAttribute.compositeKeyParts[pName]; keyParts != nil {
					for i, keyPart := range keyParts {
						if keyPart == "" {
							panic(fmt.Sprintf("%s's compositeKeyPart #%d for key '%s' is missing", pName, i, keyAttribute.name))
						}
					}
				} else {
					if _, ok := pType.FieldByName(keyAttribute.name); ok {
						keyAttribute.compositeKeyParts[pName] = []string{keyAttribute.name}
					}
				}
			}
		}

		t.persistableTypes[pName] = pt
	}
}

func (t *table) Create(p data.Persistable) *gomerr.ApplicationError {
	return t.put(p, true)
}

func (t *table) Update(p data.Persistable, update data.Persistable) *gomerr.ApplicationError {
	// TODO:p1 support partial update vs put()
	if update != nil {
		pv := reflect.ValueOf(p).Elem()
		uv := reflect.ValueOf(update).Elem()

		for i := 0; i < uv.NumField(); i++ {
			uField := uv.Field(i)
			if !uField.CanSet() || uField.IsZero() {
				continue
			}

			pv.Field(i).Set(uField)
		}
	}

	return t.put(p, false)
}

func (t *table) put(p data.Persistable, ensureUniqueId bool) *gomerr.ApplicationError {
	pt := t.persistableTypes[p.PersistableTypeName()]

	for fieldName, additionalFields := range pt.uniqueFields {
		if ae := t.preQueryConstraintsCheck(p, fieldName, additionalFields); ae != nil {
			return ae
		}
	}

	av, err := dynamodbattribute.MarshalMap(p)
	if err != nil {
		logs.Error.Println("Failed to marshal p: " + err.Error())
		logs.Error.Printf("%T: %v", p, p)

		return gomerr.InternalServerError("Unable to construct persistable form.")
	}

	pt.convertFieldNamesToDbNames(&av)

	for _, index := range t.indexes {
		index.populateKeyValues(av, p, t.valueSeparator)
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
			// ErrCodeProvisionedThroughputExceededException, ErrCodeResourceNotFoundException,
			// ErrCodeItemCollectionSizeLimitExceededException, ErrCodeTransactionConflictException,
			// ErrCodeRequestLimitExceeded, ErrCodeInternalServerError
			switch awsErr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return gomerr.ConflictException("Conditional check failed")
			default:
				logs.Error.Println(awsErr.Code(), awsErr.Error())
			}
		} else {
			logs.Error.Println(err.Error())
		}

		return gomerr.InternalServerError("Unable to persist resource.")
	}

	return nil
}

func (t *table) Read(p data.Persistable) *gomerr.ApplicationError {
	keys := make(map[string]*dynamodb.AttributeValue, 2)
	t.populateKeyValues(keys, p, t.valueSeparator)

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
				return gomerr.ResourceNotFound(p)
			default:
				// ErrCodeProvisionedThroughputExceededException, ErrCodeRequestLimitExceeded, ErrCodeInternalServerError:
				logs.Error.Println(awsErr.Code(), awsErr.Error())
			}
		} else {
			logs.Error.Println(err.Error())
		}

		return gomerr.InternalServerError("Unable to get resource.")
	}

	if output.Item == nil {
		return gomerr.ResourceNotFound(p)
	}

	err = dynamodbattribute.UnmarshalMap(output.Item, p)
	if err != nil {
		logs.Error.Println("Failed to unmarshal p: " + err.Error())

		return gomerr.InternalServerError("Unable to retrieve resource.")
	}

	return nil
}

func (t *table) Delete(p data.Persistable) *gomerr.ApplicationError {
	// TODO:p2 support a soft-delete option

	keys := make(map[string]*dynamodb.AttributeValue, 2)
	t.populateKeyValues(keys, p, t.valueSeparator)

	input := &dynamodb.DeleteItemInput{
		Key:       keys,
		TableName: t.tableName,
	}

	_, err := t.ddb.DeleteItem(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				return gomerr.ResourceNotFound(p)
			default:
				// ErrCodeProvisionedThroughputExceededException, ErrCodeRequestLimitExceeded, ErrCodeInternalServerError:
				logs.Error.Println(awsErr.Code(), awsErr.Error())
			}
		} else {
			logs.Error.Println(err.Error())
		}

		return gomerr.InternalServerError("Unable to delete resource.")
	}

	return nil
}

func (t *table) Query(q data.Queryable, arrayOfPersistable interface{}) (nextToken *string, ae *gomerr.ApplicationError) {
	qi, ae := t.buildQueryInput(q)
	if ae != nil {
		return nil, ae
	}

	qo, ae := t.runQuery(qi)
	if ae != nil {
		return nil, ae
	}

	err := dynamodbattribute.UnmarshalListOfMaps(qo.Items, arrayOfPersistable)
	if err != nil {
		logs.Error.Println("Failed to unmarshal p: " + err.Error())

		return nil, gomerr.InternalServerError("Unable to retrieve resource.")
	}

	return t.nextTokenizer.tokenize(q, qo.LastEvaluatedKey)
}

func (t *table) preQueryConstraintsCheck(p data.Persistable, fieldName string, additionalFields []string) *gomerr.ApplicationError {
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

	qi, ae := t.buildQueryInput(q)
	if ae != nil {
		return ae
	}

	for queryLimit := int64(1); ; queryLimit += 100 { // Bump limit up each time
		qi.Limit = &queryLimit

		qo, ae := t.runQuery(qi)
		if ae != nil {
			return ae
		}

		if len(qo.Items) > 0 {
			return gomerr.ConflictException("A resource exists with a conflicting value", map[string]string{"Field": fieldName, "Resource": p.PersistableTypeName()})
		}

		if qo.LastEvaluatedKey == nil {
			return nil
		}

		// TODO: log that we're looping - should be a warning sign
		qi.ExclusiveStartKey = qo.LastEvaluatedKey
	}
}

func (t *table) buildQueryInput(q data.Queryable) (*dynamodb.QueryInput, *gomerr.ApplicationError) {
	index, consistent, ae := indexFor(t, q)
	if ae != nil {
		return nil, ae
	}

	expressionAttributeNames := make(map[string]*string, 2)
	expressionAttributeValues := make(map[string]*dynamodb.AttributeValue, 2)

	keyConditionExpresion := safeName(index.pk.name, expressionAttributeNames) + "=:pk"
	expressionAttributeValues[":pk"] = index.pk.attributeValue(q, t.valueSeparator)

	if index.sk != nil {
		if av := index.sk.attributeValue(q, t.valueSeparator); av != nil {
			keyConditionExpresion += " AND " + safeName(index.sk.name, expressionAttributeNames) + "=:sk"
			expressionAttributeValues[":sk"] = av
		}
	}

	for _, attribute := range q.ResponseFields() {
		safeName(attribute, expressionAttributeNames)
	}

	if len(expressionAttributeNames) == 0 {
		expressionAttributeNames = nil
	}

	// TODO: projectionExpression
	//var projectionExpressionPtr *string
	//projectionExpression := strings.Join(attributes, ",") // Join() returns "" if len(attributes) == 0
	//if projectionExpression != "" {
	//	projectionExpressionPtr = &projectionExpression
	//}

	exclusiveStartKey, ae := t.nextTokenizer.untokenize(q)
	if ae != nil {
		return nil, ae
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

func (t *table) runQuery(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, *gomerr.ApplicationError) {
	output, err := t.ddb.Query(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// TODO: improve exceptions
			switch awsErr.Code() {
			default:
				// ErrCodeProvisionedThroughputExceededException, ErrCodeRequestLimitExceeded, ErrCodeInternalServerError:
				logs.Error.Println(awsErr.Code(), awsErr.Error())
			}
		} else {
			logs.Error.Println(err.Error())
		}

		return nil, gomerr.InternalServerError("Unable to list resource")
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

func safeName(fieldName string, expressionAttributeNames map[string]*string) string {
	// TODO: calculate once and store in persistableType
	if _, reserved := reservedWords[strings.ToUpper(fieldName)]; reserved || strings.ContainsAny(fieldName, ". ") || fieldName[0] >= '0' || fieldName[0] <= '9' {
		replacement := "#a" + strconv.Itoa(len(expressionAttributeNames))
		expressionAttributeNames[replacement] = &fieldName

		return replacement
	}

	return fieldName
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
