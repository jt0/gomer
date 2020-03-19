package dynamodb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/logs"
)

type table struct {
	ddb               *dynamodb.DynamoDB
	name              *string
	defaultLimit      *int64
	maxLimit          *int64
	defaultConsistent *bool
	pk                key
	sk                *key
	indexes           map[string]map[string]index
}

type index struct {
	name *string
	pk   key
	sk   *key
	gsi  bool
	//projects bool
}

type key struct {
	name          string
	attributeType string
}

type Configuration struct {
	DynamoDb *dynamodb.DynamoDB
	DefaultMaxResults int
	MaxMaxResults int
	ConsistentRead bool
}

func Store(tableName string, config *Configuration) data.Store {
	defaultLimit := int64(config.DefaultMaxResults)
	maxLimit := int64(config.MaxMaxResults)

	table := &table{
		ddb:               config.DynamoDb,
		name:              &tableName,
		defaultLimit:      &defaultLimit,
		maxLimit:          &maxLimit,
		defaultConsistent: &config.ConsistentRead,
		indexes:           make(map[string]map[string]index),
	}

	return table.getTable()
}

func (t *table) Create(p data.Persistable) *gomerr.ApplicationError {
	return t.put(p, true)
}

func (t *table) Update(p data.Persistable) *gomerr.ApplicationError {
	// TODO:p1 if encryption, use put.  Else, optimize write
	return t.put(p, false)
}

func (t *table) put(p data.Persistable, ensureUniqueId bool) *gomerr.ApplicationError {
	av, err := dynamodbattribute.MarshalMap(p)
	if err != nil {
		logs.Error.Println("Failed to marshal p: " + err.Error())
		logs.Error.Printf("%T: %v", p, p)

		return gomerr.InternalServerError("Unable to construct persistable form.")
	}

	keyValues := p.KeyValues()

	if _, present := av[t.pk.name]; !present {
		av[t.pk.name] = toAttributeValue(t.pk.attributeType, keyValues[0])
	}
	if t.sk != nil {
		if _, present := av[t.sk.name]; !present {
			av[t.sk.name] = toAttributeValue(t.sk.attributeType, keyValues[1])
		}
	}

	// TODO:p1 add id protection expression
	if ensureUniqueId {

	}

	// TODO:p1 optimistic locking

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: t.name,
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

type ConsistentRead interface {
	ConsistentRead() bool
}

func (t *table) Read(p data.Persistable) *gomerr.ApplicationError {
	var consistent *bool
	if cr, ok := p.(ConsistentRead); ok {
		c := cr.ConsistentRead()
		consistent = &c
	} else {
		consistent = t.defaultConsistent
	}

	input := &dynamodb.GetItemInput{
		Key:            t.keys(p),
		ConsistentRead: consistent,
		TableName:      t.name,
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

	input := &dynamodb.DeleteItemInput{
		Key:       t.keys(p),
		TableName: t.name,
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

func (t *table) keys(p data.Persistable) map[string]*dynamodb.AttributeValue {
	keys := make(map[string]*dynamodb.AttributeValue, 2)
	keyValues := p.KeyValues()

	keys[t.pk.name] = toAttributeValue(t.pk.attributeType, keyValues[0])

	if t.sk != nil {
		keys[t.sk.name] = toAttributeValue(t.sk.attributeType, keyValues[1])
	}

	return keys
}

func toAttributeValue(attributeType string, value interface{}) *dynamodb.AttributeValue {
	switch attributeType {
	case dynamodb.ScalarAttributeTypeS:
		s := fmt.Sprint(value)                      // TODO:p1 replace with a better conversion mechanism (e.g. handle times)
		return &dynamodb.AttributeValue{S: &s}
	case dynamodb.ScalarAttributeTypeN:
		n := fmt.Sprint(value)
		return &dynamodb.AttributeValue{N: &n}
	case dynamodb.ScalarAttributeTypeB:
		return &dynamodb.AttributeValue{B: value.([]byte)}
	default:
		panic("Unknown scalar attribute type value: " + attributeType)
	}
}

func (t *table) Query(q data.Queryable, arrayOfPersistable interface{}) (nextToken *string, ae *gomerr.ApplicationError) {
	keys, attributes := q.QueryInfo()
	index, ae := t.index(keys)
	if ae != nil {
		// TODO:p3 support scan as an option?
		return nil, ae
	}

	expressionAttributeValues := make(map[string]*dynamodb.AttributeValue)

	keyConditionExpresion := keys[0].Name + "=:pk"
	expressionAttributeValues[":pk"] = toAttributeValue(index.pk.attributeType, keys[0].Value)

	var scanIndexForward bool
	if len(keys) > 1 {
		if keys[1].Value != nil {
			sk := keys[1].Name
			keyConditionExpresion += " AND " + sk + "=:sk"
			//// TODO:p2 support additional query types
			//if (sk)[len(sk)-1] == ':' {
			//	keyConditionExpresion += " AND begins_with(" + sk + ",:sk)"
			//} else {
			//}
			expressionAttributeValues[":sk"] = toAttributeValue(index.sk.attributeType, keys[1].Value)
		}

		scanIndexForward = !keys[1].Descending
	}

	expressionAttributeNames := make(map[string]*string)
	avCounter := 0
	for i, attribute := range attributes {
		if reservedWords[strings.ToUpper(attribute)] {
			original := attributes[i]
			replacement := "#a" + strconv.Itoa(avCounter)
			expressionAttributeNames[replacement] = &original
			attributes[i] = replacement
			avCounter++
		}
	}
	if avCounter == 0 {
		expressionAttributeNames = nil
	}

	var projectionExpressionPtr *string
	projectionExpression := strings.Join(attributes, ",")  // Join() returns "" if len(attributes) == 0
	if projectionExpression != "" {
		projectionExpressionPtr = &projectionExpression
	}

	var consistent *bool
	if index.gsi {  // Must be false if a GSI
		f := false
		consistent = &f
	} else {
		if cr, ok := q.(ConsistentRead); ok {
			c := cr.ConsistentRead()
			consistent = &c
		} else {
			consistent = t.defaultConsistent
		}
	}

	input := &dynamodb.QueryInput{
		TableName:                 t.name,
		IndexName:                 index.name,
		ConsistentRead:            consistent,
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		KeyConditionExpression:    &keyConditionExpresion,
		FilterExpression:          nil,
		ProjectionExpression:      projectionExpressionPtr,
		ScanIndexForward:          &scanIndexForward,
		ExclusiveStartKey:         fromToken(q.NextToken()),
		Limit:                     t.limit(q.MaxResults()),
	}

	output, err := t.ddb.Query(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
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

	err = dynamodbattribute.UnmarshalListOfMaps(output.Items, arrayOfPersistable)
	if err != nil {
		logs.Error.Println("Failed to unmarshal p: " + err.Error())

		return nil, gomerr.InternalServerError("Unable to retrieve resource.")
	}

	return toToken(output.LastEvaluatedKey), nil
}

func (t *table) index(queryKeys []data.QueryKey) (*index, *gomerr.ApplicationError) {
	if queryKeys == nil || len(queryKeys) == 0 {
		return nil, gomerr.InternalServerError("Invalid query keys")
	}

	var second string
	switch len(queryKeys) {
	case 1: second = ""
	case 2: second = queryKeys[1].Name
	}

	if index, ok := t.indexes[queryKeys[0].Name][second]; !ok {
		logs.Error.Printf("No index for '%s' and '%s'", queryKeys[0].Name, second)

		return nil, gomerr.InternalServerError("No index for query keys")
	} else {
		return &index, nil
	}
}

func (t *table) limit(maxResults *int64) *int64 {
	if maxResults != nil && *maxResults > 0 {
		if *maxResults <= *t.maxLimit {
			return maxResults
		} else {
			return t.maxLimit
		}
	} else {
		return t.defaultLimit
	}
}

func fromToken(token *string) map[string]*dynamodb.AttributeValue {
	if token != nil {
		// TODO:p0 implement
	}

	return nil
}

func toToken(lastEvaluatedKey map[string]*dynamodb.AttributeValue) *string {
	if lastEvaluatedKey != nil {
		// TODO:p0 implement
	}

	return nil
}

func (t *table) getTable() *table {
	input := &dynamodb.DescribeTableInput{TableName: t.name}
	output, err := t.ddb.DescribeTable(input)
	if err != nil {
		// TODO:p1 support re-messaging of errors
		panic("Table inspection error")
	}

	attributeTypes := make(map[string]string)
	for _, at := range output.Table.AttributeDefinitions {
		attributeTypes[*at.AttributeName] = *at.AttributeType
	}

	for _, ks := range output.Table.KeySchema {
		switch *ks.KeyType {
		case dynamodb.KeyTypeHash:
			t.pk = key{name: *ks.AttributeName, attributeType: attributeTypes[*ks.AttributeName]}
		case dynamodb.KeyTypeRange:
			t.sk = &key{name: *ks.AttributeName, attributeType: attributeTypes[*ks.AttributeName]}
		}
	}

	// TODO:p1 GSIs

	localIndexes := make(map[string]index, len(output.Table.LocalSecondaryIndexes) + 2)
	for _, lsi := range output.Table.LocalSecondaryIndexes {
		index := index{name: lsi.IndexName}
		for _, ks := range lsi.KeySchema {
			switch *ks.KeyType {
			case dynamodb.KeyTypeHash:
				index.pk = key{name: *ks.AttributeName, attributeType: attributeTypes[*ks.AttributeName]}
			case dynamodb.KeyTypeRange:
				index.sk = &key{name: *ks.AttributeName, attributeType: attributeTypes[*ks.AttributeName]}
			}
		}

		if index.sk != nil {
			localIndexes[index.sk.name] = index
		} // this may not require an if check since this would be a useless LSI.
	}

	mainIndex := index{name: nil, pk: t.pk, sk: t.sk}
	if t.sk != nil {
		localIndexes[t.sk.name] = mainIndex
	}
	localIndexes[""] = mainIndex

	t.indexes[t.pk.name] = localIndexes

	return t
}
