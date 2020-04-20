package dynamodb

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
	"github.com/jt0/gomer/logs"
	"github.com/jt0/gomer/util"
)

type table struct {
	ddb               *dynamodb.DynamoDB
	name              *string
	defaultLimit      *int64
	maxLimit          *int64
	defaultConsistent *bool
	pk                *key
	sk                *key
	indexes           map[string]*index
	persistableData   map[string]*persistableData
	valueSeparator    string
}

type index struct {
	name *string
	pk   *key
	sk   *key
	lsi  bool
	//projects bool
}

type key struct {
	name           string
	attributeType  string
	compositeParts map[string][]string
}

type persistableData struct {
	uniquenessConstraints map[string]string // fieldName -> indexName
	dbNames               map[string]string // persistable name -> storage name
}

type Configuration struct {
	DynamoDb                *dynamodb.DynamoDB
	DefaultMaxResults       int
	MaxMaxResults           int
	ConsistentRead          bool
	CompositeValueSeparator string
}

func Store(tableName string, config *Configuration, persistablesForStore ...data.Persistable) data.Store {
	defaultLimit := int64(config.DefaultMaxResults)
	maxLimit := int64(config.MaxMaxResults)

	table := &table{
		ddb:               config.DynamoDb,
		name:              &tableName,
		defaultLimit:      &defaultLimit,
		maxLimit:          &maxLimit,
		defaultConsistent: &config.ConsistentRead,
		indexes:           make(map[string]*index),
		valueSeparator:    config.CompositeValueSeparator,
	}

	table.prepare(persistablesForStore)

	return table
}

func (t *table) Create(p data.Persistable) *gomerr.ApplicationError {
	return t.put(p, true)
}

func (t *table) Update(p data.Persistable) *gomerr.ApplicationError {
	// TODO:p1 support partial update vs put()
	return t.put(p, false)
}

func (t *table) put(p data.Persistable, ensureUniqueId bool) *gomerr.ApplicationError {
	pd := t.persistableData[p.TypeName()]

	if ae := t.checkUniquenessConstraints(p, pd); ae != nil {
		return ae
	}

	av, err := dynamodbattribute.MarshalMap(p)
	if err != nil {
		logs.Error.Println("Failed to marshal p: " + err.Error())
		logs.Error.Printf("%T: %v", p, p)

		return gomerr.InternalServerError("Unable to construct persistable form.")
	}

	convertFieldNamesToDbNames(&av, pd)

	t.populateKeyValues(av, p, t)
	for _, idx := range t.indexes {
		t.populateKeyValues(av, p, idx)
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
		TableName:           t.name,
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

func (t *table) checkUniquenessConstraints(p data.Persistable, pd *persistableData) *gomerr.ApplicationError {
	if pd == nil {
		return nil
	}

	for fieldName, indexName := range pd.uniquenessConstraints {
		if ae := t.checkUniqueness(fieldName, indexName, p); ae != nil {
			return ae
		}
	}

	return nil
}

var uniqueCheckLimit = int64(1)

func (t *table) checkUniqueness(fieldName string, indexName string, p data.Persistable) *gomerr.ApplicationError {
	idx, ok := t.indexes[indexName]
	if !ok {
		return gomerr.InternalServerError("Unable to find uniqueness index")
	}

	keys := make(map[string]*dynamodb.AttributeValue, 2)
	t.populateKeyValues(keys, p, idx)

	expressionAttributeNames := make(map[string]*string)
	keyConditionExpresion := safeName(idx.pk.name, expressionAttributeNames) + "=:pk"
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{":pk": keys[idx.pk.name]}

	if idx.sk != nil {
		keyConditionExpresion += " AND " + safeName(idx.sk.name, expressionAttributeNames) + "=:sk"
		expressionAttributeValues[":sk"] = keys[idx.sk.name]
	}

	if len(expressionAttributeNames) == 0 {
		expressionAttributeNames = nil
	}

	input := &dynamodb.QueryInput{
		TableName:                 t.name,
		IndexName:                 idx.name,
		ConsistentRead:            &idx.lsi,
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		KeyConditionExpression:    &keyConditionExpresion,
		//ProjectionExpression:      projectionExpressionPtr,
		Limit: &uniqueCheckLimit,
	}

	for {
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

			return gomerr.InternalServerError("Unable to list resource")
		}

		if len(output.Items) > 0 {
			return gomerr.ConflictException("A resource exists with a duplicate attribute", map[string]string{"Attribute": fieldName, "Resource": p.TypeName()})
		}

		if output.LastEvaluatedKey == nil {
			return nil
		}

		// TODO: log that we're looping - should be a warning sign
		input.ExclusiveStartKey = output.LastEvaluatedKey
		input.Limit = nil // remove limit to reduce the number of round trips to dynamo
	}
}

func safeName(fieldName string, expressionAttributeNames map[string]*string) string {
	if _, reserved := reservedWords[strings.ToUpper(fieldName)]; reserved {
		replacement := "#a" + strconv.Itoa(len(expressionAttributeNames))
		expressionAttributeNames[replacement] = &fieldName

		return replacement
	}

	return fieldName
}

func convertFieldNamesToDbNames(av *map[string]*dynamodb.AttributeValue, pd *persistableData) {
	if len(pd.dbNames) == 0 {
		return
	}

	cv := make(map[string]*dynamodb.AttributeValue, len(*av))
	for k, v := range *av {
		if dbName, ok := pd.dbNames[k]; ok {
			if dbName != "-" {
				cv[dbName] = v
			}
		} else {
			cv[k] = v
		}
	}

	*av = cv
}

func (t *table) populateKeyValues(av map[string]*dynamodb.AttributeValue, p data.Persistable, k keyable) {
	pk := k.getPk()
	if _, present := av[pk.name]; !present {
		av[pk.name] = toAttributeValue(pk.attributeType, t.buildKeyValue(p, pk))
	}

	if sk := k.getSk(); sk != nil {
		if _, present := av[sk.name]; !present {
			av[sk.name] = toAttributeValue(sk.attributeType, t.buildKeyValue(p, sk))
		}
	}
}

func (t *table) buildKeyValue(p data.Persistable, key *key) string {
	pv := reflect.ValueOf(p).Elem()

	compositeParts, ok := key.compositeParts[p.TypeName()]
	if !ok || len(compositeParts) == 0 {
		return fmt.Sprint(pv.FieldByName(key.name).Interface())
	}

	compositeValues := make([]string, len(compositeParts))
	for i, compositePart := range compositeParts {
		if compositePart[:1] == "'" {
			compositeValues[i] = compositePart[1 : len(compositePart)-1]
		} else {
			compositeValues[i] = fmt.Sprint(pv.FieldByName(compositePart).Interface())
		}
	}

	return strings.Join(compositeValues, t.valueSeparator)
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

	keys := make(map[string]*dynamodb.AttributeValue, 2)
	t.populateKeyValues(keys, p, t)

	input := &dynamodb.GetItemInput{
		Key:            keys,
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

	keys := make(map[string]*dynamodb.AttributeValue, 2)
	t.populateKeyValues(keys, p, t)

	input := &dynamodb.DeleteItemInput{
		Key:       keys,
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

func toAttributeValue(attributeType string, value interface{}) *dynamodb.AttributeValue {
	switch attributeType {
	case dynamodb.ScalarAttributeTypeS:
		s := fmt.Sprint(value) // TODO:p1 replace with a better conversion mechanism (e.g. handle times)
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
		if _, reserved := reservedWords[strings.ToUpper(attribute)]; reserved {
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
	projectionExpression := strings.Join(attributes, ",") // Join() returns "" if len(attributes) == 0
	if projectionExpression != "" {
		projectionExpressionPtr = &projectionExpression
	}

	var consistent *bool
	if !index.lsi { // consistent must be false if a GSI
		consistent = &index.lsi
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

	//var second string
	//switch len(queryKeys) {
	//case 1:
	//	second = ""
	//case 2:
	//	second = queryKeys[1].Name
	//}

	//if index, ok := t.indexByFields[queryKeys[0].Name][second]; !ok {
	//	logs.Error.Printf("No index for '%s' and '%s'", queryKeys[0].Name, second)
	//
	//	return nil, gomerr.InternalServerError("No index for query keys")
	//} else {
	//	return index, nil
	//}

	return nil, nil
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

func (t *table) prepare(persistablesForStore []data.Persistable) *table {
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

	addKeyDetails(t, output.Table.KeySchema, attributeTypes) // Add key details for the table itself
	t.processLocalIndexes(output.Table.LocalSecondaryIndexes, attributeTypes)
	t.processGlobalIndexes(output.Table.GlobalSecondaryIndexes, attributeTypes)
	t.processPersistables(persistablesForStore)

	return t
}

func (t *table) processLocalIndexes(lsids []*dynamodb.LocalSecondaryIndexDescription, attributeTypes map[string]string) {
	for _, lsid := range lsids {
		lsi := &index{name: lsid.IndexName, lsi: true}
		addKeyDetails(lsi, lsid.KeySchema[1:], attributeTypes)

		lsi.pk = t.pk

		t.indexes[*lsid.IndexName] = lsi
	}
}

func (t *table) processGlobalIndexes(gsids []*dynamodb.GlobalSecondaryIndexDescription, attributeTypes map[string]string) {
	for _, gsid := range gsids {
		gsi := &index{name: gsid.IndexName, lsi: false}
		addKeyDetails(gsi, gsid.KeySchema, attributeTypes)

		t.indexes[*gsid.IndexName] = gsi
	}
}

func addKeyDetails(k keyable, keySchemaElements []*dynamodb.KeySchemaElement, attributeTypes map[string]string) {
	for _, keySchemaElement := range keySchemaElements {
		key := &key{
			name:           *keySchemaElement.AttributeName,
			attributeType:  attributeTypes[*keySchemaElement.AttributeName],
			compositeParts: make(map[string][]string),
		}
		switch *keySchemaElement.KeyType {
		case dynamodb.KeyTypeHash:
			k.setPk(key)
		case dynamodb.KeyTypeRange:
			k.setSk(key)
		}
	}
}

func (t *table) processPersistables(persistablesForStore []data.Persistable) {
	t.persistableData = make(map[string]*persistableData, len(persistablesForStore))
	for _, p := range persistablesForStore {
		pd := &persistableData{
			uniquenessConstraints: make(map[string]string),
			dbNames:               make(map[string]string),
		}

		pt := reflect.TypeOf(p)
		ptName := strings.ToLower(util.UnqualifiedTypeName(pt))

		t.processFields(ptName, pt.Elem(), pd)

		// Validate that each key has fully defined key fields for this persistable type
		for _, key := range t.keys() {
			if key == nil {
				continue
			}

			if compositeParts, ok := key.compositeParts[ptName]; ok {
				for i, compositePart := range compositeParts {
					if compositePart == "" {
						panic(fmt.Sprintf("%s's compositePart #%d for key '%s' is missing", ptName, i, key.name))
					}
				}
			}
		}

		t.persistableData[ptName] = pd
	}
}

func (t *table) keys() []*key {
	keys := append(make([]*key, 0, 2*len(t.indexes)+2), t.pk, t.sk)

	for _, v := range t.indexes {
		keys = append(keys, v.pk, v.sk)
	}

	return keys
}

func (t *table) processFields(persistableName string, persistableType reflect.Type, pd *persistableData) {
	for i := 0; i < persistableType.NumField(); i++ {
		field := persistableType.Field(i)
		fieldName := field.Name

		if field.Type.Kind() == reflect.Struct && field.Anonymous {
			t.processFields(persistableName, field.Type, pd)
		} else if unicode.IsLower([]rune(fieldName)[0]) {
			continue
		} else {
			t.processDataTag(field.Tag.Get("data"), fieldName, pd)
			t.processDdbKeysTag(field.Tag.Get("ddb.keys"), fieldName, persistableName)
		}
	}
}

var dataTagRegex = regexp.MustCompile(`(-|\w*)?(,unique\(([\w-.]+)\))?`)

func (t *table) processDataTag(dataTag string, fieldName string, pd *persistableData) {
	if dataTag == "" {
		return
	}

	groups := dataTagRegex.FindStringSubmatch(dataTag)
	if groups == nil {
		panic("Improperly formatted data structTag: " + dataTag)
	}

	if groups[1] != "" {
		pd.dbNames[fieldName] = groups[1]
	}

	if groups[2] != "" {
		_, ok := t.indexes[groups[3]]
		if !ok {
			panic(fmt.Sprintf("No index found with name %s for unique field %s", groups[3], fieldName))
		}

		pd.uniquenessConstraints[fieldName] = groups[3]
	}
}

// Format: tuple (separated by commas) of (indexName.)?
func (t *table) processDdbKeysTag(ddbKeyTag string, fieldName string, persistableName string) {
	if ddbKeyTag == "" {
		return
	}

	for _, keyStatement := range strings.Split(ddbKeyTag, ",") {
		keyStatement = strings.TrimSpace(keyStatement)
		if keyStatement == "" {
			continue
		}

		t.processDdbKeyStatement(persistableName, keyStatement, fieldName)
	}
}

var ddbKeyStatementRegex = regexp.MustCompile(`(([\w-.]+):)?(pk|sk)(.(\d+))?(=('\w+'))?`)

func (t *table) processDdbKeyStatement(persistableName string, keyStatement string, compositePart string) {
	groups := ddbKeyStatementRegex.FindStringSubmatch(keyStatement)
	if groups == nil {
		panic("Improperly formatted ddb.keys element: " + keyStatement)
	}

	var keyable keyable
	if groups[2] == "" {
		keyable = t
	} else {
		index, ok := t.indexes[groups[2]]
		if !ok {
			panic("unknown index '" + groups[2] + "' for table " + *t.name)
		}

		keyable = index
	}

	var key *key
	if groups[3] == "pk" {
		key = keyable.getPk()
	} else {
		key = keyable.getSk()
	}

	index, err := strconv.Atoi(groups[5])
	if err != nil {
		panic("expected an index value for ddb.keys element: " + keyStatement)
	}

	if groups[7] != "" { // True means a static value for this composite part. Replace w/ the value.
		compositePart = groups[7]
	}

	key.insertCompositePartAtIndex(persistableName, compositePart, index)
}

func (key *key) insertCompositePartAtIndex(persistableName string, compositePart string, index int) {
	compositeParts, ok := key.compositeParts[persistableName]
	if !ok {
		compositeParts = make([]string, 0, index+1)
	}

	lenCompositeParts := len(compositeParts)
	capCompositeParts := cap(compositeParts)
	if index < lenCompositeParts {
		if compositeParts[index] != "" {
			panic(fmt.Sprintf("already found key part '%s' at index %d for key %s ", compositeParts[index], index, key.name))
		}
	} else if index < capCompositeParts {
		compositeParts = compositeParts[0 : index+1]
	} else {
		compositeParts = append(compositeParts, make([]string, index+1-capCompositeParts)...)
	}

	compositeParts[index] = compositePart
	key.compositeParts[persistableName] = compositeParts
}

func (t *table) keyables() []keyable {
	keyables := append(make([]keyable, 0, len(t.indexes)+1), t)

	for _, v := range t.indexes {
		keyables = append(keyables, v)
	}

	return keyables
}

type keyable interface {
	getPk() *key
	setPk(pk *key)
	getSk() *key
	setSk(sk *key)
}

func (t *table) getPk() *key {
	return t.pk
}

func (t *table) setPk(pk *key) {
	t.pk = pk
}

func (t *table) getSk() *key {
	return t.sk
}

func (t *table) setSk(sk *key) {
	t.sk = sk
}

func (i *index) getPk() *key {
	return i.pk
}

func (i *index) setPk(pk *key) {
	i.pk = pk
}

func (i *index) getSk() *key {
	return i.sk
}

func (i *index) setSk(sk *key) {
	i.sk = sk
}
