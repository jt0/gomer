package _test

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TableDefinition struct {
	TableName            string
	AttributeDefinitions []types.AttributeDefinition
	KeySchema            []types.KeySchemaElement
	LocalSecondaryIndex  []types.LocalSecondaryIndex
	GlobalSecondaryIndex []types.GlobalSecondaryIndex
}

func (t *TableDefinition) WithTableName(tableName string) *TableDefinition {
	t.TableName = tableName
	return t
}

func (t *TableDefinition) WithAttributeDefinition(attributeName string, attributeType types.ScalarAttributeType) *TableDefinition {
	t.AttributeDefinitions = append(t.AttributeDefinitions, types.AttributeDefinition{
		AttributeName: aws.String(attributeName),
		AttributeType: attributeType,
	})
	return t
}

func (t *TableDefinition) WithKeySchema(attributeName string, keyType types.KeyType) *TableDefinition {
	t.KeySchema = append(t.KeySchema, types.KeySchemaElement{
		AttributeName: aws.String(attributeName),
		KeyType:       keyType,
	})
	return t
}

func (t *TableDefinition) WithLsi(indexName string, keySchema []types.KeySchemaElement, projection types.Projection) *TableDefinition {
	t.LocalSecondaryIndex = append(t.LocalSecondaryIndex, types.LocalSecondaryIndex{
		IndexName:  aws.String(indexName),
		KeySchema:  keySchema,
		Projection: &projection,
	})
	return t
}

func (t *TableDefinition) WithGsi(indexName string, keySchema []types.KeySchemaElement, projection types.Projection) *TableDefinition {
	t.GlobalSecondaryIndex = append(t.GlobalSecondaryIndex, types.GlobalSecondaryIndex{
		IndexName:  aws.String(indexName),
		KeySchema:  keySchema,
		Projection: &projection,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	return t
}

func (t *TableDefinition) Create(ddb *dynamodb.Client) {
	// check for table's existence
	if _, err := ddb.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: &t.TableName,
	}); err != nil {
		var rnf *types.ResourceNotFoundException
		if !errors.As(err, &rnf) {
			panic(err.Error()) // anything other than a rnf, panic
		}
		// Table doesn't exist, so okay to create
	} else {
		// TODO: validate output to see if it matches definition?

		return
	}

	if _, err := ddb.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName:              &t.TableName,
		AttributeDefinitions:   t.AttributeDefinitions,
		KeySchema:              t.KeySchema,
		LocalSecondaryIndexes:  t.LocalSecondaryIndex,
		GlobalSecondaryIndexes: t.GlobalSecondaryIndex,
		ProvisionedThroughput: ptr(types.ProvisionedThroughput{
			ReadCapacityUnits:  ptr(int64(5)),
			WriteCapacityUnits: ptr(int64(5)),
		}),
	}); err != nil {
		panic(fmt.Sprintf("Error creating %s table: %s", t.TableName, err.Error()))
	}
}

func ptr[T any](t T) *T {
	return &t
}
