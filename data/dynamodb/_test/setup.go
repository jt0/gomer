package _test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
		ProvisionedThroughput: Ptr(types.ProvisionedThroughput{
			ReadCapacityUnits:  Ptr(int64(5)),
			WriteCapacityUnits: Ptr(int64(5)),
		}),
	}); err != nil {
		panic(fmt.Sprintf("Error creating %s table: %s", t.TableName, err.Error()))
	}
}

func Ptr[T any](t T) *T {
	return &t
}

// NewClient creates a DynamoDB client configured for either local or AWS usage.
// If DDB_LOCAL environment variable is set, uses local DynamoDB:
//   - Empty string or "true": uses default port 7001
//   - Specific port number: uses that port (e.g., "8000")
//
// If DDB_LOCAL is not set, uses default AWS configuration.
func NewClient() (*dynamodb.Client, bool, error) {
	port, useDdbLocal := os.LookupEnv("DDB_LOCAL")

	if !useDdbLocal {
		// Use AWS configuration
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, false, err
		}
		return dynamodb.NewFromConfig(cfg), false, nil
	}

	// Use local DynamoDB
	if port == "" {
		// TODO: replace with support for launching a process-managed ddblocal instance.
		port = "7001"
	}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		return nil, true, err
	}

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = Ptr("http://127.0.0.1:" + port)
	})

	return client, true, nil
}

// DeleteAllTableData deletes all items from a table
func DeleteAllTableData(ddb *dynamodb.Client, tableName string) error {
	scanOutput, err := ddb.Scan(context.Background(), &dynamodb.ScanInput{
		TableName: &tableName,
	})
	if err != nil {
		return err
	}

	for _, item := range scanOutput.Items {
		_, err = ddb.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
			TableName: &tableName,
			Key: map[string]types.AttributeValue{
				"PK": item["PK"],
				"SK": item["SK"],
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteTable deletes the overall table
func DeleteTable(ddb *dynamodb.Client, tableName string) error {
	_, err := ddb.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: &tableName,
	})
	return err
}
