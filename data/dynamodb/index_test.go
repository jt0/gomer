package dynamodb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/data"
	ddb "github.com/jt0/gomer/data/dynamodb"
	ddbtest "github.com/jt0/gomer/data/dynamodb/_test"
)

const tableName = "gomer_keys_test"

// Test setup helpers
func setupTestStore(t *testing.T, persistables ...data.Persistable) (data.Store, *dynamodb.Client) {
	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)

	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	// Create table definition with all needed indexes
	tableDef := &ddbtest.TableDefinition{}
	tableDef.WithTableName(tableName).
		WithAttributeDefinition("PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("SK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("GSI1PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("GSI1SK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("LSI1SK", types.ScalarAttributeTypeS).
		WithKeySchema("PK", types.KeyTypeHash).
		WithKeySchema("SK", types.KeyTypeRange).
		WithGsi("gsi_1", []types.KeySchemaElement{
			{AttributeName: ddbtest.Ptr("GSI1PK"), KeyType: types.KeyTypeHash},
			{AttributeName: ddbtest.Ptr("GSI1SK"), KeyType: types.KeyTypeRange},
		}, types.Projection{ProjectionType: types.ProjectionTypeAll}).
		WithLsi("lsi_1", []types.KeySchemaElement{
			{AttributeName: ddbtest.Ptr("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: ddbtest.Ptr("LSI1SK"), KeyType: types.KeyTypeRange},
		}, types.Projection{ProjectionType: types.ProjectionTypeAll})

	tableDef.Create(client)

	store, ge := ddb.Store(tableName, &ddb.Configuration{
		DynamoDb:           client,
		MaxResultsDefault:  100,
		MaxResultsMax:      1000,
		ConsistencyDefault: ddb.Preferred,
	})
	assert.Success(t, ge)
	assert.Success(t, store.AddPersistables(persistables...))

	return store, client
}

func deleteTestTable(t *testing.T, client *dynamodb.Client) {
	err := ddbtest.DeleteTable(client, "gomer_keys_test")
	assert.Success(t, err)
}

// ==============================================================================
// Section 1: Simple Key Construction Tests
// ==============================================================================

// TestIndex_BuildKeyValue_SinglePK tests section 1.1: Single Partition Key (PK Only)
func TestIndex_BuildKeyValue_SinglePK(t *testing.T) {
	tests := []struct {
		name        string
		entity      data.Persistable
		expectedPK  string
		shouldError bool
	}{
		{
			name:       "String field",
			entity:     &ddbtest.CompositeKeyEntity{PartitionKey: "user123", SortKey: "sk1"},
			expectedPK: "user123",
		},
		{
			name:       "Empty string",
			entity:     &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"},
			expectedPK: "",
		},
		{
			name:       "Numeric field",
			entity:     &ddbtest.NumericKeyEntity{Id: 42, Version: 1},
			expectedPK: "42",
		},
		{
			name:       "Zero numeric",
			entity:     &ddbtest.NumericKeyEntity{Id: 0, Version: 1},
			expectedPK: "", // Zero values are omitted
		},
		{
			name: "Pointer to string",
			entity: func() *ddbtest.PointerKeyEntity {
				s := "test"
				return &ddbtest.PointerKeyEntity{Id: &s, SortVal: ddbtest.Ptr(1)}
			}(),
			expectedPK: "test",
		},
		{
			name:       "Nil pointer",
			entity:     &ddbtest.PointerKeyEntity{Id: nil, SortVal: ddbtest.Ptr(1)},
			expectedPK: "",
		},
	}

	store, client := setupTestStore(t,
		new(ddbtest.CompositeKeyEntity),
		new(ddbtest.NumericKeyEntity),
		new(ddbtest.PointerKeyEntity),
	)
	defer deleteTestTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For empty PK, Create should fail
			if tt.expectedPK == "" {
				ge := store.Create(context.Background(), tt.entity)
				assert.Error(t, ge) // Expect KeyValueNotFound error
				return
			}

			// Create the entity
			assert.Success(t, store.Create(context.Background(), tt.entity))

			// Read back to verify keys were stored correctly
			assert.Success(t, store.Read(context.Background(), tt.entity))
		})
	}
}

// TestIndex_BuildKeyValue_PKSK tests section 1.2: Partition Key + Sort Key
func TestIndex_BuildKeyValue_PKSK(t *testing.T) {
	tests := []struct {
		name       string
		entity     *ddbtest.CompositeKeyEntity
		expectedPK string
		expectedSK string
	}{
		{
			name: "Both set",
			entity: &ddbtest.CompositeKeyEntity{
				PartitionKey: "tenant1",
				SortKey:      "item1",
				Data:         "test",
			},
			expectedPK: "tenant1",
			expectedSK: "item1",
		},
		// Removed: Test expectation was incorrect. Empty single string field is treated as "not set"
		// per framework design (index.go:399). This case is correctly tested in CRUD tests as
		// "create with missing required sk field" which expects failure.
		{
			name: "Both empty",
			entity: &ddbtest.CompositeKeyEntity{
				PartitionKey: "",
				SortKey:      "",
				Data:         "test",
			},
			expectedPK: "",
			expectedSK: "",
		},
	}

	store, client := setupTestStore(t, new(ddbtest.CompositeKeyEntity))
	defer deleteTestTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Empty PK should fail
			if tt.expectedPK == "" {
				ge := store.Create(context.Background(), tt.entity)
				assert.Error(t, ge)
				return
			}

			// Create the entity
			ge := store.Create(context.Background(), tt.entity)
			assert.Success(t, ge)

			// Read back and verify
			readEntity := &ddbtest.CompositeKeyEntity{
				PartitionKey: tt.entity.PartitionKey,
				SortKey:      tt.entity.SortKey,
			}
			ge = store.Read(context.Background(), readEntity)
			assert.Success(t, ge)
			assert.Equals(t, tt.entity.Data, readEntity.Data)
		})
	}
}

// ==============================================================================
// Section 2: Composite Key Construction Tests
// ==============================================================================

// TestIndex_BuildKeyValue_CompositeKeys tests section 2.1: Multi-Part Partition Key
func TestIndex_BuildKeyValue_CompositeKeys(t *testing.T) {
	tests := []struct {
		name       string
		entity     *ddbtest.MultiPartKeyEntity
		expectedPK string
	}{
		{
			name: "Two parts",
			entity: &ddbtest.MultiPartKeyEntity{
				TenantId:   "T1",
				EntityType: "USER",
				Id:         "123",
			},
			expectedPK: "T1#USER",
		},
		{
			name: "Missing last",
			entity: &ddbtest.MultiPartKeyEntity{
				TenantId:   "T1",
				EntityType: "USER",
				Id:         "234",
			},
			expectedPK: "T1#USER",
		},
		{
			name: "First part empty",
			entity: &ddbtest.MultiPartKeyEntity{
				TenantId:   "",
				EntityType: "USER",
				Id:         "123",
			},
			expectedPK: "#USER", // Empty first part produces "#USER" (separator + second part)
		},
	}

	store, client := setupTestStore(t, new(ddbtest.MultiPartKeyEntity))
	defer deleteTestTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create and verify
			ge := store.Create(context.Background(), tt.entity)
			assert.Success(t, ge)

			// Read back
			readEntity := &ddbtest.MultiPartKeyEntity{
				TenantId:   tt.entity.TenantId,
				EntityType: tt.entity.EntityType,
				Id:         tt.entity.Id,
			}
			ge = store.Read(context.Background(), readEntity)
			assert.Success(t, ge)
		})
	}
}

// ==============================================================================
// Section 4: Separator Escaping Tests
// ==============================================================================

// TestIndex_EscapeKeyValue tests section 4.1: Field Value Escaping
func TestIndex_EscapeKeyValue(t *testing.T) {
	tests := []struct {
		name     string
		toEscape string
		escaped  string
	}{
		{
			name:     "No escaping needed",
			toEscape: "simple",
			escaped:  "simple",
		},
		{
			name:     "Escape separator",
			toEscape: "has#separator",
			escaped:  "has$#separator",
		},
		{
			name:     "Escape the escape char",
			toEscape: "has$escape",
			escaped:  "has$$escape",
		},
		//{
		//	name:           "Empty string",
		//	toEscape:       "",
		//	expectedOutput: "",
		//},
		{
			name:     "Multiple separators",
			toEscape: "multiple#values#here",
			escaped:  "multiple$#values$#here",
		},
	}

	store, client := setupTestStore(t, new(ddbtest.EscapedValueEntity))
	defer deleteTestTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test via actual entity with special characters
			entity := &ddbtest.EscapedValueEntity{
				Id:       "test",
				ToEscape: tt.toEscape,
				Trailing: "foo",
			}

			// Create entity
			ge := store.Create(context.Background(), entity)
			assert.Success(t, ge)

			// Validate the
			out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
				TableName: new(tableName),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: entity.Id},
					"SK": &types.AttributeValueMemberS{Value: tt.escaped + "#" + entity.Trailing},
				},
			})
			assert.Success(t, err)
			assert.Assert(t, out.Item != nil)

			// Read back - values should be unescaped
			readEntity := &ddbtest.EscapedValueEntity{
				Id:       "test",
				ToEscape: tt.toEscape,
				Trailing: "foo",
			}
			ge = store.Read(context.Background(), readEntity)
			assert.Success(t, ge)

			// Verify the value roundtrips correctly
			assert.Equals(t, tt.toEscape, readEntity.ToEscape)
		})
	}
}
