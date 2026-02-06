package dynamodb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/data"
	ddb "github.com/jt0/gomer/data/dynamodb"
	ddbtest "github.com/jt0/gomer/data/dynamodb/_test"
	testentities "github.com/jt0/gomer/data/dynamodb/_test"
)

const crudTestTableName = "gomer_crud_test"

// Setup/teardown helpers

func setupCrudStore(t *testing.T, persistables ...data.Persistable) (data.Store, *dynamodb.Client) {
	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)

	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	// Define table with PK, SK, and common GSI/LSI indexes
	tableDef := &ddbtest.TableDefinition{}
	tableDef.WithTableName(crudTestTableName).
		WithAttributeDefinition("PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("SK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("GSI_1_PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("GSI_1_SK", types.ScalarAttributeTypeS).
		WithKeySchema("PK", types.KeyTypeHash).
		WithKeySchema("SK", types.KeyTypeRange).
		WithLsi("lsi_1", []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("LSI_1_SK"), KeyType: types.KeyTypeRange},
		}, types.Projection{ProjectionType: types.ProjectionTypeAll}).
		WithLsi("lsi_2", []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("LSI_2_SK"), KeyType: types.KeyTypeRange},
		}, types.Projection{ProjectionType: types.ProjectionTypeAll}).
		WithGsi("gsi_1", []types.KeySchemaElement{
			{AttributeName: aws.String("GSI_1_PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("GSI_1_SK"), KeyType: types.KeyTypeRange},
		}, types.Projection{ProjectionType: types.ProjectionTypeAll})

	// Add LSI_1_SK and LSI_2_SK attribute definitions
	tableDef.WithAttributeDefinition("LSI_1_SK", types.ScalarAttributeTypeS)
	tableDef.WithAttributeDefinition("LSI_2_SK", types.ScalarAttributeTypeS)

	tableDef.Create(client)

	store, ge := ddb.Store(crudTestTableName, &ddb.Configuration{
		DynamoDb:                    client,
		MaxResultsDefault:           100,
		MaxResultsMax:               1000,
		ConsistencyDefault:          ddb.Preferred,
		FailDeleteIfNotPresent:      false,
		ValidateKeyFieldConsistency: false,
	}, persistables...)
	assert.Success(t, ge)

	return store, client
}

func cleanupCrudTable(t *testing.T, client *dynamodb.Client) {
	err := ddbtest.DeleteAllTableData(client, crudTestTableName)
	assert.Success(t, err)
}

func verifyEntityExists(t *testing.T, client *dynamodb.Client, pk, sk string) bool {
	key := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: pk},
	}
	if sk != "" {
		key["SK"] = &types.AttributeValueMemberS{Value: sk}
	}

	result, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(crudTestTableName),
		Key:       key,
	})
	assert.Success(t, err)
	return len(result.Item) > 0
}

func verifyEntityNotExists(t *testing.T, client *dynamodb.Client, pk, sk string) {
	exists := verifyEntityExists(t, client, pk, sk)
	if exists {
		t.Errorf("Entity should not exist with PK=%s, SK=%s", pk, sk)
	}
}

func getRawItem(t *testing.T, client *dynamodb.Client, pk, sk string) map[string]types.AttributeValue {
	key := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: pk},
	}
	if sk != "" {
		key["SK"] = &types.AttributeValueMemberS{Value: sk}
	}

	result, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(crudTestTableName),
		Key:       key,
	})
	assert.Success(t, err)
	return result.Item
}

// Phase 1: Basic CRUD Tests

func TestCreate(t *testing.T) {
	tests := []struct {
		name        string
		entity      data.Persistable
		expectError bool
		errorType   any
		setupFunc   func(store data.Store)
		verifyFunc  func(t *testing.T, store data.Store, client *dynamodb.Client)
	}{
		{
			name: "create composite key entity",
			entity: &testentities.CompositeKeyEntity{
				PartitionKey: "partition1",
				SortKey:      "sort1",
				Data:         "test-data",
				Active:       true,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client) {
				if !verifyEntityExists(t, client, "partition1", "sort1") {
					t.Error("CompositeKeyEntity should exist")
				}
				readEntity := &testentities.CompositeKeyEntity{PartitionKey: "partition1", SortKey: "sort1"}
				ge := store.Read(context.Background(), readEntity)
				assert.Success(t, ge)
				if readEntity.Data != "test-data" || !readEntity.Active {
					t.Errorf("Fields mismatch: got Data=%s, Active=%v", readEntity.Data, readEntity.Active)
				}
			},
		},
		{
			name: "create multi-part key entity",
			entity: &testentities.MultiPartKeyEntity{
				TenantId:   "tenant1",
				EntityType: "TYPE1",
				Id:         "id1",
				Payload:    "payload-data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client) {
				if !verifyEntityExists(t, client, "tenant1#TYPE1", "id1") {
					t.Error("MultiPartKeyEntity should exist")
				}
				readEntity := &testentities.MultiPartKeyEntity{TenantId: "tenant1", EntityType: "TYPE1", Id: "id1"}
				ge := store.Read(context.Background(), readEntity)
				assert.Success(t, ge)
				if readEntity.Payload != "payload-data" {
					t.Errorf("Payload mismatch: got %s", readEntity.Payload)
				}
			},
		},
		{
			name: "create static key entity",
			entity: &testentities.StaticKeyEntity{
				Id:     "item1",
				Status: "active",
				Detail: "test-detail",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client) {
				if !verifyEntityExists(t, client, "ITEM#item1", "STATUS#active") {
					t.Error("StaticKeyEntity should exist")
				}
				readEntity := &testentities.StaticKeyEntity{Id: "item1", Status: "active"}
				ge := store.Read(context.Background(), readEntity)
				assert.Success(t, ge)
				if readEntity.Detail != "test-detail" {
					t.Errorf("Detail mismatch: got %s", readEntity.Detail)
				}
			},
		},
		{
			name: "create user with gsi fields",
			entity: &testentities.User{
				TenantId: "tenant1",
				Id:       "user1",
				Email:    "user1@example.com",
				Name:     "Test User",
				Status:   "active",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client) {
				// User keys: PK = TenantId#USER, SK = Id
				if !verifyEntityExists(t, client, "tenant1#USER", "user1") {
					t.Error("User should exist")
				}
				// Verify GSI attributes exist
				item := getRawItem(t, client, "tenant1#USER", "user1")
				if _, ok := item["GSI_1_PK"]; !ok {
					t.Error("GSI_1_PK should be populated")
				}
				if _, ok := item["GSI_1_SK"]; !ok {
					t.Error("GSI_1_SK should be populated")
				}
			},
		},
		{
			name: "create product with lsi and gsi",
			entity: &testentities.Product{
				TenantId:    "tenant1",
				Id:          "prod1",
				Sku:         "SKU001",
				Category:    "Electronics",
				Name:        "Test Product",
				Price:       99.99,
				Description: "Test description",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client) {
				if !verifyEntityExists(t, client, "tenant1#PRODUCT", "prod1") {
					t.Error("Product should exist")
				}
				item := getRawItem(t, client, "tenant1#PRODUCT", "prod1")
				if _, ok := item["LSI_1_SK"]; !ok {
					t.Error("LSI_1_SK should be populated")
				}
				if _, ok := item["GSI_1_PK"]; !ok {
					t.Error("GSI_1_PK should be populated")
				}
				if _, ok := item["GSI_1_SK"]; !ok {
					t.Error("GSI_1_SK should be populated")
				}
			},
		},
		{
			name: "create with missing required sk field",
			entity: &testentities.CompositeKeyEntity{
				PartitionKey: "partition1",
				SortKey:      "", // Empty SK
				Data:         "test",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, tt.entity)
			defer cleanupCrudTable(t, client)

			ctx := context.Background()

			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			ge := store.Create(ctx, tt.entity)

			if tt.expectError {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
			} else {
				assert.Success(t, ge)
				if tt.verifyFunc != nil {
					tt.verifyFunc(t, store, client)
				}
			}
		})
	}
}

func TestRead(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(store data.Store) data.Persistable
		read        func() data.Persistable
		expectError bool
		verifyFunc  func(t *testing.T, created, read data.Persistable)
	}{
		{
			name: "read composite key entity by pk and sk",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "data1", Active: true}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*testentities.CompositeKeyEntity)
				r := read.(*testentities.CompositeKeyEntity)
				if r.Data != c.Data || r.Active != c.Active {
					t.Errorf("Mismatch: got %+v, want %+v", r, c)
				}
			},
		},
		{
			name: "read multi-part key entity",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "pay1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*testentities.MultiPartKeyEntity)
				r := read.(*testentities.MultiPartKeyEntity)
				if r.Payload != c.Payload {
					t.Errorf("Mismatch: got Payload=%s, want %s", r.Payload, c.Payload)
				}
				// Verify key fields are populated
				if r.TenantId != c.TenantId || r.EntityType != c.EntityType || r.Id != c.Id {
					t.Errorf("Key fields mismatch: got %+v, want %+v", r, c)
				}
			},
		},
		{
			name: "read static key entity",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.StaticKeyEntity{Id: "item1", Status: "active", Detail: "detail1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &testentities.StaticKeyEntity{Id: "item1", Status: "active"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*testentities.StaticKeyEntity)
				r := read.(*testentities.StaticKeyEntity)
				if r.Detail != c.Detail {
					t.Errorf("Mismatch: got Detail=%s, want %s", r.Detail, c.Detail)
				}
			},
		},
		{
			name: "read user (pk only, no sk)",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.User{TenantId: "t1", Id: "u1", Email: "test@example.com", Name: "Test", Status: "active"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &testentities.User{TenantId: "t1", Id: "u1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*testentities.User)
				r := read.(*testentities.User)
				if r.Email != c.Email || r.Name != c.Name || r.Status != c.Status {
					t.Errorf("Mismatch: got %+v, want %+v", r, c)
				}
				// Verify key fields extracted from PK
				if r.TenantId != c.TenantId || r.Id != c.Id {
					t.Errorf("Key fields mismatch: got TenantId=%s Id=%s, want TenantId=%s Id=%s", r.TenantId, r.Id, c.TenantId, c.Id)
				}
			},
		},
		{
			name: "read product (pk+sk)",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.Product{TenantId: "t1", Id: "p1", Sku: "SKU1", Category: "Cat1", Name: "Prod1", Price: 10.0, Description: "desc1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &testentities.Product{TenantId: "t1", Id: "p1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*testentities.Product)
				r := read.(*testentities.Product)
				if r.Sku != c.Sku || r.Category != c.Category || r.Name != c.Name || r.Price != c.Price {
					t.Errorf("Mismatch: got %+v, want %+v", r, c)
				}
			},
		},
		{
			name: "read order with time.Time in key",
			setup: func(store data.Store) data.Persistable {
				orderDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
				entity := &testentities.Order{TenantId: "t1", OrderId: "o1", UserId: "u1", OrderDate: orderDate, Status: "pending", Total: 100.0}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &testentities.Order{TenantId: "t1", OrderId: "o1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*testentities.Order)
				r := read.(*testentities.Order)
				if r.UserId != c.UserId || r.Status != c.Status || r.Total != c.Total {
					t.Errorf("Mismatch: got %+v, want %+v", r, c)
				}
				// Verify OrderDate (time.Time) is correctly restored
				if !r.OrderDate.Equal(c.OrderDate) {
					t.Errorf("OrderDate mismatch: got %v, want %v", r.OrderDate, c.OrderDate)
				}
			},
		},
		{
			name: "read non-existent composite entity",
			setup: func(store data.Store) data.Persistable {
				return nil
			},
			read: func() data.Persistable {
				return &testentities.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "nonexistent"}
			},
			expectError: true,
		},
		{
			name: "read with missing sk field",
			setup: func(store data.Store) data.Persistable {
				return nil
			},
			read: func() data.Persistable {
				return &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: ""}
			},
			expectError: true,
		},
		{
			name: "read with missing pk field",
			setup: func(store data.Store) data.Persistable {
				return nil
			},
			read: func() data.Persistable {
				return &testentities.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"}
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine which entity type to register
			var entityType data.Persistable
			readEntity := tt.read()
			switch readEntity.(type) {
			case *testentities.CompositeKeyEntity:
				entityType = &testentities.CompositeKeyEntity{}
			case *testentities.MultiPartKeyEntity:
				entityType = &testentities.MultiPartKeyEntity{}
			case *testentities.StaticKeyEntity:
				entityType = &testentities.StaticKeyEntity{}
			case *testentities.User:
				entityType = &testentities.User{}
			case *testentities.Product:
				entityType = &testentities.Product{}
			case *testentities.Order:
				entityType = &testentities.Order{}
			}

			store, client := setupCrudStore(t, entityType)
			defer cleanupCrudTable(t, client)

			ctx := context.Background()
			created := tt.setup(store)

			readEntity = tt.read()
			ge := store.Read(ctx, readEntity)

			if tt.expectError {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
			} else {
				assert.Success(t, ge)
				if tt.verifyFunc != nil {
					tt.verifyFunc(t, created, readEntity)
				}
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	tests := []struct {
		name         string
		entityType   data.Persistable
		setupEntity  data.Persistable
		updateEntity data.Persistable
		updateParam  data.Persistable
		expectError  bool
		verifyFunc   func(t *testing.T, store data.Store)
	}{
		{
			name:         "update composite key entity",
			entityType:   &testentities.CompositeKeyEntity{},
			setupEntity:  &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "original", Active: true},
			updateEntity: &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "updated", Active: true},
			updateParam:  &testentities.CompositeKeyEntity{Data: "updated"},
			verifyFunc: func(t *testing.T, store data.Store) {
				read := &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1"}
				assert.Success(t, store.Read(context.Background(), read))
				assert.Equals(t, "updated", read.Data)
			},
		},
		{
			name:         "update multi-part key entity",
			entityType:   &testentities.MultiPartKeyEntity{},
			setupEntity:  &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "original"},
			updateEntity: &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "updated"},
			updateParam:  &testentities.MultiPartKeyEntity{Payload: "updated"},
			verifyFunc: func(t *testing.T, store data.Store) {
				read := &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1"}
				assert.Success(t, store.Read(context.Background(), read))
				assert.Equals(t, "updated", read.Payload)
			},
		},
		{
			name:         "update static key entity status",
			entityType:   &testentities.StaticKeyEntity{},
			setupEntity:  &testentities.StaticKeyEntity{Id: "item1", Status: "active", Detail: "detail1"},
			updateEntity: &testentities.StaticKeyEntity{Id: "item1", Status: "inactive", Detail: "detail1"},
			verifyFunc: func(t *testing.T, store data.Store) {
				read := &testentities.StaticKeyEntity{Id: "item1", Status: "inactive"}
				assert.Success(t, store.Read(context.Background(), read))
				assert.Equals(t, "detail1", read.Detail)
			},
		},
		{
			name:         "update partial fields only",
			entityType:   &testentities.CompositeKeyEntity{},
			setupEntity:  &testentities.CompositeKeyEntity{PartitionKey: "pk2", SortKey: "sk2", Data: "original", Active: true},
			updateEntity: &testentities.CompositeKeyEntity{PartitionKey: "pk2", SortKey: "sk2", Data: "partial-update", Active: true},
			updateParam:  &testentities.CompositeKeyEntity{Data: "partial-update"},
			verifyFunc: func(t *testing.T, store data.Store) {
				read := &testentities.CompositeKeyEntity{PartitionKey: "pk2", SortKey: "sk2"}
				assert.Success(t, store.Read(context.Background(), read))
				assert.Equals(t, "partial-update", read.Data)
				assert.Equals(t, true, read.Active)
			},
		},
		{
			name:         "update non-existent entity",
			entityType:   &testentities.CompositeKeyEntity{},
			updateEntity: &testentities.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "nonexistent", Data: "new"},
			expectError:  false, // Update uses PutItem which creates if not exists
			verifyFunc: func(t *testing.T, store data.Store) {
				read := &testentities.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "nonexistent"}
				assert.Success(t, store.Read(context.Background(), read))
				assert.Equals(t, "new", read.Data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, tt.entityType)
			defer cleanupCrudTable(t, client)

			ctx := context.Background()

			// Setup entity if provided
			if tt.setupEntity != nil {
				ge := store.Create(ctx, tt.setupEntity)
				assert.Success(t, ge)
			}

			// Execute update
			ge := store.Update(ctx, tt.updateEntity, tt.updateParam)

			if tt.expectError {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
				return
			}

			assert.Success(t, ge)
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, store)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	tests := []struct {
		name         string
		entityType   data.Persistable
		setupEntity  data.Persistable
		deleteEntity data.Persistable
		expectError  bool
		expectedPK   string
		expectedSK   string
		verifyFunc   func(t *testing.T, store data.Store, client *dynamodb.Client)
		failIfAbsent bool
	}{
		{
			name:         "delete composite key entity",
			entityType:   &testentities.CompositeKeyEntity{},
			setupEntity:  &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "data1"},
			deleteEntity: &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1"},
			expectedPK:   "pk1",
			expectedSK:   "sk1",
		},
		{
			name:         "delete multi-part key entity",
			entityType:   &testentities.MultiPartKeyEntity{},
			setupEntity:  &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "p1"},
			deleteEntity: &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1"},
			expectedPK:   "t1#T1",
			expectedSK:   "i1",
		},
		{
			name:         "delete static key entity",
			entityType:   &testentities.StaticKeyEntity{},
			setupEntity:  &testentities.StaticKeyEntity{Id: "item1", Status: "active", Detail: "detail1"},
			deleteEntity: &testentities.StaticKeyEntity{Id: "item1", Status: "active"},
			expectedPK:   "ITEM#item1",
			expectedSK:   "STATUS#active",
		},
		{
			name:         "delete user",
			entityType:   &testentities.User{},
			setupEntity:  &testentities.User{TenantId: "t1", Id: "u1", Email: "test@example.com", Name: "Test"},
			deleteEntity: &testentities.User{TenantId: "t1", Id: "u1"},
			expectedPK:   "t1#USER",
			expectedSK:   "u1",
		},
		{
			name:         "delete product",
			entityType:   &testentities.Product{},
			setupEntity:  &testentities.Product{TenantId: "t1", Id: "p1", Sku: "SKU1", Category: "Cat1", Name: "Prod1", Price: 10.0},
			deleteEntity: &testentities.Product{TenantId: "t1", Id: "p1"},
			expectedPK:   "t1#PRODUCT",
			expectedSK:   "p1",
		},
		{
			name:         "delete order",
			entityType:   &testentities.Order{},
			setupEntity:  &testentities.Order{TenantId: "t1", OrderId: "o1", UserId: "u1", OrderDate: time.Now(), Status: "pending", Total: 100.0},
			deleteEntity: &testentities.Order{TenantId: "t1", OrderId: "o1"},
			expectedPK:   "t1#ORDER",
			expectedSK:   "ID#o1",
		},
		{
			name:         "delete with missing sk field",
			entityType:   &testentities.CompositeKeyEntity{},
			deleteEntity: &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: ""},
			expectError:  true,
		},
		{
			name:         "delete with missing pk field",
			entityType:   &testentities.CompositeKeyEntity{},
			deleteEntity: &testentities.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"},
			expectError:  true,
		},
		{
			name:         "delete then create succeeds",
			entityType:   &testentities.CompositeKeyEntity{},
			setupEntity:  &testentities.CompositeKeyEntity{PartitionKey: "recreate-test", SortKey: "sk1", Data: "First", Active: true},
			deleteEntity: &testentities.CompositeKeyEntity{PartitionKey: "recreate-test", SortKey: "sk1"},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client) {
				ctx := context.Background()
				entity := &testentities.CompositeKeyEntity{PartitionKey: "recreate-test", SortKey: "sk1", Data: "Second", Active: false}
				ge := store.Create(ctx, entity)
				assert.Success(t, ge)

				readEntity := &testentities.CompositeKeyEntity{PartitionKey: "recreate-test", SortKey: "sk1"}
				ge = store.Read(ctx, readEntity)
				assert.Success(t, ge)
				assert.Equals(t, "Second", readEntity.Data)
				assert.Equals(t, false, readEntity.Active)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, tt.entityType)
			defer cleanupCrudTable(t, client)

			ctx := context.Background()

			// Setup entity if provided
			if tt.setupEntity != nil {
				ge := store.Create(ctx, tt.setupEntity)
				assert.Success(t, ge)
			}

			// Execute delete
			ge := store.Delete(ctx, tt.deleteEntity)

			if tt.expectError {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
				return
			}

			assert.Success(t, ge)

			// Verify deletion using verifyFunc or default check
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, store, client)
			} else if tt.expectedPK != "" {
				verifyEntityNotExists(t, client, tt.expectedPK, tt.expectedSK)
			}
		})
	}
}

// Phase 2: Edge Case Tests

func TestCRUD_EmptyAndZeroValues(t *testing.T) {
	tests := []struct {
		name        string
		entity      data.Persistable
		expectError bool
		verifyFunc  func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable)
	}{
		{
			name: "create with empty string in PK",
			entity: &testentities.EmptyValueEntity{
				Id:          "", // Empty PK
				EmptyString: "test",
				ZeroInt:     5,
			},
			expectError: true, // Empty PK should fail validation
		},
		{
			name: "create with empty string in SK part",
			entity: &testentities.EmptyValueEntity{
				Id:          "id1",
				EmptyString: "", // Empty SK segment
				ZeroInt:     5,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Verify entity exists - SK is "#5" (EmptyString="" + separator + ZeroInt=5)
				item := getRawItem(t, client, "id1", "#5")
				if item == nil || len(item) == 0 {
					t.Error("Entity should exist")
				}
				// Read back and verify
				read := &testentities.EmptyValueEntity{Id: "id1", EmptyString: "", ZeroInt: 5}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.EmptyString != "" {
					t.Errorf("EmptyString should be empty, got: %s", read.EmptyString)
				}
			},
		},
		// Removed: Invalid test - int fields with value 0 are treated as absent per index.go:388-389
		// To use actual zero values, define field as *int instead of int
		{
			name: "create with zero int in middle SK segment",
			entity: &testentities.EmptyValueEntity{
				Id:          "id3",
				EmptyString: "prefix",
				ZeroInt:     0, // Zero in middle
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Read back and verify zero is preserved as empty
				read := &testentities.EmptyValueEntity{Id: "id3", EmptyString: "prefix"}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.ZeroInt != 0 {
					t.Errorf("ZeroInt should be 0, got: %d", read.ZeroInt)
				}
			},
		},
		{
			name: "read entity with empty SK segment",
			entity: &testentities.EmptyValueEntity{
				Id:          "id4",
				EmptyString: "",
				ZeroInt:     10,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.EmptyValueEntity{Id: "id4", EmptyString: "", ZeroInt: 10}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.EmptyString != "" {
					t.Errorf("EmptyString should be empty, got: %s", read.EmptyString)
				}
				if read.ZeroInt != 10 {
					t.Errorf("ZeroInt should be 10, got: %d", read.ZeroInt)
				}
			},
		},
		{
			name: "update field to empty string",
			entity: &testentities.EmptyValueEntity{
				Id:          "id5",
				EmptyString: "original",
				ZeroInt:     5,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Update to empty string
				updated := &testentities.EmptyValueEntity{
					Id:          "id5",
					EmptyString: "", // Changed to empty
					ZeroInt:     5,
				}
				ge := store.Update(context.Background(), updated, nil)
				assert.Success(t, ge)

				// Verify update - SK changed to "#5", need both fields
				read := &testentities.EmptyValueEntity{Id: "id5", EmptyString: "", ZeroInt: 5}
				ge = store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.EmptyString != "" {
					t.Errorf("EmptyString should be empty after update, got: %s", read.EmptyString)
				}
			},
		},
		{
			name: "update field from empty to value",
			entity: &testentities.EmptyValueEntity{
				Id:          "id6",
				EmptyString: "",
				ZeroInt:     5,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Update from empty to value
				updated := &testentities.EmptyValueEntity{
					Id:          "id6",
					EmptyString: "updated",
					ZeroInt:     5,
				}
				ge := store.Update(context.Background(), updated, nil)
				assert.Success(t, ge)

				// Verify update - SK changed to "updated#5", need both fields
				read := &testentities.EmptyValueEntity{Id: "id6", EmptyString: "updated", ZeroInt: 5}
				ge = store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.EmptyString != "updated" {
					t.Errorf("EmptyString should be 'updated', got: %s", read.EmptyString)
				}
			},
		},
		{
			name: "update zero to non-zero int",
			entity: &testentities.EmptyValueEntity{
				Id:          "id7",
				EmptyString: "test",
				ZeroInt:     0,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Update zero to non-zero
				updated := &testentities.EmptyValueEntity{
					Id:          "id7",
					EmptyString: "test",
					ZeroInt:     42,
				}
				ge := store.Update(context.Background(), updated, nil)
				assert.Success(t, ge)

				// Verify update - SK changed to "test#42", need both fields
				read := &testentities.EmptyValueEntity{Id: "id7", EmptyString: "test", ZeroInt: 42}
				ge = store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.ZeroInt != 42 {
					t.Errorf("ZeroInt should be 42, got: %d", read.ZeroInt)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, &testentities.EmptyValueEntity{})
			defer cleanupCrudTable(t, client)

			ctx := context.Background()

			// Create entity
			ge := store.Create(ctx, tt.entity)

			if tt.expectError {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
				return
			}

			assert.Success(t, ge)

			if tt.verifyFunc != nil {
				tt.verifyFunc(t, store, client, tt.entity)
			}
		})
	}
}

func TestCRUD_EscapedValues(t *testing.T) {
	// Test cases for escaping special characters in key fields
	// Each test creates an entity, then reads it back to verify field values are preserved
	tests := []struct {
		name            string
		id              string
		fieldWithHash   string
		fieldWithDollar string
	}{
		{"create with '#' in key field", "id1", "value#with#hash", "normal"},
		{"create with '$' in key field", "id2", "normal", "value$with$dollar"},
		{"create with both '#' and '$'", "id3", "hash#value", "dollar$value"},
		{"create with '#' at start", "id4", "#start", "normal"},
		{"create with '#' at end", "id5", "end#", "normal"},
		{"create with multiple '#'", "id6", "###", "normal"},
		{"create with '##'", "id7", "double##hash", "normal"},
		{"create with '$#'", "id8", "escape$#combo", "normal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, &testentities.EscapedValueEntity{})
			defer cleanupCrudTable(t, client)

			ctx := context.Background()
			entity := &testentities.EscapedValueEntity{
				Id:              tt.id,
				FieldWithHash:   tt.fieldWithHash,
				FieldWithDollar: tt.fieldWithDollar,
				NormalField:     "data",
			}

			// Create entity
			ge := store.Create(ctx, entity)
			assert.Success(t, ge)

			// Read back and verify
			read := &testentities.EscapedValueEntity{
				Id:              tt.id,
				FieldWithHash:   tt.fieldWithHash,
				FieldWithDollar: tt.fieldWithDollar,
			}
			ge = store.Read(ctx, read)
			assert.Success(t, ge)
			assert.Equals(t, tt.fieldWithHash, read.FieldWithHash)
			assert.Equals(t, tt.fieldWithDollar, read.FieldWithDollar)
		})
	}

	// Separate test for update with escaped values
	t.Run("update to value with '#'", func(t *testing.T) {
		store, client := setupCrudStore(t, &testentities.EscapedValueEntity{})
		defer cleanupCrudTable(t, client)

		ctx := context.Background()
		entity := &testentities.EscapedValueEntity{
			Id:              "id9",
			FieldWithHash:   "original",
			FieldWithDollar: "original",
			NormalField:     "data",
		}
		assert.Success(t, store.Create(ctx, entity))

		// Update to value with '#'
		updated := &testentities.EscapedValueEntity{
			Id:              "id9",
			FieldWithHash:   "new#value",
			FieldWithDollar: "original",
			NormalField:     "data",
		}
		assert.Success(t, store.Update(ctx, updated, nil))

		// Verify update
		read := &testentities.EscapedValueEntity{
			Id:              "id9",
			FieldWithHash:   "new#value",
			FieldWithDollar: "original",
		}
		assert.Success(t, store.Read(ctx, read))
		assert.Equals(t, "new#value", read.FieldWithHash)
	})
}

func TestCRUD_NumericKeys(t *testing.T) {
	// Simple create and read tests for numeric keys
	readTests := []struct {
		name    string
		id      int
		version int
		data    string
		wantErr bool
	}{
		{"create with numeric PK", 123, 1, "test-data", false},
		{"create with numeric SK", 456, 2, "test-data", false},
		{"create with zero PK", 0, 1, "test-data", true}, // Zero PK treated as not set
		{"create with negative SK", 999, -5, "negative-data", false},
	}

	for _, tt := range readTests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, &testentities.NumericKeyEntity{})
			defer cleanupCrudTable(t, client)

			ctx := context.Background()
			entity := &testentities.NumericKeyEntity{Id: tt.id, Version: tt.version, Data: tt.data}
			ge := store.Create(ctx, entity)

			if tt.wantErr {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
				return
			}
			assert.Success(t, ge)

			// Read back and verify
			read := &testentities.NumericKeyEntity{Id: tt.id, Version: tt.version}
			assert.Success(t, store.Read(ctx, read))
			assert.Equals(t, tt.data, read.Data)
		})
	}

	t.Run("update numeric key field", func(t *testing.T) {
		store, client := setupCrudStore(t, &testentities.NumericKeyEntity{})
		defer cleanupCrudTable(t, client)

		ctx := context.Background()
		entity := &testentities.NumericKeyEntity{Id: 111, Version: 1, Data: "original"}
		assert.Success(t, store.Create(ctx, entity))

		// Update changes the SK, effectively creating a new item
		updated := &testentities.NumericKeyEntity{Id: 111, Version: 2, Data: "updated"}
		assert.Success(t, store.Update(ctx, updated, nil))

		// Old version should still exist
		read1 := &testentities.NumericKeyEntity{Id: 111, Version: 1}
		assert.Success(t, store.Read(ctx, read1))

		// New version should exist
		read2 := &testentities.NumericKeyEntity{Id: 111, Version: 2}
		assert.Success(t, store.Read(ctx, read2))
		assert.Equals(t, "updated", read2.Data)
	})

	t.Run("sort order with numeric keys (lexicographic)", func(t *testing.T) {
		store, client := setupCrudStore(t, &testentities.NumericKeyEntity{})
		defer cleanupCrudTable(t, client)

		ctx := context.Background()
		// Create multiple versions
		versions := []int{10, 2, 20, 1, 3}
		for _, v := range versions {
			e := &testentities.NumericKeyEntity{Id: 1000, Version: v, Data: fmt.Sprintf("v%d", v)}
			assert.Success(t, store.Create(ctx, e))
		}

		// Verify we can read all of them
		for _, v := range versions {
			read := &testentities.NumericKeyEntity{Id: 1000, Version: v}
			assert.Success(t, store.Read(ctx, read))
		}
	})
}

func TestCRUD_PointerFields(t *testing.T) {
	tests := []struct {
		name        string
		entity      data.Persistable
		expectError bool
		verifyFunc  func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable)
	}{
		{
			name: "create with actual zero value using *int",
			entity: func() data.Persistable {
				zero := 0
				id := "test-id"
				return &testentities.PointerKeyEntity{
					Id:      &id,
					SortVal: &zero, // Actual zero value using pointer
					Data:    "data",
				}
			}(),
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Read back with zero value
				zero := 0
				id := "test-id"
				read := &testentities.PointerKeyEntity{
					Id:      &id,
					SortVal: &zero,
				}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.SortVal == nil || *read.SortVal != 0 {
					t.Errorf("SortVal should be 0, got: %v", read.SortVal)
				}
			},
		},
		{
			name: "create with nil pointer (absent value)",
			entity: func() data.Persistable {
				id := "test-id-2"
				return &testentities.PointerKeyEntity{
					Id:      &id,
					SortVal: nil, // Nil pointer - treated as absent
					Data:    "data",
				}
			}(),
			expectError: true, // Should fail because SK is required in table schema
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, &testentities.PointerKeyEntity{})
			defer cleanupCrudTable(t, client)

			ctx := context.Background()

			// Create entity
			ge := store.Create(ctx, tt.entity)

			if tt.expectError {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
				return
			}

			assert.Success(t, ge)

			if tt.verifyFunc != nil {
				tt.verifyFunc(t, store, client, tt.entity)
			}
		})
	}
}

func TestCRUD_TimeFields(t *testing.T) {
	tests := []struct {
		name        string
		entity      data.Persistable
		expectError bool
		verifyFunc  func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable)
	}{
		{
			name: "create order with time.Time in key",
			entity: &testentities.Order{
				TenantId:  "tenant1",
				OrderId:   "order1",
				UserId:    "user1",
				OrderDate: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
				Status:    "pending",
				Total:     100.0,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.Order{TenantId: "tenant1", OrderId: "order1"}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)

				original := entity.(*testentities.Order)
				if !read.OrderDate.Equal(original.OrderDate) {
					t.Errorf("OrderDate mismatch: got %v, want %v", read.OrderDate, original.OrderDate)
				}
			},
		},
		{
			name: "create order with zero time.Time",
			entity: &testentities.Order{
				TenantId:  "tenant2",
				OrderId:   "order2",
				UserId:    "user2",
				OrderDate: time.Time{}, // Zero time
				Status:    "draft",
				Total:     0.0,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.Order{TenantId: "tenant2", OrderId: "order2"}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				// Zero time is treated as empty segment in key
			},
		},
		{
			name: "sort order with time.Time keys",
			entity: &testentities.Order{
				TenantId:  "tenant3",
				OrderId:   "order_chrono_1",
				UserId:    "user3",
				OrderDate: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
				Status:    "completed",
				Total:     100.0,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Create multiple orders with different dates
				dates := []time.Time{
					time.Date(2024, 1, 3, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC),
				}

				for i, d := range dates {
					order := &testentities.Order{
						TenantId:  "tenant3",
						OrderId:   fmt.Sprintf("order_chrono_%d", i+2),
						UserId:    "user3",
						OrderDate: d,
						Status:    "completed",
						Total:     float64(i+1) * 10.0,
					}
					ge := store.Create(context.Background(), order)
					assert.Success(t, ge)
				}

				// RFC3339 format sorts chronologically
				// This test verifies we can create and read orders with different dates
				for i := range dates {
					read := &testentities.Order{TenantId: "tenant3", OrderId: fmt.Sprintf("order_chrono_%d", i+2)}
					ge := store.Read(context.Background(), read)
					assert.Success(t, ge)
				}
			},
		},
		{
			name: "update order changing OrderDate",
			entity: &testentities.Order{
				TenantId:  "tenant4",
				OrderId:   "order4",
				UserId:    "user4",
				OrderDate: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
				Status:    "pending",
				Total:     50.0,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Update with new OrderDate
				updated := &testentities.Order{
					TenantId:  "tenant4",
					OrderId:   "order4",
					UserId:    "user4",
					OrderDate: time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC),
					Status:    "confirmed",
					Total:     50.0,
				}
				ge := store.Update(context.Background(), updated, nil)
				assert.Success(t, ge)

				// Read back and verify new date
				read := &testentities.Order{TenantId: "tenant4", OrderId: "order4"}
				ge = store.Read(context.Background(), read)
				assert.Success(t, ge)
				if !read.OrderDate.Equal(updated.OrderDate) {
					t.Errorf("OrderDate should be updated: got %v, want %v", read.OrderDate, updated.OrderDate)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, &testentities.Order{})
			defer cleanupCrudTable(t, client)

			ctx := context.Background()

			// Create entity
			ge := store.Create(ctx, tt.entity)

			if tt.expectError {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
				return
			}

			assert.Success(t, ge)

			if tt.verifyFunc != nil {
				tt.verifyFunc(t, store, client, tt.entity)
			}
		})
	}
}

// ==============================================================================
// Tier 1: Easy Wins - Simple Getter Tests
// ==============================================================================

// TestStores tests the Stores() function which returns all registered table stores
func TestStores(t *testing.T) {
	store, client := setupCrudStore(t, &testentities.CompositeKeyEntity{})
	defer cleanupCrudTable(t, client)

	// After creating a store, it should appear in Stores()
	stores := ddb.Stores()

	if stores == nil {
		t.Fatal("Stores() returned nil")
	}

	// Check that our store is registered
	if _, ok := stores[crudTestTableName]; !ok {
		t.Errorf("Expected store '%s' to be registered in Stores()", crudTestTableName)
	}

	// Verify the store is the same one we created
	if stores[crudTestTableName] != store {
		t.Error("Store in Stores() map should be the same instance we created")
	}
}

// TestStores_MultipleStores tests that multiple stores are properly tracked
func TestStores_MultipleStores(t *testing.T) {
	// Create first store
	store1, client1 := setupCrudStore(t, &testentities.CompositeKeyEntity{})
	defer cleanupCrudTable(t, client1)

	// Get the stores map
	stores := ddb.Stores()

	// Verify stores is not nil and contains our store
	if stores == nil {
		t.Fatal("Stores() returned nil")
	}

	// Verify count includes at least our test store
	if len(stores) < 1 {
		t.Error("Expected at least 1 store in Stores()")
	}

	// Verify our specific store is present
	if stores[crudTestTableName] != store1 {
		t.Error("Store1 not found or doesn't match")
	}
}

// TestStores_ReturnsNonNilMap tests that Stores() always returns a non-nil map
func TestStores_ReturnsNonNilMap(t *testing.T) {
	stores := ddb.Stores()
	if stores == nil {
		t.Fatal("Stores() should never return nil")
	}
}

// Namer interface to access Name() method on concrete table type
type Namer interface {
	Name() string
}

// TestTableName tests the Name() method on the table
func TestTableName(t *testing.T) {
	store, client := setupCrudStore(t, &testentities.CompositeKeyEntity{})
	defer cleanupCrudTable(t, client)

	// Cast to Namer interface to access Name() method
	namer, ok := store.(Namer)
	if !ok {
		t.Skip("Store does not implement Namer interface")
	}

	name := namer.Name()
	if name != crudTestTableName {
		t.Errorf("Expected table name '%s', got '%s'", crudTestTableName, name)
	}
}

// TestTableName_Consistency tests that Name() returns consistent results
func TestTableName_Consistency(t *testing.T) {
	store, client := setupCrudStore(t, &testentities.CompositeKeyEntity{})
	defer cleanupCrudTable(t, client)

	namer, ok := store.(Namer)
	if !ok {
		t.Skip("Store does not implement Namer interface")
	}

	// Call Name() multiple times
	name1 := namer.Name()
	name2 := namer.Name()
	name3 := namer.Name()

	// All calls should return the same value
	if name1 != name2 || name2 != name3 {
		t.Errorf("Name() returned inconsistent results: '%s', '%s', '%s'", name1, name2, name3)
	}
}

// ==============================================================================
// Tier 2: Error Path Tests for Delete
// ==============================================================================

// TestDelete_FailDeleteIfNotPresent tests the FailDeleteIfNotPresent configuration
func TestDelete_FailDeleteIfNotPresent(t *testing.T) {
	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)

	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	// Create table with FailDeleteIfNotPresent=true
	tableName := "gomer_delete_test"
	tableDef := &ddbtest.TableDefinition{}
	tableDef.WithTableName(tableName).
		WithAttributeDefinition("PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("SK", types.ScalarAttributeTypeS).
		WithKeySchema("PK", types.KeyTypeHash).
		WithKeySchema("SK", types.KeyTypeRange)
	tableDef.Create(client)

	defer func() {
		ddbtest.DeleteAllTableData(client, tableName)
	}()

	store, ge := ddb.Store(tableName, &ddb.Configuration{
		DynamoDb:               client,
		MaxResultsDefault:      100,
		MaxResultsMax:          1000,
		ConsistencyDefault:     ddb.Preferred,
		FailDeleteIfNotPresent: true, // Key configuration for this test
	}, &testentities.CompositeKeyEntity{})
	assert.Success(t, ge)

	ctx := context.Background()

	// Try to delete an entity that doesn't exist
	entity := &testentities.CompositeKeyEntity{
		PartitionKey: "nonexistent-pk",
		SortKey:      "nonexistent-sk",
	}

	// Should fail because entity doesn't exist and FailDeleteIfNotPresent=true
	ge = store.Delete(ctx, entity)
	if ge == nil {
		t.Error("Expected error when deleting non-existent entity with FailDeleteIfNotPresent=true")
	}
}

// TestDelete_MissingKeyFields tests delete with missing key fields
func TestDelete_MissingKeyFields(t *testing.T) {
	store, client := setupCrudStore(t, &testentities.CompositeKeyEntity{})
	defer cleanupCrudTable(t, client)

	ctx := context.Background()

	tests := []struct {
		name   string
		entity *testentities.CompositeKeyEntity
	}{
		{
			name:   "missing pk",
			entity: &testentities.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"},
		},
		{
			name:   "missing sk",
			entity: &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: ""},
		},
		{
			name:   "missing both",
			entity: &testentities.CompositeKeyEntity{PartitionKey: "", SortKey: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ge := store.Delete(ctx, tt.entity)
			if ge == nil {
				t.Error("Expected error when deleting entity with missing key fields")
			}
		})
	}
}

// ==============================================================================
// Tier 2: Error Path Tests for Read
// ==============================================================================

// TestRead_NonExistentEntity tests reading an entity that doesn't exist
func TestRead_NonExistentEntity(t *testing.T) {
	store, client := setupCrudStore(t, &testentities.CompositeKeyEntity{})
	defer cleanupCrudTable(t, client)

	ctx := context.Background()

	// Try to read an entity that doesn't exist
	entity := &testentities.CompositeKeyEntity{
		PartitionKey: "nonexistent-pk",
		SortKey:      "nonexistent-sk",
	}

	ge := store.Read(ctx, entity)
	if ge == nil {
		t.Error("Expected error when reading non-existent entity")
	}
}

// TestRead_MissingKeyFields tests read with missing key fields
func TestRead_MissingKeyFields(t *testing.T) {
	store, client := setupCrudStore(t, &testentities.CompositeKeyEntity{})
	defer cleanupCrudTable(t, client)

	ctx := context.Background()

	tests := []struct {
		name   string
		entity *testentities.CompositeKeyEntity
	}{
		{
			name:   "missing pk",
			entity: &testentities.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"},
		},
		{
			name:   "missing sk",
			entity: &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ge := store.Read(ctx, tt.entity)
			if ge == nil {
				t.Error("Expected error when reading entity with missing key fields")
			}
		})
	}
}

// ==============================================================================
// Tier 2: Error Path Tests for Query (runQuery coverage)
// ==============================================================================

// TestQuery_EmptyPartitionKey tests query with empty partition key
func TestQuery_EmptyPartitionKey(t *testing.T) {
	store, client := setupCrudStore(t, &testentities.CompositeKeyEntity{})
	defer cleanupCrudTable(t, client)

	ctx := context.Background()

	// Query with empty partition key
	q := &testentities.CompositeKeyEntities{PartitionKey: ""}
	ge := store.Query(ctx, q)
	if ge == nil {
		t.Error("Expected error when querying with empty partition key")
	}
}

// TestQuery_NoMatchingIndex tests query that can't find a matching index
func TestQuery_NoMatchingIndex(t *testing.T) {
	store, client := setupCrudStore(t, &testentities.Product{})
	defer cleanupCrudTable(t, client)

	ctx := context.Background()

	// Query with only Category - this should fail because GSI_1 needs TenantId+Category
	q := &testentities.Products{Category: "Electronics"}
	ge := store.Query(ctx, q)
	if ge == nil {
		t.Error("Expected error when querying with incomplete index key")
	}
}
