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
		errorType   interface{}
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
		name        string
		setup       func(store data.Store) data.Persistable
		update      func(created data.Persistable) (updated data.Persistable, updateParam data.Persistable)
		expectError bool
		verifyFunc  func(t *testing.T, store data.Store, updated data.Persistable)
	}{
		{
			name: "update composite key entity",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "original", Active: true}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			update: func(created data.Persistable) (data.Persistable, data.Persistable) {
				updated := &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "updated", Active: true}
				updateParam := &testentities.CompositeKeyEntity{Data: "updated"}
				return updated, updateParam
			},
			verifyFunc: func(t *testing.T, store data.Store, updated data.Persistable) {
				read := &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1"}
				assert.Success(t, store.Read(context.Background(), read))
				if read.Data != "updated" {
					t.Errorf("Data should be updated: got %s", read.Data)
				}
			},
		},
		{
			name: "update multi-part key entity",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "original"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			update: func(created data.Persistable) (data.Persistable, data.Persistable) {
				updated := &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "updated"}
				updateParam := &testentities.MultiPartKeyEntity{Payload: "updated"}
				return updated, updateParam
			},
			verifyFunc: func(t *testing.T, store data.Store, updated data.Persistable) {
				read := &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1"}
				assert.Success(t, store.Read(context.Background(), read))
				if read.Payload != "updated" {
					t.Errorf("Payload should be updated: got %s", read.Payload)
				}
			},
		},
		{
			name: "update static key entity status",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.StaticKeyEntity{Id: "item1", Status: "active", Detail: "detail1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			update: func(created data.Persistable) (data.Persistable, data.Persistable) {
				updated := &testentities.StaticKeyEntity{Id: "item1", Status: "inactive", Detail: "detail1"}
				return updated, nil // Full update to rebuild SK
			},
			verifyFunc: func(t *testing.T, store data.Store, updated data.Persistable) {
				read := &testentities.StaticKeyEntity{Id: "item1", Status: "inactive"}
				assert.Success(t, store.Read(context.Background(), read))
				if read.Detail != "detail1" {
					t.Errorf("Detail should remain: got %s", read.Detail)
				}
			},
		},
		{
			name: "update partial fields only",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.CompositeKeyEntity{PartitionKey: "pk2", SortKey: "sk2", Data: "original", Active: true}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			update: func(created data.Persistable) (data.Persistable, data.Persistable) {
				updated := &testentities.CompositeKeyEntity{PartitionKey: "pk2", SortKey: "sk2", Data: "partial-update", Active: true}
				updateParam := &testentities.CompositeKeyEntity{Data: "partial-update"} // Only update Data
				return updated, updateParam
			},
			verifyFunc: func(t *testing.T, store data.Store, updated data.Persistable) {
				read := &testentities.CompositeKeyEntity{PartitionKey: "pk2", SortKey: "sk2"}
				assert.Success(t, store.Read(context.Background(), read))
				if read.Data != "partial-update" {
					t.Errorf("Data should be updated: got %s", read.Data)
				}
				if !read.Active {
					t.Error("Active should remain true (not in updateParam)")
				}
			},
		},
		{
			name: "update non-existent entity",
			setup: func(store data.Store) data.Persistable {
				return nil // No setup, entity doesn't exist
			},
			update: func(created data.Persistable) (data.Persistable, data.Persistable) {
				updated := &testentities.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "nonexistent", Data: "new"}
				return updated, nil
			},
			expectError: false, // Update uses PutItem which creates if not exists
			verifyFunc: func(t *testing.T, store data.Store, updated data.Persistable) {
				read := &testentities.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "nonexistent"}
				assert.Success(t, store.Read(context.Background(), read))
				if read.Data != "new" {
					t.Errorf("Data should be 'new': got %s", read.Data)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine entity type
			var entityType data.Persistable
			if tt.setup != nil {
				// Use a temporary store to determine type
				tempStore, tempClient := setupCrudStore(t, &testentities.CompositeKeyEntity{}, &testentities.MultiPartKeyEntity{}, &testentities.StaticKeyEntity{})
				created := tt.setup(tempStore)
				cleanupCrudTable(t, tempClient)

				if created != nil {
					entityType = created
				} else {
					// For non-existent entity test
					updated, _ := tt.update(nil)
					entityType = updated
				}
			} else {
				updated, _ := tt.update(nil)
				entityType = updated
			}

			store, client := setupCrudStore(t, entityType)
			defer cleanupCrudTable(t, client)

			ctx := context.Background()
			created := tt.setup(store)

			updated, updateParam := tt.update(created)
			ge := store.Update(ctx, updated, updateParam)

			if tt.expectError {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
			} else {
				assert.Success(t, ge)
				if tt.verifyFunc != nil {
					tt.verifyFunc(t, store, updated)
				}
			}
		})
	}
}

func TestDelete(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(store data.Store) data.Persistable
		delete       func(created data.Persistable) data.Persistable
		expectError  bool
		verifyFunc   func(t *testing.T, store data.Store, client *dynamodb.Client, deleted data.Persistable)
		failIfAbsent bool
	}{
		{
			name: "delete composite key entity",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "data1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			delete: func(created data.Persistable) data.Persistable {
				return &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1"}
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, deleted data.Persistable) {
				verifyEntityNotExists(t, client, "pk1", "sk1")
			},
		},
		{
			name: "delete multi-part key entity",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "p1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			delete: func(created data.Persistable) data.Persistable {
				return &testentities.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1"}
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, deleted data.Persistable) {
				verifyEntityNotExists(t, client, "t1#T1", "i1")
			},
		},
		{
			name: "delete static key entity",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.StaticKeyEntity{Id: "item1", Status: "active", Detail: "detail1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			delete: func(created data.Persistable) data.Persistable {
				return &testentities.StaticKeyEntity{Id: "item1", Status: "active"}
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, deleted data.Persistable) {
				verifyEntityNotExists(t, client, "ITEM#item1", "STATUS#active")
			},
		},
		{
			name: "delete user",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.User{TenantId: "t1", Id: "u1", Email: "test@example.com", Name: "Test"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			delete: func(created data.Persistable) data.Persistable {
				return &testentities.User{TenantId: "t1", Id: "u1"}
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, deleted data.Persistable) {
				// User keys: PK = TenantId#USER, SK = Id
				verifyEntityNotExists(t, client, "t1#USER", "u1")
			},
		},
		{
			name: "delete product",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.Product{TenantId: "t1", Id: "p1", Sku: "SKU1", Category: "Cat1", Name: "Prod1", Price: 10.0}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			delete: func(created data.Persistable) data.Persistable {
				return &testentities.Product{TenantId: "t1", Id: "p1"}
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, deleted data.Persistable) {
				verifyEntityNotExists(t, client, "t1#PRODUCT", "p1")
			},
		},
		{
			name: "delete order",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.Order{TenantId: "t1", OrderId: "o1", UserId: "u1", OrderDate: time.Now(), Status: "pending", Total: 100.0}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			delete: func(created data.Persistable) data.Persistable {
				return &testentities.Order{TenantId: "t1", OrderId: "o1"}
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, deleted data.Persistable) {
				verifyEntityNotExists(t, client, "t1#ORDER", "ID#o1")
			},
		},
		{
			name: "delete with missing sk field",
			setup: func(store data.Store) data.Persistable {
				return nil
			},
			delete: func(created data.Persistable) data.Persistable {
				return &testentities.CompositeKeyEntity{PartitionKey: "pk1", SortKey: ""}
			},
			expectError: true,
		},
		{
			name: "delete with missing pk field",
			setup: func(store data.Store) data.Persistable {
				return nil
			},
			delete: func(created data.Persistable) data.Persistable {
				return &testentities.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"}
			},
			expectError: true,
		},
		{
			name: "delete then create succeeds",
			setup: func(store data.Store) data.Persistable {
				entity := &testentities.CompositeKeyEntity{
					PartitionKey: "recreate-test",
					SortKey:      "sk1",
					Data:         "First",
					Active:       true,
				}
				ge := store.Create(context.Background(), entity)
				assert.Success(t, ge)
				return entity
			},
			delete: func(created data.Persistable) data.Persistable {
				return created
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, deleted data.Persistable) {
				// After delete, create with same ID should succeed
				ctx := context.Background()
				entity := &testentities.CompositeKeyEntity{
					PartitionKey: "recreate-test",
					SortKey:      "sk1",
					Data:         "Second",
					Active:       false,
				}
				ge := store.Create(ctx, entity)
				assert.Success(t, ge)

				// Verify it was created with new data
				readEntity := &testentities.CompositeKeyEntity{
					PartitionKey: "recreate-test",
					SortKey:      "sk1",
				}
				ge = store.Read(ctx, readEntity)
				assert.Success(t, ge)
				assert.Equals(t, "Second", readEntity.Data)
				assert.Equals(t, false, readEntity.Active)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine entity type
			var entityType data.Persistable
			if tt.setup != nil {
				// Create temporary store to get entity type
				tempStore, tempClient := setupCrudStore(t, &testentities.CompositeKeyEntity{}, &testentities.MultiPartKeyEntity{}, &testentities.StaticKeyEntity{}, &testentities.User{}, &testentities.Product{}, &testentities.Order{})
				created := tt.setup(tempStore)
				cleanupCrudTable(t, tempClient)

				if created != nil {
					entityType = created
				} else {
					deleted := tt.delete(nil)
					entityType = deleted
				}
			} else {
				deleted := tt.delete(nil)
				entityType = deleted
			}

			// Create store with appropriate config
			client, isLocal, err := ddbtest.NewClient()
			assert.Success(t, err)
			if !isLocal {
				t.Skip("Skipping test: DDB_LOCAL not set")
			}

			// Create table
			tableDef := &ddbtest.TableDefinition{}
			tableDef.WithTableName(crudTestTableName).
				WithAttributeDefinition("PK", types.ScalarAttributeTypeS).
				WithAttributeDefinition("SK", types.ScalarAttributeTypeS).
				WithKeySchema("PK", types.KeyTypeHash).
				WithKeySchema("SK", types.KeyTypeRange)
			tableDef.Create(client)

			store, ge := ddb.Store(crudTestTableName, &ddb.Configuration{
				DynamoDb:               client,
				MaxResultsDefault:      100,
				MaxResultsMax:          1000,
				ConsistencyDefault:     ddb.Preferred,
				FailDeleteIfNotPresent: tt.failIfAbsent,
			}, entityType)
			assert.Success(t, ge)

			defer cleanupCrudTable(t, client)

			ctx := context.Background()
			created := tt.setup(store)

			deleted := tt.delete(created)
			ge = store.Delete(ctx, deleted)

			if tt.expectError {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
			} else {
				assert.Success(t, ge)
				if tt.verifyFunc != nil {
					tt.verifyFunc(t, store, client, deleted)
				}
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
	tests := []struct {
		name        string
		entity      data.Persistable
		expectError bool
		verifyFunc  func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable)
	}{
		{
			name: "create with '#' in key field",
			entity: &testentities.EscapedValueEntity{
				Id:              "id1",
				FieldWithHash:   "value#with#hash",
				FieldWithDollar: "normal",
				NormalField:     "data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Read back and verify '#' is unescaped
				read := &testentities.EscapedValueEntity{
					Id:              "id1",
					FieldWithHash:   "value#with#hash",
					FieldWithDollar: "normal",
				}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.FieldWithHash != "value#with#hash" {
					t.Errorf("FieldWithHash should preserve '#', got: %s", read.FieldWithHash)
				}
			},
		},
		{
			name: "create with '$' in key field",
			entity: &testentities.EscapedValueEntity{
				Id:              "id2",
				FieldWithHash:   "normal",
				FieldWithDollar: "value$with$dollar",
				NormalField:     "data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Read back and verify '$' is unescaped
				read := &testentities.EscapedValueEntity{
					Id:              "id2",
					FieldWithHash:   "normal",
					FieldWithDollar: "value$with$dollar",
				}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.FieldWithDollar != "value$with$dollar" {
					t.Errorf("FieldWithDollar should preserve '$', got: %s", read.FieldWithDollar)
				}
			},
		},
		{
			name: "create with both '#' and '$'",
			entity: &testentities.EscapedValueEntity{
				Id:              "id3",
				FieldWithHash:   "hash#value",
				FieldWithDollar: "dollar$value",
				NormalField:     "data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Read back and verify both are unescaped
				read := &testentities.EscapedValueEntity{
					Id:              "id3",
					FieldWithHash:   "hash#value",
					FieldWithDollar: "dollar$value",
				}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.FieldWithHash != "hash#value" {
					t.Errorf("FieldWithHash mismatch: got %s", read.FieldWithHash)
				}
				if read.FieldWithDollar != "dollar$value" {
					t.Errorf("FieldWithDollar mismatch: got %s", read.FieldWithDollar)
				}
			},
		},
		{
			name: "create with '#' at start",
			entity: &testentities.EscapedValueEntity{
				Id:              "id4",
				FieldWithHash:   "#start",
				FieldWithDollar: "normal",
				NormalField:     "data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.EscapedValueEntity{
					Id:              "id4",
					FieldWithHash:   "#start",
					FieldWithDollar: "normal",
				}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.FieldWithHash != "#start" {
					t.Errorf("FieldWithHash should start with '#', got: %s", read.FieldWithHash)
				}
			},
		},
		{
			name: "create with '#' at end",
			entity: &testentities.EscapedValueEntity{
				Id:              "id5",
				FieldWithHash:   "end#",
				FieldWithDollar: "normal",
				NormalField:     "data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.EscapedValueEntity{
					Id:              "id5",
					FieldWithHash:   "end#",
					FieldWithDollar: "normal",
				}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.FieldWithHash != "end#" {
					t.Errorf("FieldWithHash should end with '#', got: %s", read.FieldWithHash)
				}
			},
		},
		{
			name: "create with multiple '#'",
			entity: &testentities.EscapedValueEntity{
				Id:              "id6",
				FieldWithHash:   "###",
				FieldWithDollar: "normal",
				NormalField:     "data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.EscapedValueEntity{
					Id:              "id6",
					FieldWithHash:   "###",
					FieldWithDollar: "normal",
				}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.FieldWithHash != "###" {
					t.Errorf("FieldWithHash should be '###', got: %s", read.FieldWithHash)
				}
			},
		},
		{
			name: "create with '##'",
			entity: &testentities.EscapedValueEntity{
				Id:              "id7",
				FieldWithHash:   "double##hash",
				FieldWithDollar: "normal",
				NormalField:     "data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.EscapedValueEntity{
					Id:              "id7",
					FieldWithHash:   "double##hash",
					FieldWithDollar: "normal",
				}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.FieldWithHash != "double##hash" {
					t.Errorf("FieldWithHash should preserve '##', got: %s", read.FieldWithHash)
				}
			},
		},
		{
			name: "create with '$#'",
			entity: &testentities.EscapedValueEntity{
				Id:              "id8",
				FieldWithHash:   "escape$#combo",
				FieldWithDollar: "normal",
				NormalField:     "data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.EscapedValueEntity{
					Id:              "id8",
					FieldWithHash:   "escape$#combo",
					FieldWithDollar: "normal",
				}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.FieldWithHash != "escape$#combo" {
					t.Errorf("FieldWithHash should preserve '$#', got: %s", read.FieldWithHash)
				}
			},
		},
		{
			name: "update to value with '#'",
			entity: &testentities.EscapedValueEntity{
				Id:              "id9",
				FieldWithHash:   "original",
				FieldWithDollar: "original",
				NormalField:     "data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Update to value with '#'
				updated := &testentities.EscapedValueEntity{
					Id:              "id9",
					FieldWithHash:   "new#value",
					FieldWithDollar: "original",
					NormalField:     "data",
				}
				ge := store.Update(context.Background(), updated, nil)
				assert.Success(t, ge)

				// Verify update
				read := &testentities.EscapedValueEntity{
					Id:              "id9",
					FieldWithHash:   "new#value",
					FieldWithDollar: "original",
				}
				ge = store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.FieldWithHash != "new#value" {
					t.Errorf("FieldWithHash should be 'new#value', got: %s", read.FieldWithHash)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, &testentities.EscapedValueEntity{})
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

func TestCRUD_NumericKeys(t *testing.T) {
	tests := []struct {
		name        string
		entity      data.Persistable
		expectError bool
		verifyFunc  func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable)
	}{
		{
			name: "create with numeric PK",
			entity: &testentities.NumericKeyEntity{
				Id:      123,
				Version: 1,
				Data:    "test-data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Verify numeric keys are stored as strings
				read := &testentities.NumericKeyEntity{Id: 123, Version: 1}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.Data != "test-data" {
					t.Errorf("Data mismatch: got %s", read.Data)
				}
			},
		},
		{
			name: "create with numeric SK",
			entity: &testentities.NumericKeyEntity{
				Id:      456,
				Version: 2,
				Data:    "test-data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.NumericKeyEntity{Id: 456, Version: 2}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.Data != "test-data" {
					t.Errorf("Data mismatch: got %s", read.Data)
				}
			},
		},
		{
			name: "create with zero PK",
			entity: &testentities.NumericKeyEntity{
				Id:      0, // Zero PK - treated as not set
				Version: 1,
				Data:    "test-data",
			},
			expectError: true, // Zero PK should fail validation
		},
		// Removed: Invalid test - int with value 0 treated as absent per index.go:388-389
		// To test actual zero value, define Version as *int instead of int
		{
			name: "create with negative SK",
			entity: &testentities.NumericKeyEntity{
				Id:      999,
				Version: -5,
				Data:    "negative-data",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &testentities.NumericKeyEntity{Id: 999, Version: -5}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.Data != "negative-data" {
					t.Errorf("Data mismatch: got %s", read.Data)
				}
			},
		},
		{
			name: "update numeric key field",
			entity: &testentities.NumericKeyEntity{
				Id:      111,
				Version: 1,
				Data:    "original",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Update changes the SK, effectively creating a new item
				updated := &testentities.NumericKeyEntity{
					Id:      111,
					Version: 2, // New SK
					Data:    "updated",
				}
				ge := store.Update(context.Background(), updated, nil)
				assert.Success(t, ge)

				// Old version should still exist
				read1 := &testentities.NumericKeyEntity{Id: 111, Version: 1}
				ge = store.Read(context.Background(), read1)
				assert.Success(t, ge)

				// New version should exist
				read2 := &testentities.NumericKeyEntity{Id: 111, Version: 2}
				ge = store.Read(context.Background(), read2)
				assert.Success(t, ge)
				if read2.Data != "updated" {
					t.Errorf("Updated data mismatch: got %s", read2.Data)
				}
			},
		},
		{
			name: "sort order with numeric keys (lexicographic)",
			entity: &testentities.NumericKeyEntity{
				Id:      1000,
				Version: 3, // Changed from 1 to avoid conflict with verifyFunc versions
				Data:    "v3",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Create multiple versions to test sort order
				versions := []int{10, 2, 20, 1}
				for _, v := range versions {
					e := &testentities.NumericKeyEntity{Id: 1000, Version: v, Data: fmt.Sprintf("v%d", v)}
					ge := store.Create(context.Background(), e)
					assert.Success(t, ge)
				}

				// Sort order in DynamoDB will be lexicographic: "1", "10", "2", "20"
				// This test just verifies we can create and read them
				for _, v := range versions {
					read := &testentities.NumericKeyEntity{Id: 1000, Version: v}
					ge := store.Read(context.Background(), read)
					assert.Success(t, ge)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupCrudStore(t, &testentities.NumericKeyEntity{})
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
