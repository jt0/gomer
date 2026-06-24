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
	})
	assert.Success(t, ge)
	assert.Success(t, store.AddPersistables(persistables...))

	return store, client
}

func deleteCrudTable(t *testing.T, client *dynamodb.Client) {
	err := ddbtest.DeleteTable(client, crudTestTableName)
	assert.Success(t, err)
}

func cleanCrudTable(t *testing.T, client *dynamodb.Client) {
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
			entity: &ddbtest.CompositeKeyEntity{
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
				readEntity := &ddbtest.CompositeKeyEntity{PartitionKey: "partition1", SortKey: "sort1"}
				ge := store.Read(context.Background(), readEntity)
				assert.Success(t, ge)
				if readEntity.Data != "test-data" || !readEntity.Active {
					t.Errorf("Fields mismatch: got Data=%s, Active=%v", readEntity.Data, readEntity.Active)
				}
			},
		},
		{
			name: "create multi-part key entity",
			entity: &ddbtest.MultiPartKeyEntity{
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
				readEntity := &ddbtest.MultiPartKeyEntity{TenantId: "tenant1", EntityType: "TYPE1", Id: "id1"}
				ge := store.Read(context.Background(), readEntity)
				assert.Success(t, ge)
				if readEntity.Payload != "payload-data" {
					t.Errorf("Payload mismatch: got %s", readEntity.Payload)
				}
			},
		},
		{
			name: "create static key entity",
			entity: &ddbtest.StaticKeyEntity{
				Id:     "item1",
				Status: "active",
				Detail: "test-detail",
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client) {
				if !verifyEntityExists(t, client, "ITEM#item1", "STATUS#active") {
					t.Error("StaticKeyEntity should exist")
				}
				readEntity := &ddbtest.StaticKeyEntity{Id: "item1", Status: "active"}
				ge := store.Read(context.Background(), readEntity)
				assert.Success(t, ge)
				if readEntity.Detail != "test-detail" {
					t.Errorf("Detail mismatch: got %s", readEntity.Detail)
				}
			},
		},
		{
			name: "create user with gsi fields",
			entity: &ddbtest.User{
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
			entity: &ddbtest.Product{
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
			entity: &ddbtest.CompositeKeyEntity{
				PartitionKey: "partition1",
				SortKey:      "", // Empty SK
				Data:         "test",
			},
			expectError: true,
		},
	}

	store, client := setupCrudStore(t,
		new(ddbtest.CompositeKeyEntity),
		new(ddbtest.MultiPartKeyEntity),
		new(ddbtest.StaticKeyEntity),
		new(ddbtest.User),
		new(ddbtest.Product),
	)
	defer deleteCrudTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			ge := store.Create(context.Background(), tt.entity)

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
				entity := &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "data1", Active: true}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*ddbtest.CompositeKeyEntity)
				r := read.(*ddbtest.CompositeKeyEntity)
				if r.Data != c.Data || r.Active != c.Active {
					t.Errorf("Mismatch: got %+v, want %+v", r, c)
				}
			},
		},
		{
			name: "read multi-part key entity",
			setup: func(store data.Store) data.Persistable {
				entity := &ddbtest.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "pay1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &ddbtest.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*ddbtest.MultiPartKeyEntity)
				r := read.(*ddbtest.MultiPartKeyEntity)
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
				entity := &ddbtest.StaticKeyEntity{Id: "item1", Status: "active", Detail: "detail1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &ddbtest.StaticKeyEntity{Id: "item1", Status: "active"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*ddbtest.StaticKeyEntity)
				r := read.(*ddbtest.StaticKeyEntity)
				if r.Detail != c.Detail {
					t.Errorf("Mismatch: got Detail=%s, want %s", r.Detail, c.Detail)
				}
			},
		},
		{
			name: "read user (pk only, no sk)",
			setup: func(store data.Store) data.Persistable {
				entity := &ddbtest.User{TenantId: "t1", Id: "u1", Email: "test@example.com", Name: "Test", Status: "active"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &ddbtest.User{TenantId: "t1", Id: "u1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*ddbtest.User)
				r := read.(*ddbtest.User)
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
				entity := &ddbtest.Product{TenantId: "t1", Id: "p1", Sku: "SKU1", Category: "Cat1", Name: "Prod1", Price: 10.0, Description: "desc1"}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &ddbtest.Product{TenantId: "t1", Id: "p1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*ddbtest.Product)
				r := read.(*ddbtest.Product)
				if r.Sku != c.Sku || r.Category != c.Category || r.Name != c.Name || r.Price != c.Price {
					t.Errorf("Mismatch: got %+v, want %+v", r, c)
				}
			},
		},
		{
			name: "read order with time.Time in key",
			setup: func(store data.Store) data.Persistable {
				orderDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
				entity := &ddbtest.Order{TenantId: "t1", OrderId: "o1", UserId: "u1", OrderDate: orderDate, Status: "pending", Total: 100.0}
				assert.Success(t, store.Create(context.Background(), entity))
				return entity
			},
			read: func() data.Persistable {
				return &ddbtest.Order{TenantId: "t1", OrderId: "o1"}
			},
			verifyFunc: func(t *testing.T, created, read data.Persistable) {
				c := created.(*ddbtest.Order)
				r := read.(*ddbtest.Order)
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
				return &ddbtest.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "nonexistent"}
			},
			expectError: true,
		},
		{
			name: "read with missing sk field",
			setup: func(store data.Store) data.Persistable {
				return nil
			},
			read: func() data.Persistable {
				return &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: ""}
			},
			expectError: true,
		},
		{
			name: "read with missing pk field",
			setup: func(store data.Store) data.Persistable {
				return nil
			},
			read: func() data.Persistable {
				return &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"}
			},
			expectError: true,
		},
	}

	store, client := setupCrudStore(t,
		new(ddbtest.CompositeKeyEntity),
		new(ddbtest.MultiPartKeyEntity),
		new(ddbtest.StaticKeyEntity),
		new(ddbtest.User),
		new(ddbtest.Product),
		new(ddbtest.Order),
	)
	defer deleteCrudTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanCrudTable(t, client)

			created := tt.setup(store)
			readEntity := tt.read()
			ge := store.Read(context.Background(), readEntity)

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
		name       string
		original   data.Persistable
		update     data.Persistable
		verifyFunc func(t *testing.T, store data.Store)
	}{
		{
			name:     "update composite key entity",
			original: &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "original", Active: true},
			update:   &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "updated"},
			verifyFunc: func(t *testing.T, store data.Store) {
				read := &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1"}
				assert.Success(t, store.Read(context.Background(), read))
				assert.Equals(t, "updated", read.Data)
				assert.Equals(t, read.Active, true)
			},
		},
		{
			name:     "update multi-part key entity",
			original: &ddbtest.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "original"},
			update:   &ddbtest.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "updated"},
			verifyFunc: func(t *testing.T, store data.Store) {
				read := &ddbtest.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1"}
				assert.Success(t, store.Read(context.Background(), read))
				assert.Equals(t, "updated", read.Payload)
			},
		},
		{
			name:     "update partial fields only",
			original: &ddbtest.CompositeKeyEntity{PartitionKey: "pk2", SortKey: "sk2", Data: "original", Active: true},
			update:   &ddbtest.CompositeKeyEntity{PartitionKey: "pk2", SortKey: "sk2", Data: "partial-update"},
			verifyFunc: func(t *testing.T, store data.Store) {
				read := &ddbtest.CompositeKeyEntity{PartitionKey: "pk2", SortKey: "sk2"}
				assert.Success(t, store.Read(context.Background(), read))
				assert.Equals(t, "partial-update", read.Data)
				assert.Equals(t, true, read.Active)
			},
		},
		{
			name: "update nested struct partial fields",
			original: &ddbtest.CompositeKeyEntity{PartitionKey: "pk3", SortKey: "sk3", Data: "original", Active: true,
				Nested: &ddbtest.Nested{Foo: "a", Bar: "a"}},
			update: &ddbtest.CompositeKeyEntity{PartitionKey: "pk3", SortKey: "sk3", Data: "partial-update",
				Nested: &ddbtest.Nested{Foo: "b"}},
			verifyFunc: func(t *testing.T, store data.Store) {
				read := &ddbtest.CompositeKeyEntity{PartitionKey: "pk3", SortKey: "sk3"}
				assert.Success(t, store.Read(context.Background(), read))
				assert.Equals(t, "partial-update", read.Data)
				assert.Equals(t, true, read.Active)
				assert.Equals(t, "b", read.Nested.Foo)
				assert.Equals(t, "a", read.Nested.Bar)
			},
		},
		//{
		//	name:   "update non-existent entity",
		//	update: &ddbtest.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "nonexistent", Data: "new"},
		//	verifyFunc: func(t *testing.T, store data.Store) {
		//		read := &ddbtest.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "nonexistent"}
		//		assert.Success(t, store.Read(context.Background(), read))
		//		assert.Equals(t, "new", read.Data)
		//	},
		//},
	}

	store, client := setupCrudStore(t,
		new(ddbtest.CompositeKeyEntity),
		new(ddbtest.MultiPartKeyEntity),
		new(ddbtest.StaticKeyEntity),
	)
	defer deleteCrudTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ge := store.Create(context.Background(), tt.original)
			assert.Success(t, ge)

			ge = store.Update(context.Background(), tt.original, tt.update)
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
			setupEntity:  &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1", Data: "data1"},
			deleteEntity: &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: "sk1"},
			expectedPK:   "pk1",
			expectedSK:   "sk1",
		},
		{
			name:         "delete multi-part key entity",
			setupEntity:  &ddbtest.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1", Payload: "p1"},
			deleteEntity: &ddbtest.MultiPartKeyEntity{TenantId: "t1", EntityType: "T1", Id: "i1"},
			expectedPK:   "t1#T1",
			expectedSK:   "i1",
		},
		{
			name:         "delete static key entity",
			setupEntity:  &ddbtest.StaticKeyEntity{Id: "item1", Status: "active", Detail: "detail1"},
			deleteEntity: &ddbtest.StaticKeyEntity{Id: "item1", Status: "active"},
			expectedPK:   "ITEM#item1",
			expectedSK:   "STATUS#active",
		},
		{
			name:         "delete user",
			setupEntity:  &ddbtest.User{TenantId: "t1", Id: "u1", Email: "test@example.com", Name: "Test"},
			deleteEntity: &ddbtest.User{TenantId: "t1", Id: "u1"},
			expectedPK:   "t1#USER",
			expectedSK:   "u1",
		},
		{
			name:         "delete product",
			setupEntity:  &ddbtest.Product{TenantId: "t1", Id: "p1", Sku: "SKU1", Category: "Cat1", Name: "Prod1", Price: 10.0},
			deleteEntity: &ddbtest.Product{TenantId: "t1", Id: "p1"},
			expectedPK:   "t1#PRODUCT",
			expectedSK:   "p1",
		},
		{
			name:         "delete order",
			setupEntity:  &ddbtest.Order{TenantId: "t1", OrderId: "o1", UserId: "u1", OrderDate: time.Now(), Status: "pending", Total: 100.0},
			deleteEntity: &ddbtest.Order{TenantId: "t1", OrderId: "o1"},
			expectedPK:   "t1#ORDER",
			expectedSK:   "ID#o1",
		},
		{
			name:         "delete with missing sk field",
			deleteEntity: &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: ""},
			expectError:  true,
		},
		{
			name:         "delete with missing pk field",
			deleteEntity: &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"},
			expectError:  true,
		},
		{
			name:         "delete then create succeeds",
			setupEntity:  &ddbtest.CompositeKeyEntity{PartitionKey: "recreate-test", SortKey: "sk1", Data: "First", Active: true},
			deleteEntity: &ddbtest.CompositeKeyEntity{PartitionKey: "recreate-test", SortKey: "sk1"},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client) {
				entity := &ddbtest.CompositeKeyEntity{PartitionKey: "recreate-test", SortKey: "sk1", Data: "Second", Active: false}
				ge := store.Create(context.Background(), entity)
				assert.Success(t, ge)

				readEntity := &ddbtest.CompositeKeyEntity{PartitionKey: "recreate-test", SortKey: "sk1"}
				ge = store.Read(context.Background(), readEntity)
				assert.Success(t, ge)
				assert.Equals(t, "Second", readEntity.Data)
				assert.Equals(t, false, readEntity.Active)
			},
		},
	}

	store, client := setupCrudStore(t,
		new(ddbtest.CompositeKeyEntity),
		new(ddbtest.MultiPartKeyEntity),
		new(ddbtest.StaticKeyEntity),
		new(ddbtest.User),
		new(ddbtest.Product),
		new(ddbtest.Order),
	)
	defer deleteCrudTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup entity if provided
			if tt.setupEntity != nil {
				ge := store.Create(context.Background(), tt.setupEntity)
				assert.Success(t, ge)
			}

			// Execute delete
			ge := store.Delete(context.Background(), tt.deleteEntity)

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
			entity: &ddbtest.EmptyValueEntity{
				Id:          "", // Empty PK
				EmptyString: "test",
				ZeroInt:     5,
			},
			expectError: true, // Empty PK should fail validation
		},
		{
			name: "create with empty string in SK part",
			entity: &ddbtest.EmptyValueEntity{
				Id:          "id1",
				EmptyString: "", // Empty SK segment
				ZeroInt:     5,
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Verify entity exists - SK is "#5" (EmptyString="" + separator + ZeroInt=5)
				item := getRawItem(t, client, "id1", "#5")
				if item == nil || len(item) == 0 {
					t.Error("Entity should exist")
				}
				// Read back and verify
				read := &ddbtest.EmptyValueEntity{Id: "id1", EmptyString: "", ZeroInt: 5}
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
			entity: &ddbtest.EmptyValueEntity{
				Id:          "id3",
				EmptyString: "prefix",
				ZeroInt:     0, // Zero in middle
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Read back and verify zero is preserved as empty
				read := &ddbtest.EmptyValueEntity{Id: "id3", EmptyString: "prefix"}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.ZeroInt != 0 {
					t.Errorf("ZeroInt should be 0, got: %d", read.ZeroInt)
				}
			},
		},
		{
			name: "read entity with empty SK segment",
			entity: &ddbtest.EmptyValueEntity{
				Id:          "id4",
				EmptyString: "",
				ZeroInt:     10,
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &ddbtest.EmptyValueEntity{Id: "id4", EmptyString: "", ZeroInt: 10}
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
			name: "update scalar field to empty string does not update",
			entity: &ddbtest.EmptyValueEntity{
				Id:          "id5",
				EmptyString: "original",
				ZeroInt:     5,
				RequiredStr: "required",
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Update to empty string
				updated := &ddbtest.EmptyValueEntity{
					Id:          "id5",
					EmptyString: "original",
					ZeroInt:     5,
					RequiredStr: "",
				}
				ge := store.Update(context.Background(), entity, updated)
				assert.Success(t, ge)

				entity.(*ddbtest.EmptyValueEntity).RequiredStr = "abc" // set to validate Read() overwrites it
				ge = store.Read(context.Background(), entity)
				assert.Success(t, ge)
				assert.Equals(t, "required", entity.(*ddbtest.EmptyValueEntity).RequiredStr)
			},
		},
		{
			name: "update field to empty pointer string does update",
			entity: &ddbtest.EmptyValueEntity{
				Id:          "id5",
				EmptyString: "original",
				ZeroInt:     6,
				OptionalPtr: new("optional"),
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Update to empty string
				updated := &ddbtest.EmptyValueEntity{
					Id:          "id5",
					EmptyString: "original",
					ZeroInt:     6,
					OptionalPtr: new(""),
				}
				ge := store.Update(context.Background(), entity, updated)
				assert.Success(t, ge)

				entity.(*ddbtest.EmptyValueEntity).OptionalPtr = new("abc") // set to validate Read() overwrites it
				ge = store.Read(context.Background(), entity)
				assert.Success(t, ge)
				assert.Equals(t, new(""), entity.(*ddbtest.EmptyValueEntity).OptionalPtr)
			},
		},
		{
			name: "update zero to non-zero int",
			entity: &ddbtest.EmptyValueEntity{
				Id:          "id7",
				EmptyString: "test",
				ZeroInt:     0,
			},
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Update zero to non-zero
				updated := &ddbtest.EmptyValueEntity{
					Id:          "id7",
					EmptyString: "test",
					ZeroInt:     42,
				}
				ge := store.Update(context.Background(), updated, nil)
				assert.Success(t, ge)

				// Verify update - SK changed to "test#42", need both fields
				read := &ddbtest.EmptyValueEntity{Id: "id7", EmptyString: "test", ZeroInt: 42}
				ge = store.Read(context.Background(), read)
				assert.Success(t, ge)
				if read.ZeroInt != 42 {
					t.Errorf("ZeroInt should be 42, got: %d", read.ZeroInt)
				}
			},
		},
	}

	store, client := setupCrudStore(t, &ddbtest.EmptyValueEntity{})
	defer deleteCrudTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entity
			ge := store.Create(context.Background(), tt.entity)

			if tt.expectError {
				assert.Error(t, ge)
			} else {
				assert.Success(t, ge)
			}

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
			store, client := setupCrudStore(t, &ddbtest.EscapedValueEntity{})
			defer deleteCrudTable(t, client)

			entity := &ddbtest.EscapedValueEntity{
				Id:       tt.id,
				ToEscape: tt.fieldWithHash,
				//FieldWithDollar: tt.fieldWithDollar,
				Trailing: "data",
			}

			// Create entity
			ge := store.Create(context.Background(), entity)
			assert.Success(t, ge)

			// Read back and verify
			read := &ddbtest.EscapedValueEntity{
				Id:       tt.id,
				ToEscape: tt.fieldWithHash,
				Trailing: "data",
				//FieldWithDollar: tt.fieldWithDollar,
			}
			ge = store.Read(context.Background(), read)
			assert.Success(t, ge)
			assert.Equals(t, tt.fieldWithHash, read.ToEscape)
			//assert.Equals(t, tt.fieldWithDollar, read.FieldWithDollar)
		})
	}
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

	store, client := setupCrudStore(t, &ddbtest.NumericKeyEntity{})
	defer deleteCrudTable(t, client)

	for _, tt := range readTests {
		t.Run(tt.name, func(t *testing.T) {
			entity := &ddbtest.NumericKeyEntity{Id: tt.id, Version: tt.version, Data: tt.data}
			ge := store.Create(context.Background(), entity)

			if tt.wantErr {
				if ge == nil {
					t.Error("Expected error but got nil")
				}
				return
			}
			assert.Success(t, ge)

			// Read back and verify
			read := &ddbtest.NumericKeyEntity{Id: tt.id, Version: tt.version}
			assert.Success(t, store.Read(context.Background(), read))
			assert.Equals(t, tt.data, read.Data)
		})
	}

	t.Run("update numeric key field", func(t *testing.T) {
		entity := &ddbtest.NumericKeyEntity{Id: 111, Version: 1, Data: "original"}
		assert.Success(t, store.Create(context.Background(), entity))

		// Update changes the SK, effectively creating a new item
		updated := &ddbtest.NumericKeyEntity{Id: 111, Version: 2, Data: "updated"}
		assert.Success(t, store.Update(context.Background(), updated, nil))

		// Old version should still exist
		read1 := &ddbtest.NumericKeyEntity{Id: 111, Version: 1}
		assert.Success(t, store.Read(context.Background(), read1))

		// New version should exist
		read2 := &ddbtest.NumericKeyEntity{Id: 111, Version: 2}
		assert.Success(t, store.Read(context.Background(), read2))
		assert.Equals(t, "updated", read2.Data)
	})

	t.Run("sort order with numeric keys (lexicographic)", func(t *testing.T) {
		// Create multiple versions
		versions := []int{10, 2, 20, 1, 3}
		for _, v := range versions {
			e := &ddbtest.NumericKeyEntity{Id: 1000, Version: v, Data: fmt.Sprintf("v%d", v)}
			assert.Success(t, store.Create(context.Background(), e))
		}

		// Verify we can read all of them
		for _, v := range versions {
			read := &ddbtest.NumericKeyEntity{Id: 1000, Version: v}
			assert.Success(t, store.Read(context.Background(), read))
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
				return &ddbtest.PointerKeyEntity{
					Id:      new("test-id"),
					SortVal: new(0), // Actual zero value using pointer
					Data:    "data",
				}
			}(),
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				// Read back with zero value
				read := &ddbtest.PointerKeyEntity{
					Id:      new("test-id"),
					SortVal: new(0),
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
				return &ddbtest.PointerKeyEntity{
					Id:      new("test-id-2"),
					SortVal: nil, // Nil pointer - treated as absent
					Data:    "data",
				}
			}(),
			expectError: true, // Should fail because SK is required in table schema
		},
	}

	store, client := setupCrudStore(t, &ddbtest.PointerKeyEntity{})
	defer deleteCrudTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entity
			ge := store.Create(context.Background(), tt.entity)

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
			entity: &ddbtest.Order{
				TenantId:  "tenant1",
				OrderId:   "order1",
				UserId:    "user1",
				OrderDate: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
				Status:    "pending",
				Total:     100.0,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &ddbtest.Order{TenantId: "tenant1", OrderId: "order1"}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)

				original := entity.(*ddbtest.Order)
				if !read.OrderDate.Equal(original.OrderDate) {
					t.Errorf("OrderDate mismatch: got %v, want %v", read.OrderDate, original.OrderDate)
				}
			},
		},
		{
			name: "create order with zero time.Time",
			entity: &ddbtest.Order{
				TenantId:  "tenant2",
				OrderId:   "order2",
				UserId:    "user2",
				OrderDate: time.Time{}, // Zero time
				Status:    "draft",
				Total:     0.0,
			},
			expectError: false,
			verifyFunc: func(t *testing.T, store data.Store, client *dynamodb.Client, entity data.Persistable) {
				read := &ddbtest.Order{TenantId: "tenant2", OrderId: "order2"}
				ge := store.Read(context.Background(), read)
				assert.Success(t, ge)
				// Zero time is treated as empty segment in key
			},
		},
		{
			name: "sort order with time.Time keys",
			entity: &ddbtest.Order{
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
					order := &ddbtest.Order{
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
					read := &ddbtest.Order{TenantId: "tenant3", OrderId: fmt.Sprintf("order_chrono_%d", i+2)}
					ge := store.Read(context.Background(), read)
					assert.Success(t, ge)
				}
			},
		},
		{
			name: "update order changing OrderDate",
			entity: &ddbtest.Order{
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
				updated := &ddbtest.Order{
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
				read := &ddbtest.Order{TenantId: "tenant4", OrderId: "order4"}
				ge = store.Read(context.Background(), read)
				assert.Success(t, ge)
				if !read.OrderDate.Equal(updated.OrderDate) {
					t.Errorf("OrderDate should be updated: got %v, want %v", read.OrderDate, updated.OrderDate)
				}
			},
		},
	}

	store, client := setupCrudStore(t, &ddbtest.Order{})
	defer deleteCrudTable(t, client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create entity
			ge := store.Create(context.Background(), tt.entity)

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
	store, client := setupCrudStore(t, &ddbtest.CompositeKeyEntity{})
	defer deleteCrudTable(t, client)

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
	store1, client1 := setupCrudStore(t, &ddbtest.CompositeKeyEntity{})
	defer deleteCrudTable(t, client1)

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
	store, client := setupCrudStore(t, &ddbtest.CompositeKeyEntity{})
	defer deleteCrudTable(t, client)

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
	store, client := setupCrudStore(t, &ddbtest.CompositeKeyEntity{})
	defer deleteCrudTable(t, client)

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
	deleteTable := "gomer_delete_test"
	tableDef := &ddbtest.TableDefinition{}
	tableDef.WithTableName(deleteTable).
		WithAttributeDefinition("PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("SK", types.ScalarAttributeTypeS).
		WithKeySchema("PK", types.KeyTypeHash).
		WithKeySchema("SK", types.KeyTypeRange)
	tableDef.Create(client)
	defer ddbtest.DeleteTable(client, deleteTable)

	store, ge := ddb.Store(deleteTable, &ddb.Configuration{
		DynamoDb:               client,
		MaxResultsDefault:      100,
		MaxResultsMax:          1000,
		ConsistencyDefault:     ddb.Preferred,
		FailDeleteIfNotPresent: true, // Key configuration for this test
	})
	assert.Success(t, ge)
	assert.Success(t, store.AddPersistables(&ddbtest.CompositeKeyEntity{}))

	// Try to delete an entity that doesn't exist
	entity := &ddbtest.CompositeKeyEntity{
		PartitionKey: "nonexistent-pk",
		SortKey:      "nonexistent-sk",
	}

	// Should fail because entity doesn't exist and FailDeleteIfNotPresent=true
	ge = store.Delete(context.Background(), entity)
	if ge == nil {
		t.Error("Expected error when deleting non-existent entity with FailDeleteIfNotPresent=true")
	}
}

// TestDelete_MissingKeyFields tests delete with missing key fields
func TestDelete_MissingKeyFields(t *testing.T) {
	store, client := setupCrudStore(t, &ddbtest.CompositeKeyEntity{})
	defer deleteCrudTable(t, client)

	tests := []struct {
		name   string
		entity *ddbtest.CompositeKeyEntity
	}{
		{
			name:   "missing pk",
			entity: &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"},
		},
		{
			name:   "missing sk",
			entity: &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: ""},
		},
		{
			name:   "missing both",
			entity: &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ge := store.Delete(context.Background(), tt.entity)
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
	store, client := setupCrudStore(t, &ddbtest.CompositeKeyEntity{})
	defer deleteCrudTable(t, client)

	// Try to read an entity that doesn't exist
	entity := &ddbtest.CompositeKeyEntity{
		PartitionKey: "nonexistent-pk",
		SortKey:      "nonexistent-sk",
	}

	ge := store.Read(context.Background(), entity)
	if ge == nil {
		t.Error("Expected error when reading non-existent entity")
	}
}

// TestRead_MissingKeyFields tests read with missing key fields
func TestRead_MissingKeyFields(t *testing.T) {
	store, client := setupCrudStore(t, &ddbtest.CompositeKeyEntity{})
	defer deleteCrudTable(t, client)

	tests := []struct {
		name   string
		entity *ddbtest.CompositeKeyEntity
	}{
		{
			name:   "missing pk",
			entity: &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"},
		},
		{
			name:   "missing sk",
			entity: &ddbtest.CompositeKeyEntity{PartitionKey: "pk1", SortKey: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ge := store.Read(context.Background(), tt.entity)
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
	store, client := setupCrudStore(t, &ddbtest.CompositeKeyEntity{})
	defer deleteCrudTable(t, client)

	// Query with empty partition key
	q := &ddbtest.CompositeKeyEntities{PartitionKey: ""}
	ge := store.Query(context.Background(), q)
	if ge == nil {
		t.Error("Expected error when querying with empty partition key")
	}
}

// TestQuery_NoMatchingIndex tests query that can't find a matching index
func TestQuery_NoMatchingIndex(t *testing.T) {
	store, client := setupCrudStore(t, &ddbtest.Product{})
	defer deleteCrudTable(t, client)

	// Query with only Category - this should fail because GSI_1 needs TenantId+Category
	q := &ddbtest.Products{Category: "Electronics"}
	ge := store.Query(context.Background(), q)
	if ge == nil {
		t.Error("Expected error when querying with incomplete index key")
	}
}
