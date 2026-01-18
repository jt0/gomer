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

const queryTestTableName = "gomer_query_test"

// Setup/teardown helpers

func setupQueryStore(t *testing.T, persistables ...data.Persistable) (data.Store, *dynamodb.Client) {
	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)

	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	// Define table with PK, SK, and common GSI/LSI indexes
	tableDef := &ddbtest.TableDefinition{}
	tableDef.WithTableName(queryTestTableName).
		WithAttributeDefinition("PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("SK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("GSI_1_PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("GSI_1_SK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("LSI_1_SK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("LSI_2_SK", types.ScalarAttributeTypeS).
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

	tableDef.Create(client)

	store, ge := ddb.Store(queryTestTableName, &ddb.Configuration{
		DynamoDb:                    client,
		MaxResultsDefault:           100,
		MaxResultsMax:               1000,
		ConsistencyDefault:          ddb.Preferred,
		QueryWildcardChar:           '*',
		FailDeleteIfNotPresent:      false,
		ValidateKeyFieldConsistency: false,
	}, persistables...)
	assert.Success(t, ge)

	return store, client
}

func cleanupQueryTable(t *testing.T, client *dynamodb.Client) {
	err := ddbtest.DeleteAllTableData(client, queryTestTableName)
	assert.Success(t, err)
}

// Test data population helper
func populateTestData(t *testing.T, store data.Store) {
	ctx := context.Background()

	// Create CompositeKeyEntity items
	for i := 1; i <= 5; i++ {
		entity := &testentities.CompositeKeyEntity{
			PartitionKey: "T1",
			SortKey:      fmt.Sprintf("item%d", i),
			Data:         fmt.Sprintf("data%d", i),
			Active:       i%2 == 1, // odd numbers are active
		}
		assert.Success(t, store.Create(ctx, entity))
	}

	// Create CompositeKeyEntity items for T2 partition
	for i := 1; i <= 3; i++ {
		entity := &testentities.CompositeKeyEntity{
			PartitionKey: "T2",
			SortKey:      fmt.Sprintf("item%d", i),
			Data:         "test",
			Active:       true,
		}
		assert.Success(t, store.Create(ctx, entity))
	}

	// Create MultiPartKeyEntity items
	for _, entityType := range []string{"USER", "ADMIN"} {
		for i := 1; i <= 3; i++ {
			entity := &testentities.MultiPartKeyEntity{
				TenantId:   "T1",
				EntityType: entityType,
				Id:         fmt.Sprintf("id%d", i),
				Payload:    fmt.Sprintf("payload-%s-%d", entityType, i),
			}
			assert.Success(t, store.Create(ctx, entity))
		}
	}

	// Create StaticKeyEntity items
	for _, status := range []string{"active", "inactive", "pending"} {
		entity := &testentities.StaticKeyEntity{
			Id:     "item123",
			Status: status,
			Detail: fmt.Sprintf("detail-%s", status),
		}
		assert.Success(t, store.Create(ctx, entity))
	}

	// Create User items
	for i := 1; i <= 5; i++ {
		user := &testentities.User{
			TenantId: "T1",
			Id:       fmt.Sprintf("user%d", i),
			Email:    fmt.Sprintf("user%d@example.com", i),
			Name:     fmt.Sprintf("User %d", i),
			Status:   "active",
		}
		assert.Success(t, store.Create(ctx, user))
	}

	// Create Product items
	categories := []string{"Electronics", "Books", "Clothing"}
	for i := 1; i <= 6; i++ {
		product := &testentities.Product{
			TenantId:    "T1",
			Id:          fmt.Sprintf("prod%d", i),
			Sku:         fmt.Sprintf("SKU%03d", i),
			Category:    categories[(i-1)%3],
			Name:        fmt.Sprintf("Product %d", i),
			Price:       float64(i) * 10.0,
			Description: fmt.Sprintf("Description for product %d", i),
		}
		assert.Success(t, store.Create(ctx, product))
	}

	// Create Order items
	baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 5; i++ {
		order := &testentities.Order{
			TenantId:  "T1",
			OrderId:   fmt.Sprintf("order%d", i),
			UserId:    fmt.Sprintf("user%d", (i-1)%3+1), // user1, user2, user3, user1, user2
			OrderDate: baseDate.Add(time.Duration(i) * 24 * time.Hour),
			Status:    []string{"pending", "shipped", "delivered"}[(i-1)%3],
			Total:     float64(i) * 100.0,
		}
		assert.Success(t, store.Create(ctx, order))
	}
}

// Helper functions for verification

func extractSortKeys(q data.Queryable) []string {
	items := q.Items()
	keys := make([]string, len(items))
	for i, item := range items {
		switch v := item.(type) {
		case *testentities.CompositeKeyEntity:
			keys[i] = v.SortKey
		case *testentities.MultiPartKeyEntity:
			keys[i] = v.Id
		case *testentities.StaticKeyEntity:
			keys[i] = v.Status
		default:
			keys[i] = fmt.Sprintf("%v", item)
		}
	}
	return keys
}

func extractIds(q data.Queryable) []string {
	items := q.Items()
	ids := make([]string, len(items))
	for i, item := range items {
		switch v := item.(type) {
		case *testentities.User:
			ids[i] = v.Id
		case *testentities.Product:
			ids[i] = v.Id
		case *testentities.Order:
			ids[i] = v.OrderId
		default:
			ids[i] = fmt.Sprintf("%v", item)
		}
	}
	return ids
}

func uniqueIds(ids []string) []string {
	seen := make(map[string]bool)
	result := []string{}
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
}

// Section 1: Basic Query Operations Tests

func TestQuery_BasicQueries(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func(store data.Store)
		queryFunc     func() data.Queryable
		expectedCount int
		verifyFunc    func(t *testing.T, q data.Queryable)
	}{
		{
			name: "query by pk only",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{PartitionKey: "T1"}
			},
			expectedCount: 5,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				for _, item := range items {
					entity := item.(*testentities.CompositeKeyEntity)
					if entity.PartitionKey != "T1" {
						t.Errorf("Expected PartitionKey=T1, got %s", entity.PartitionKey)
					}
				}
			},
		},
		{
			name: "query by pk and exact sk",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					SortKey:      "item1",
				}
			},
			expectedCount: 1,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				if len(items) != 1 {
					t.Fatalf("Expected 1 item, got %d", len(items))
				}
				entity := items[0].(*testentities.CompositeKeyEntity)
				if entity.SortKey != "item1" {
					t.Errorf("Expected SortKey=item1, got %s", entity.SortKey)
				}
			},
		},
		{
			name: "query by multi-part pk",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.MultiPartKeyEntities{
					TenantId:   "T1",
					EntityType: "USER",
				}
			},
			expectedCount: 3,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				for _, item := range items {
					entity := item.(*testentities.MultiPartKeyEntity)
					if entity.TenantId != "T1" || entity.EntityType != "USER" {
						t.Errorf("Expected TenantId=T1, EntityType=USER, got %s, %s", entity.TenantId, entity.EntityType)
					}
				}
			},
		},
		{
			name: "query with static prefix",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.StaticKeyEntities{Id: "item123"}
			},
			expectedCount: 3,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				for _, item := range items {
					entity := item.(*testentities.StaticKeyEntity)
					if entity.Id != "item123" {
						t.Errorf("Expected Id=item123, got %s", entity.Id)
					}
				}
			},
		},
		{
			name: "query user by tenant",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.Users{TenantId: "T1"}
			},
			expectedCount: 5,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				for _, item := range items {
					user := item.(*testentities.User)
					if user.TenantId != "T1" {
						t.Errorf("Expected TenantId=T1, got %s", user.TenantId)
					}
				}
			},
		},
		{
			name: "query user by email",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.Users{Email: "user1@example.com"}
			},
			expectedCount: 1,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				if len(items) != 1 {
					t.Fatalf("Expected 1 item, got %d", len(items))
				}
				user := items[0].(*testentities.User)
				if user.Email != "user1@example.com" {
					t.Errorf("Expected Email=user1@example.com, got %s", user.Email)
				}
			},
		},
		{
			name: "query product by tenant",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.Products{TenantId: "T1"}
			},
			expectedCount: 6,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				for _, item := range items {
					product := item.(*testentities.Product)
					if product.TenantId != "T1" {
						t.Errorf("Expected TenantId=T1, got %s", product.TenantId)
					}
				}
			},
		},
		{
			name: "query product by category",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.Products{
					TenantId: "T1",
					Category: "Electronics",
				}
			},
			expectedCount: 2,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				for _, item := range items {
					product := item.(*testentities.Product)
					if product.Category != "Electronics" {
						t.Errorf("Expected Category=Electronics, got %s", product.Category)
					}
				}
			},
		},
		{
			name: "query order by tenant",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.Orders{TenantId: "T1"}
			},
			expectedCount: 5,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				for _, item := range items {
					order := item.(*testentities.Order)
					if order.TenantId != "T1" {
						t.Errorf("Expected TenantId=T1, got %s", order.TenantId)
					}
				}
			},
		},
		{
			name: "query with no results",
			setupFunc: func(store data.Store) {
				populateTestData(t, store)
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{PartitionKey: "NONEXISTENT"}
			},
			expectedCount: 0,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				if len(items) != 0 {
					t.Errorf("Expected empty results, got %d items", len(items))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup store - register all entity types that populateTestData uses
			store, client := setupQueryStore(t,
				&testentities.CompositeKeyEntity{},
				&testentities.MultiPartKeyEntity{},
				&testentities.StaticKeyEntity{},
				&testentities.User{},
				&testentities.Product{},
				&testentities.Order{},
			)
			defer cleanupQueryTable(t, client)

			// Setup test data
			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			// Execute query
			q := tt.queryFunc()
			err := store.Query(context.Background(), q)
			assert.Success(t, err)

			// Verify count
			items := q.Items()
			if len(items) != tt.expectedCount {
				t.Errorf("Expected %d items, got %d", tt.expectedCount, len(items))
			}

			// Additional verification
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, q)
			}
		})
	}
}

// Section 2: Sort Key Condition Tests

func TestQuery_SortKeyConditions(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func(store data.Store)
		queryFunc     func() data.Queryable
		expectedCount int
		verifyFunc    func(t *testing.T, q data.Queryable)
	}{
		{
			name: "exact sk match",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 5; i++ {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      fmt.Sprintf("item%d", i),
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					SortKey:      "item1",
				}
			},
			expectedCount: 1,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				entity := q.Items()[0].(*testentities.CompositeKeyEntity)
				if entity.SortKey != "item1" {
					t.Errorf("Expected SortKey=item1, got %s", entity.SortKey)
				}
			},
		},
		{
			name: "sk begins_with via wildcard",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				items := []string{"item1", "item123", "item_a", "other1"}
				for _, sk := range items {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      sk,
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					SortKey:      "item*",
				}
			},
			expectedCount: 3, // item1, item123, item_a (not other1)
			verifyFunc: func(t *testing.T, q data.Queryable) {
				keys := extractSortKeys(q)
				expected := map[string]bool{"item1": true, "item123": true, "item_a": true}
				for _, key := range keys {
					if !expected[key] {
						t.Errorf("Unexpected key %s in results", key)
					}
				}
			},
		},
		{
			name: "sk begins_with via trailing separator",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				// Create items with composite SK
				for _, entityType := range []string{"USER", "ADMIN"} {
					for i := 1; i <= 3; i++ {
						entity := &testentities.MultiPartKeyEntity{
							TenantId:   "T1",
							EntityType: entityType,
							Id:         fmt.Sprintf("id%d", i),
							Payload:    "test",
						}
						assert.Success(t, store.Create(ctx, entity))
					}
				}
			},
			queryFunc: func() data.Queryable {
				// Query with TenantId and EntityType, but no Id
				// This should use begins_with due to trailing separator
				return &testentities.MultiPartKeyEntities{
					TenantId:   "T1",
					EntityType: "USER",
				}
			},
			expectedCount: 3, // All USER items
			verifyFunc: func(t *testing.T, q data.Queryable) {
				for _, item := range q.Items() {
					entity := item.(*testentities.MultiPartKeyEntity)
					if entity.EntityType != "USER" {
						t.Errorf("Expected EntityType=USER, got %s", entity.EntityType)
					}
				}
			},
		},
		{
			name: "no sk provided",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 5; i++ {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      fmt.Sprintf("item%d", i),
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					// No SortKey provided
				}
			},
			expectedCount: 5, // All items in partition
			verifyFunc: func(t *testing.T, q data.Queryable) {
				if len(q.Items()) != 5 {
					t.Errorf("Expected 5 items, got %d", len(q.Items()))
				}
			},
		},
		{
			name: "zero sk value",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 3; i++ {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      fmt.Sprintf("item%d", i),
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					SortKey:      "", // Empty SK
				}
			},
			expectedCount: 3, // All items (empty SK treated as not provided)
			verifyFunc: func(t *testing.T, q data.Queryable) {
				if len(q.Items()) != 3 {
					t.Errorf("Expected 3 items, got %d", len(q.Items()))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine persistable from query type
			q := tt.queryFunc()
			var persistable data.Persistable
			switch q.(type) {
			case *testentities.CompositeKeyEntities:
				persistable = &testentities.CompositeKeyEntity{}
			case *testentities.MultiPartKeyEntities:
				persistable = &testentities.MultiPartKeyEntity{}
			}

			store, client := setupQueryStore(t, persistable)
			defer cleanupQueryTable(t, client)

			// Setup test data
			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			// Execute query
			q = tt.queryFunc()
			err := store.Query(context.Background(), q)
			assert.Success(t, err)

			// Verify count
			items := q.Items()
			if len(items) != tt.expectedCount {
				t.Errorf("Expected %d items, got %d", tt.expectedCount, len(items))
			}

			// Additional verification
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, q)
			}
		})
	}
}

// Section 3: Index Selection Algorithm Tests

func TestQuery_IndexSelection(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func(store data.Store)
		queryFunc     func() data.Queryable
		expectError   bool
		expectedCount int
		verifyFunc    func(t *testing.T, q data.Queryable)
	}{
		{
			name: "single viable index - gsi",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				user := &testentities.User{
					TenantId: "T1",
					Id:       "user1",
					Email:    "user1@example.com",
					Name:     "Test User",
					Status:   "active",
				}
				assert.Success(t, store.Create(ctx, user))
			},
			queryFunc: func() data.Queryable {
				return &testentities.Users{Email: "user1@example.com"}
			},
			expectedCount: 1,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				user := q.Items()[0].(*testentities.User)
				if user.Email != "user1@example.com" {
					t.Errorf("Expected Email=user1@example.com, got %s", user.Email)
				}
			},
		},
		{
			name: "base table vs gsi",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 3; i++ {
					user := &testentities.User{
						TenantId: "T1",
						Id:       fmt.Sprintf("user%d", i),
						Email:    fmt.Sprintf("user%d@example.com", i),
						Name:     fmt.Sprintf("User %d", i),
						Status:   "active",
					}
					assert.Success(t, store.Create(ctx, user))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.Users{TenantId: "T1"}
			},
			expectedCount: 3,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				for _, item := range q.Items() {
					user := item.(*testentities.User)
					if user.TenantId != "T1" {
						t.Errorf("Expected TenantId=T1, got %s", user.TenantId)
					}
				}
			},
		},
		{
			name: "incomplete pk fails",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				product := &testentities.Product{
					TenantId: "T1",
					Id:       "prod1",
					Category: "Electronics",
					Name:     "Test",
					Price:    99.99,
				}
				assert.Success(t, store.Create(ctx, product))
			},
			queryFunc: func() data.Queryable {
				// Query with only Category (incomplete GSI_1 PK which needs TenantId+Category)
				return &testentities.Products{Category: "Electronics"}
			},
			expectError: true,
		},
		{
			name: "lsi vs base table",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 3; i++ {
					product := &testentities.Product{
						TenantId: "T1",
						Id:       fmt.Sprintf("prod%d", i),
						Sku:      fmt.Sprintf("SKU%03d", i),
						Category: "Electronics",
						Name:     fmt.Sprintf("Product %d", i),
						Price:    float64(i) * 10.0,
					}
					assert.Success(t, store.Create(ctx, product))
				}
			},
			queryFunc: func() data.Queryable {
				// Query by TenantId+Sku should use LSI_1
				return &testentities.Products{
					TenantId: "T1",
					Sku:      "SKU001",
				}
			},
			expectedCount: 1,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				product := q.Items()[0].(*testentities.Product)
				if product.Sku != "SKU001" {
					t.Errorf("Expected Sku=SKU001, got %s", product.Sku)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine persistable from query type
			q := tt.queryFunc()
			var persistable data.Persistable
			switch q.(type) {
			case *testentities.Users:
				persistable = &testentities.User{}
			case *testentities.Products:
				persistable = &testentities.Product{}
			}

			store, client := setupQueryStore(t, persistable)
			defer cleanupQueryTable(t, client)

			// Setup test data
			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			// Execute query
			q = tt.queryFunc()
			err := store.Query(context.Background(), q)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			assert.Success(t, err)

			// Verify count
			items := q.Items()
			if len(items) != tt.expectedCount {
				t.Errorf("Expected %d items, got %d", tt.expectedCount, len(items))
			}

			// Additional verification
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, q)
			}
		})
	}
}

// Section 4: Wildcard Query Tests

func TestQuery_WildcardMatching(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func(store data.Store)
		queryFunc     func() data.Queryable
		expectedCount int
		verifyFunc    func(t *testing.T, q data.Queryable)
	}{
		{
			name: "simple prefix wildcard",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				items := []string{"item1", "item123", "item_a", "other1", "itemx"}
				for _, sk := range items {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      sk,
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					SortKey:      "item*",
				}
			},
			expectedCount: 4, // item1, item123, item_a, itemx
			verifyFunc: func(t *testing.T, q data.Queryable) {
				keys := extractSortKeys(q)
				for _, key := range keys {
					if len(key) < 4 || key[:4] != "item" {
						t.Errorf("Key %s doesn't start with 'item'", key)
					}
				}
			},
		},
		{
			name: "no wildcard exact match",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				items := []string{"item", "item1", "items"}
				for _, sk := range items {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      sk,
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					SortKey:      "item",
				}
			},
			expectedCount: 1,
			verifyFunc: func(t *testing.T, q data.Queryable) {
				entity := q.Items()[0].(*testentities.CompositeKeyEntity)
				if entity.SortKey != "item" {
					t.Errorf("Expected exact match 'item', got %s", entity.SortKey)
				}
			},
		},
		{
			name: "empty string wildcard",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				items := []string{"a", "b", "c", "item1"}
				for _, sk := range items {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      sk,
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					SortKey:      "*",
				}
			},
			expectedCount: 4, // All items
			verifyFunc: func(t *testing.T, q data.Queryable) {
				if len(q.Items()) != 4 {
					t.Errorf("Expected all 4 items, got %d", len(q.Items()))
				}
			},
		},
		{
			name: "wildcard with gsi composite sk",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				products := []struct {
					name     string
					category string
				}{
					{"Laptop Pro", "Electronics"},
					{"Laptop Air", "Electronics"},
					{"Mouse", "Electronics"},
					{"Book Pro", "Books"},
				}
				for i, p := range products {
					product := &testentities.Product{
						TenantId: "T1",
						Id:       fmt.Sprintf("prod%d", i+1),
						Category: p.category,
						Name:     p.name,
						Price:    99.99,
					}
					assert.Success(t, store.Create(ctx, product))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.Products{
					TenantId: "T1",
					Category: "Electronics",
					Name:     "Laptop*",
				}
			},
			expectedCount: 2, // Laptop Pro, Laptop Air
			verifyFunc: func(t *testing.T, q data.Queryable) {
				for _, item := range q.Items() {
					product := item.(*testentities.Product)
					if product.Category != "Electronics" {
						t.Errorf("Expected Category=Electronics, got %s", product.Category)
					}
					if len(product.Name) < 6 || product.Name[:6] != "Laptop" {
						t.Errorf("Expected Name to start with 'Laptop', got %s", product.Name)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine persistable from query type
			q := tt.queryFunc()
			var persistable data.Persistable
			switch q.(type) {
			case *testentities.CompositeKeyEntities:
				persistable = &testentities.CompositeKeyEntity{}
			case *testentities.Products:
				persistable = &testentities.Product{}
			}

			store, client := setupQueryStore(t, persistable)
			defer cleanupQueryTable(t, client)

			// Setup test data
			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			// Execute query
			q = tt.queryFunc()
			err := store.Query(context.Background(), q)
			assert.Success(t, err)

			// Verify count
			items := q.Items()
			if len(items) != tt.expectedCount {
				t.Errorf("Expected %d items, got %d", tt.expectedCount, len(items))
			}

			// Additional verification
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, q)
			}
		})
	}
}

// Section 5: Pagination Tests

func TestQuery_Pagination(t *testing.T) {
	tests := []struct {
		name       string
		setupFunc  func(store data.Store)
		queryFunc  func() data.Queryable
		pageSize   int
		verifyFunc func(t *testing.T, pages []data.Queryable)
	}{
		{
			name: "query with limit",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 20; i++ {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      fmt.Sprintf("item%02d", i),
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{PartitionKey: "T1"}
			},
			pageSize: 5,
			verifyFunc: func(t *testing.T, pages []data.Queryable) {
				if len(pages) != 4 { // 20 items / 5 per page = 4 pages
					t.Errorf("Expected 4 pages, got %d", len(pages))
				}
				totalItems := 0
				for _, page := range pages {
					totalItems += len(page.Items())
				}
				if totalItems != 20 {
					t.Errorf("Expected 20 total items, got %d", totalItems)
				}
			},
		},
		{
			name: "pagination preserves sort order",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 15; i++ {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      fmt.Sprintf("item%02d", i),
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{PartitionKey: "T1"}
			},
			pageSize: 7,
			verifyFunc: func(t *testing.T, pages []data.Queryable) {
				// Collect all items across pages
				var allKeys []string
				for _, page := range pages {
					allKeys = append(allKeys, extractSortKeys(page)...)
				}
				// Verify order
				for i := 1; i < len(allKeys); i++ {
					if allKeys[i] < allKeys[i-1] {
						t.Errorf("Items not in order: %s comes after %s", allKeys[i], allKeys[i-1])
					}
				}
			},
		},
		{
			name: "no duplicates across pages",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 25; i++ {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      fmt.Sprintf("item%02d", i),
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{PartitionKey: "T1"}
			},
			pageSize: 10,
			verifyFunc: func(t *testing.T, pages []data.Queryable) {
				// Collect all keys
				var allKeys []string
				for _, page := range pages {
					allKeys = append(allKeys, extractSortKeys(page)...)
				}
				// Check for duplicates
				unique := uniqueIds(allKeys)
				if len(unique) != len(allKeys) {
					t.Errorf("Found duplicates: %d unique vs %d total", len(unique), len(allKeys))
				}
				if len(allKeys) != 25 {
					t.Errorf("Expected 25 total items, got %d", len(allKeys))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup store
			q := tt.queryFunc()
			var persistable data.Persistable
			switch q.(type) {
			case *testentities.CompositeKeyEntities:
				persistable = &testentities.CompositeKeyEntity{}
			}

			store, client := setupQueryStore(t, persistable)
			defer cleanupQueryTable(t, client)

			// Setup test data
			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			// Query all pages
			var pages []data.Queryable
			q = tt.queryFunc()

			// Cast to concrete type to access SetMaximumPageSize
			switch v := q.(type) {
			case *testentities.CompositeKeyEntities:
				v.SetMaximumPageSize(tt.pageSize)
			}

			for {
				err := store.Query(context.Background(), q)
				assert.Success(t, err)
				pages = append(pages, q)

				// Check if there are more pages
				if q.NextPageToken() == nil {
					break
				}

				// Create new query for next page
				q = tt.queryFunc()
				switch v := q.(type) {
				case *testentities.CompositeKeyEntities:
					v.SetMaximumPageSize(tt.pageSize)
				}
				q.SetNextPageToken(pages[len(pages)-1].NextPageToken())
			}

			// Verify results
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, pages)
			}
		})
	}
}

// Section 7: Filter Expression Tests

func TestQuery_FilterExpressions(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func(store data.Store)
		queryFunc     func() data.Queryable
		expectedCount int
		verifyFunc    func(t *testing.T, q data.Queryable)
	}{
		{
			name: "no filter fields",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 5; i++ {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      fmt.Sprintf("item%d", i),
						Data:         fmt.Sprintf("data%d", i),
						Active:       i%2 == 1,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{PartitionKey: "T1"}
			},
			expectedCount: 5, // All items
		},
		{
			name: "single filter field",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				for i := 1; i <= 5; i++ {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      fmt.Sprintf("item%d", i),
						Data:         "test",
						Active:       i%2 == 1, // odd numbers are active
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					Active:       true,
				}
			},
			expectedCount: 3, // items 1, 3, 5
			verifyFunc: func(t *testing.T, q data.Queryable) {
				for _, item := range q.Items() {
					entity := item.(*testentities.CompositeKeyEntity)
					if !entity.Active {
						t.Errorf("Expected Active=true, got false")
					}
				}
			},
		},
		{
			name: "multiple filter fields",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				entities := []struct {
					sortKey string
					data    string
					active  bool
				}{
					{"item1", "test", true},
					{"item2", "test", false},
					{"item3", "other", true},
					{"item4", "test", true},
				}
				for _, e := range entities {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      e.sortKey,
						Data:         e.data,
						Active:       e.active,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{
					PartitionKey: "T1",
					Data:         "test",
					Active:       true,
				}
			},
			expectedCount: 2, // item1 and item4
			verifyFunc: func(t *testing.T, q data.Queryable) {
				for _, item := range q.Items() {
					entity := item.(*testentities.CompositeKeyEntity)
					if entity.Data != "test" || !entity.Active {
						t.Errorf("Expected Data=test and Active=true, got %s, %v", entity.Data, entity.Active)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup store
			q := tt.queryFunc()
			var persistable data.Persistable
			switch q.(type) {
			case *testentities.CompositeKeyEntities:
				persistable = &testentities.CompositeKeyEntity{}
			}

			store, client := setupQueryStore(t, persistable)
			defer cleanupQueryTable(t, client)

			// Setup test data
			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			// Execute query
			q = tt.queryFunc()
			err := store.Query(context.Background(), q)
			assert.Success(t, err)

			// Verify count
			items := q.Items()
			if len(items) != tt.expectedCount {
				t.Errorf("Expected %d items, got %d", tt.expectedCount, len(items))
			}

			// Additional verification
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, q)
			}
		})
	}
}

// Section 8: Sort Order Tests

func TestQuery_SortOrder(t *testing.T) {
	tests := []struct {
		name       string
		setupFunc  func(store data.Store)
		queryFunc  func() data.Queryable
		verifyFunc func(t *testing.T, q data.Queryable)
	}{
		{
			name: "ascending order default",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				items := []string{"item05", "item20", "item01", "item10"}
				for _, sk := range items {
					entity := &testentities.CompositeKeyEntity{
						PartitionKey: "T1",
						SortKey:      sk,
						Data:         "test",
						Active:       true,
					}
					assert.Success(t, store.Create(ctx, entity))
				}
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{PartitionKey: "T1"}
			},
			verifyFunc: func(t *testing.T, q data.Queryable) {
				keys := extractSortKeys(q)
				expected := []string{"item01", "item05", "item10", "item20"}
				for i, key := range keys {
					if key != expected[i] {
						t.Errorf("Position %d: expected %s, got %s", i, expected[i], key)
					}
				}
			},
		},
		{
			name: "descending order with time.Time",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
				for i := 1; i <= 3; i++ {
					order := &testentities.Order{
						TenantId:  "T1",
						OrderId:   fmt.Sprintf("order%d", i),
						UserId:    "user1",
						OrderDate: baseDate.Add(time.Duration(i) * 24 * time.Hour),
						Status:    "pending",
						Total:     100.0,
					}
					assert.Success(t, store.Create(ctx, order))
				}
			},
			queryFunc: func() data.Queryable {
				// Query orders by UserId (uses LSI_1 with descending OrderDate)
				return &testentities.Orders{
					TenantId: "T1",
					UserId:   "user1",
				}
			},
			verifyFunc: func(t *testing.T, q data.Queryable) {
				items := q.Items()
				if len(items) != 3 {
					t.Errorf("Expected 3 items, got %d", len(items))
					return
				}
				// Verify descending order (newest first)
				orders := make([]*testentities.Order, len(items))
				for i, item := range items {
					orders[i] = item.(*testentities.Order)
				}
				// Check that dates are in descending order
				for i := 1; i < len(orders); i++ {
					if orders[i].OrderDate.After(orders[i-1].OrderDate) {
						t.Errorf("Orders not in descending date order: %v comes before %v",
							orders[i-1].OrderDate, orders[i].OrderDate)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup store
			q := tt.queryFunc()
			var persistable data.Persistable
			switch q.(type) {
			case *testentities.CompositeKeyEntities:
				persistable = &testentities.CompositeKeyEntity{}
			case *testentities.Orders:
				persistable = &testentities.Order{}
			}

			store, client := setupQueryStore(t, persistable)
			defer cleanupQueryTable(t, client)

			// Setup test data
			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			// Execute query
			q = tt.queryFunc()
			err := store.Query(context.Background(), q)
			assert.Success(t, err)

			// Verify results
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, q)
			}
		})
	}
}

// Section 9: Error Cases Tests

func TestQuery_ErrorCases(t *testing.T) {
	tests := []struct {
		name       string
		setupFunc  func(store data.Store)
		queryFunc  func() data.Queryable
		expectFail bool
	}{
		{
			name: "missing pk field",
			setupFunc: func(store data.Store) {
				// No setup needed
			},
			queryFunc: func() data.Queryable {
				return &testentities.CompositeKeyEntities{PartitionKey: ""} // Empty PK
			},
			expectFail: true,
		},
		{
			name: "incomplete composite pk",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				entity := &testentities.MultiPartKeyEntity{
					TenantId:   "T1",
					EntityType: "USER",
					Id:         "id1",
					Payload:    "test",
				}
				assert.Success(t, store.Create(ctx, entity))
			},
			queryFunc: func() data.Queryable {
				// Query with only TenantId (missing EntityType for PK)
				return &testentities.MultiPartKeyEntities{TenantId: "T1"}
			},
			expectFail: true,
		},
		{
			name: "incomplete gsi pk",
			setupFunc: func(store data.Store) {
				ctx := context.Background()
				product := &testentities.Product{
					TenantId: "T1",
					Id:       "prod1",
					Category: "Electronics",
					Name:     "Test",
					Price:    99.99,
				}
				assert.Success(t, store.Create(ctx, product))
			},
			queryFunc: func() data.Queryable {
				// Query with only Category (GSI_1 needs TenantId+Category)
				return &testentities.Products{Category: "Electronics"}
			},
			expectFail: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup store
			q := tt.queryFunc()
			var persistable data.Persistable
			switch q.(type) {
			case *testentities.CompositeKeyEntities:
				persistable = &testentities.CompositeKeyEntity{}
			case *testentities.MultiPartKeyEntities:
				persistable = &testentities.MultiPartKeyEntity{}
			case *testentities.Products:
				persistable = &testentities.Product{}
			}

			store, client := setupQueryStore(t, persistable)
			defer cleanupQueryTable(t, client)

			// Setup test data
			if tt.setupFunc != nil {
				tt.setupFunc(store)
			}

			// Execute query
			q = tt.queryFunc()
			err := store.Query(context.Background(), q)

			if tt.expectFail {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				assert.Success(t, err)
			}
		})
	}
}
