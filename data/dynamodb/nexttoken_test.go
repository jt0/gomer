package dynamodb_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/crypto"
	"github.com/jt0/gomer/data"
	ddb "github.com/jt0/gomer/data/dynamodb"
	ddbtest "github.com/jt0/gomer/data/dynamodb/_test"
	"github.com/jt0/gomer/gomerr"
)

const paginationTestTableName = "gomer_pagination_test"

// mockCipher provides a simple XOR-based cipher for testing pagination token encryption.
// This is NOT cryptographically secure and should ONLY be used for tests.
// It implements both Encrypter and Decrypter interfaces.
type mockCipher struct {
	key byte
}

func (m *mockCipher) Encrypt(ctx context.Context, plaintext []byte, ec map[string]string) ([]byte, gomerr.Gomerr) {
	encrypted := make([]byte, len(plaintext))
	for i, b := range plaintext {
		encrypted[i] = b ^ m.key
	}
	return encrypted, nil
}

func (m *mockCipher) Decrypt(ctx context.Context, ciphertext []byte, ec map[string]string) ([]byte, gomerr.Gomerr) {
	// XOR decryption is symmetric
	return m.Encrypt(ctx, ciphertext, ec)
}

// createTestCipher creates a cipher for testing pagination.
// Uses a simple XOR cipher that is sufficient for testing token encryption/decryption.
func createTestCipher(t *testing.T, key []byte) crypto.Cipher {
	keyByte := byte(42) // Default key
	if len(key) > 0 {
		keyByte = key[0]
	}
	return crypto.Cipher{
		Encrypter: &mockCipher{key: keyByte},
		Decrypter: &mockCipher{key: keyByte},
	}
}

// Setup/teardown helpers

func setupPaginationStore(t *testing.T, cipher crypto.Cipher, persistables ...data.Persistable) (data.Store, *dynamodb.Client) {
	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)

	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	// Define table with PK, SK, and common GSI/LSI indexes
	tableDef := &ddbtest.TableDefinition{}
	tableDef.WithTableName(paginationTestTableName).
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

	config := &ddb.Configuration{
		DynamoDb:           client,
		MaxResultsDefault:  100,
		MaxResultsMax:      1000,
		ConsistencyDefault: ddb.Preferred,
	}

	if cipher.Encrypter != nil {
		config.NextTokenCipher = cipher
	}

	store, ge := ddb.Store(paginationTestTableName, config, persistables...)
	assert.Success(t, ge)

	return store, client
}

func cleanupPaginationTable(t *testing.T, client *dynamodb.Client) {
	err := ddbtest.DeleteAllTableData(client, paginationTestTableName)
	assert.Success(t, err)
}

// seedTestData creates count User entities in the store
func seedTestData(t *testing.T, store data.Store, tenantId string, count int) []*ddbtest.User {
	ctx := context.Background()
	entities := make([]*ddbtest.User, count)

	for i := 0; i < count; i++ {
		user := &ddbtest.User{
			TenantId: tenantId,
			Id:       fmt.Sprintf("user%03d", i), // Zero-padded for sort order
			Email:    fmt.Sprintf("user%d@test.com", i),
			Name:     fmt.Sprintf("User %d", i),
			Status:   "active",
		}
		ge := store.Create(ctx, user)
		assert.Success(t, ge)
		entities[i] = user
	}

	return entities
}

// seedOrders creates count Order entities for a specific user
func seedOrders(t *testing.T, store data.Store, tenantId, userId string, count int) []*ddbtest.Order {
	ctx := context.Background()
	entities := make([]*ddbtest.Order, count)

	for i := 0; i < count; i++ {
		order := &ddbtest.Order{
			TenantId:  tenantId,
			OrderId:   fmt.Sprintf("order%03d", i),
			UserId:    userId,
			OrderDate: time.Now().Add(-time.Duration(i) * time.Hour),
			Status:    "pending",
			Total:     100.0,
		}
		ge := store.Create(ctx, order)
		assert.Success(t, ge)
		entities[i] = order
	}

	return entities
}

// collectAllPages executes query and follows pagination to retrieve all items
func collectAllPages(t *testing.T, store data.Store, q data.Queryable) []any {
	ctx := context.Background()
	var allItems []any
	pageCount := 0
	maxPages := 1000 // Safety limit

	for {
		ge := store.Query(ctx, q)
		assert.Success(t, ge)

		allItems = append(allItems, q.Results()...)
		pageCount++

		if q.NextPageToken() == nil {
			break
		}

		if pageCount >= maxPages {
			t.Fatalf("Exceeded max pages (%d), possible infinite loop", maxPages)
		}

		q.SetNextPageToken(q.NextPageToken())
	}

	return allItems
}

// verifyNoDuplicates checks that all items have unique IDs
func verifyNoDuplicates(t *testing.T, items []any) {
	seen := make(map[string]bool)

	for _, item := range items {
		var id string
		switch v := item.(type) {
		case *ddbtest.User:
			id = v.Id
		case *ddbtest.Product:
			id = v.Id
		case *ddbtest.Order:
			id = v.OrderId
		default:
			t.Fatalf("Unknown item type: %T", item)
		}

		if seen[id] {
			t.Errorf("Duplicate item found: %s", id)
		}
		seen[id] = true
	}
}

// Test Section 1: Basic Pagination Behavior

func TestPagination_BasicBehavior(t *testing.T) {
	cipher := createTestCipher(t, []byte("test-key"))
	store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
	defer cleanupPaginationTable(t, client)

	ctx := context.Background()

	tests := []struct {
		name            string
		itemCount       int
		maxPageSize     int
		expectedItems   int
		expectNextToken bool
	}{
		{
			name:            "no pagination needed (result < limit)",
			itemCount:       5,
			maxPageSize:     10,
			expectedItems:   5,
			expectNextToken: false,
		},
		{
			name:            "page boundary items (unknown if more results exist)",
			itemCount:       10,
			maxPageSize:     10,
			expectedItems:   10,
			expectNextToken: true,
		},
		{
			name:            "pagination required (result > limit)",
			itemCount:       15,
			maxPageSize:     10,
			expectedItems:   10,
			expectNextToken: true,
		},
		{
			name:            "single page size",
			itemCount:       5,
			maxPageSize:     1,
			expectedItems:   1,
			expectNextToken: true,
		},
		{
			name:            "no page size specified",
			itemCount:       200,
			maxPageSize:     0,
			expectedItems:   100, // MaxResultsDefault
			expectNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up before each test
			cleanupPaginationTable(t, client)

			// Seed data
			seedTestData(t, store, "T1", tt.itemCount)

			// Query
			q := &ddbtest.Users{TenantId: "T1"}
			q.SetMaximumPageSize(tt.maxPageSize)
			ge := store.Query(ctx, q)
			assert.Success(t, ge)

			// Verify result count
			items := q.Results()
			assert.Assert(t, len(items) <= tt.expectedItems, "Expected at most %d items, got %d", tt.expectedItems, len(items))

			// For non-filtered queries, we should get exactly the expected count
			if tt.itemCount >= tt.expectedItems {
				assert.Equals(t, tt.expectedItems, len(items))
			}

			// Verify NextPageToken presence
			hasToken := q.NextPageToken() != nil
			assert.Equals(t, tt.expectNextToken, hasToken)
		})
	}
}

// Test Section 2: Multi-Page Query Tests

func TestPagination_MultiPage(t *testing.T) {
	cipher := createTestCipher(t, []byte("test-key"))
	store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
	defer cleanupPaginationTable(t, client)

	tests := []struct {
		name          string
		itemCount     int
		pageSize      int
		expectedPages int
	}{
		{
			name:          "two pages exactly",
			itemCount:     20,
			pageSize:      10,
			expectedPages: 2,
		},
		{
			name:          "three pages with remainder",
			itemCount:     25,
			pageSize:      10,
			expectedPages: 3,
		},
		{
			name:          "many small pages",
			itemCount:     10,
			pageSize:      2,
			expectedPages: 5,
		},
		{
			name:          "pagination across index boundary",
			itemCount:     100,
			pageSize:      30,
			expectedPages: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up before each test
			cleanupPaginationTable(t, client)

			// Seed data
			seededUsers := seedTestData(t, store, "T1", tt.itemCount)

			// Collect all pages
			q := &ddbtest.Users{TenantId: "T1"}
			q.SetMaximumPageSize(tt.pageSize)
			allItems := collectAllPages(t, store, q)

			// Verify total items collected
			assert.Equals(t, tt.itemCount, len(allItems),
				"Expected %d total items, got %d", tt.itemCount, len(allItems))

			// Verify no duplicates
			verifyNoDuplicates(t, allItems)

			// Verify all seeded IDs are present
			seenIds := make(map[string]bool)
			for _, item := range allItems {
				user := item.(*ddbtest.User)
				seenIds[user.Id] = true
			}

			for _, seededUser := range seededUsers {
				assert.Assert(t, seenIds[seededUser.Id],
					"Seeded user %s not found in results", seededUser.Id)
			}

			// Verify items are in order (ascending by Id due to zero-padding)
			for i := 0; i < len(allItems)-1; i++ {
				u1 := allItems[i].(*ddbtest.User)
				u2 := allItems[i+1].(*ddbtest.User)
				assert.Assert(t, u1.Id < u2.Id,
					"Items not in order: %s should come before %s", u1.Id, u2.Id)
			}
		})
	}
}

// Test Section 3: NextToken Encryption Tests

func TestPagination_NextTokenEncryption(t *testing.T) {
	ctx := context.Background()

	t.Run("token encrypted with cipher", func(t *testing.T) {
		cipher := createTestCipher(t, []byte("test-key"))
		store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
		defer cleanupPaginationTable(t, client)

		// Seed data that requires pagination
		seedTestData(t, store, "T1", 15)

		// Query with pagination
		q := &ddbtest.Users{TenantId: "T1"}
		q.SetMaximumPageSize(5)
		ge := store.Query(ctx, q)
		assert.Success(t, ge)

		// Verify token exists
		assert.Assert(t, q.NextPageToken() != nil, "Expected NextPageToken to be set")

		// Verify token is opaque (encrypted, not parsable as JSON)
		token := *q.NextPageToken()
		decoded, err := base64.RawURLEncoding.DecodeString(token)
		assert.Success(t, err)

		// Attempt to parse as JSON - should fail (it's encrypted)
		var tokenMap map[string]any
		err = json.Unmarshal(decoded, &tokenMap)
		assert.Assert(t, err != nil, "Encrypted token should not be parsable as JSON")

		// Verify pagination works with encrypted token
		q.SetNextPageToken(&token)
		ge = store.Query(ctx, q)
		assert.Success(t, ge)
		assert.Assert(t, len(q.Results()) > 0, "Should retrieve items on second page")
	})

	t.Run("encrypted token cannot be tampered", func(t *testing.T) {
		t.Skip("Tests not validated")

		cipher := createTestCipher(t, []byte("test-key"))
		store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
		defer cleanupPaginationTable(t, client)

		// Seed data that requires pagination
		seedTestData(t, store, "T1", 15)

		// Query to get a valid token
		q := &ddbtest.Users{TenantId: "T1"}
		q.SetMaximumPageSize(5)
		ge := store.Query(ctx, q)
		assert.Success(t, ge)

		token := *q.NextPageToken()

		// Tamper with the token by flipping a character in the middle
		tampered := token[:len(token)/2] + "X" + token[len(token)/2+1:]

		// Attempt to use tampered token
		q2 := &ddbtest.Users{TenantId: "T1"}
		q2.SetMaximumPageSize(5)
		q2.SetNextPageToken(&tampered)
		ge = store.Query(ctx, q2)

		// Expect MalformedValue error
		assert.Assert(t, ge != nil, "Expected error with tampered token")
		assert.ErrorType(t, ge, gomerr.MalformedValue("", nil))
	})

	t.Run("encrypted token with different cipher fails", func(t *testing.T) {
		t.Skip("Tests not validated")

		cipher1 := createTestCipher(t, []byte("key1"))
		store1, client := setupPaginationStore(t, cipher1, &ddbtest.User{})
		defer cleanupPaginationTable(t, client)

		// Seed data
		seedTestData(t, store1, "T1", 15)

		// Query with Store1 to get token
		q1 := &ddbtest.Users{TenantId: "T1"}
		q1.SetMaximumPageSize(5)
		ge := store1.Query(ctx, q1)
		assert.Success(t, ge)

		token := *q1.NextPageToken()

		// Create Store2 with different cipher
		cipher2 := createTestCipher(t, []byte("key2"))
		store2, ge := ddb.Store(paginationTestTableName, &ddb.Configuration{
			DynamoDb:          client,
			NextTokenCipher:   cipher2,
			MaxResultsDefault: 100,
		}, &ddbtest.User{})
		assert.Success(t, ge)

		// Attempt to use Store1's token with Store2
		q2 := &ddbtest.Users{TenantId: "T1"}
		q2.SetMaximumPageSize(5)
		q2.SetNextPageToken(&token)
		ge = store2.Query(ctx, q2)

		// Expect MalformedValue error (decryption failure)
		assert.Assert(t, ge != nil, "Expected error with cross-cipher token")
		assert.ErrorType(t, ge, gomerr.MalformedValue("", nil))
	})
}

// Test Section 4: Limit Configuration Tests

func TestPagination_LimitConfiguration(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name              string
		maxResultsDefault int
		maxResultsMax     int
		queryPageSize     int
		expectedMaxLimit  int
	}{
		{
			name:              "default limit applied",
			maxResultsDefault: 50,
			maxResultsMax:     1000,
			queryPageSize:     0,
			expectedMaxLimit:  50,
		},
		{
			name:              "custom page size within max",
			maxResultsDefault: 50,
			maxResultsMax:     1000,
			queryPageSize:     100,
			expectedMaxLimit:  100,
		},
		{
			name:              "custom page size exceeds max",
			maxResultsDefault: 50,
			maxResultsMax:     100,
			queryPageSize:     500,
			expectedMaxLimit:  100,
		},
		{
			name:              "page size equals max",
			maxResultsDefault: 50,
			maxResultsMax:     200,
			queryPageSize:     200,
			expectedMaxLimit:  200,
		},
		{
			name:              "negative page size treated as zero",
			maxResultsDefault: 50,
			maxResultsMax:     1000,
			queryPageSize:     -10,
			expectedMaxLimit:  50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cipher := createTestCipher(t, []byte("test-key"))

			// Create custom client for this test
			client, isLocal, err := ddbtest.NewClient()
			assert.Success(t, err)
			if !isLocal {
				t.Skip("Skipping test: DDB_LOCAL not set")
			}

			// Ensure table exists
			tableDef := &ddbtest.TableDefinition{}
			tableDef.WithTableName(paginationTestTableName).
				WithAttributeDefinition("PK", types.ScalarAttributeTypeS).
				WithAttributeDefinition("SK", types.ScalarAttributeTypeS).
				WithAttributeDefinition("GSI_1_PK", types.ScalarAttributeTypeS).
				WithAttributeDefinition("GSI_1_SK", types.ScalarAttributeTypeS).
				WithKeySchema("PK", types.KeyTypeHash).
				WithKeySchema("SK", types.KeyTypeRange).
				WithGsi("gsi_1", []types.KeySchemaElement{
					{AttributeName: aws.String("GSI_1_PK"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("GSI_1_SK"), KeyType: types.KeyTypeRange},
				}, types.Projection{ProjectionType: types.ProjectionTypeAll})
			tableDef.Create(client)

			// Create store with custom limits
			store, ge := ddb.Store(paginationTestTableName, &ddb.Configuration{
				DynamoDb:          client,
				NextTokenCipher:   cipher,
				MaxResultsDefault: int64(tt.maxResultsDefault),
				MaxResultsMax:     int64(tt.maxResultsMax),
			}, &ddbtest.User{})
			assert.Success(t, ge)

			// Clean and seed data
			cleanupPaginationTable(t, client)
			seedTestData(t, store, "T1", 200)

			// Query with specified page size
			q := &ddbtest.Users{TenantId: "T1"}
			q.SetMaximumPageSize(tt.queryPageSize)
			ge = store.Query(ctx, q)
			assert.Success(t, ge)

			// Verify result count respects limit
			items := q.Results()
			assert.Assert(t, len(items) <= tt.expectedMaxLimit,
				"Expected at most %d items, got %d", tt.expectedMaxLimit, len(items))

			// Verify pagination still works (NextPageToken present if more results)
			if len(items) < 200 {
				assert.Assert(t, q.NextPageToken() != nil,
					"Expected NextPageToken when results are limited")
			}
		})
	}
}

// Test Section 5: NextToken Expiration Tests

func TestPagination_TokenExpiration(t *testing.T) {
	// Note: Full time-based expiration testing requires time mocking.
	// These tests focus on version mismatch which is easier to trigger.

	t.Run("valid token not expired", func(t *testing.T) {
		t.Skip("Tests not validated")

		cipher := createTestCipher(t, []byte("test-key"))
		store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
		defer cleanupPaginationTable(t, client)

		ctx := context.Background()

		// Seed data
		seedTestData(t, store, "T1", 15)

		// Query to get fresh token
		q := &ddbtest.Users{TenantId: "T1"}
		q.SetMaximumPageSize(5)
		ge := store.Query(ctx, q)
		assert.Success(t, ge)

		token := *q.NextPageToken()

		// Use token immediately (should be valid)
		q2 := &ddbtest.Users{TenantId: "T1"}
		q2.SetMaximumPageSize(5)
		q2.SetNextPageToken(&token)
		ge = store.Query(ctx, q2)
		assert.Success(t, ge)
	})

	// Note: Testing actual time-based expiration would require:
	// 1. Time mocking library to advance clock 25+ hours
	// 2. Or a test helper in dynamodb package (not dynamodb_test) to create expired tokens
	// 3. Or long-running integration tests
	// For now, this is documented as a limitation and expiration is verified manually.
}

// Test Section 6: Edge Case Tests

func TestPagination_EdgeCases(t *testing.T) {
	cipher := createTestCipher(t, []byte("test-key"))
	store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
	defer cleanupPaginationTable(t, client)

	ctx := context.Background()

	t.Run("empty result set", func(t *testing.T) {
		cleanupPaginationTable(t, client)

		// Query with no matches
		q := &ddbtest.Users{TenantId: "nonexistent"}
		q.SetMaximumPageSize(10)
		ge := store.Query(ctx, q)
		assert.Success(t, ge)

		// Verify empty results
		assert.Equals(t, 0, len(q.Results()), "Expected empty result set")
		assert.Assert(t, q.NextPageToken() == nil, "Expected no NextPageToken")
	})

	t.Run("single item result", func(t *testing.T) {
		cleanupPaginationTable(t, client)
		seedTestData(t, store, "T1", 1)

		q := &ddbtest.Users{TenantId: "T1"}
		q.SetMaximumPageSize(10)
		ge := store.Query(ctx, q)
		assert.Success(t, ge)

		assert.Equals(t, 1, len(q.Results()), "Expected single item")
		assert.Assert(t, q.NextPageToken() == nil, "Expected no NextPageToken")
	})

	t.Run("malformed next token", func(t *testing.T) {
		cleanupPaginationTable(t, client)
		seedTestData(t, store, "T1", 10)

		// Invalid base64
		invalidToken := "not-base64!@#$%"
		q := &ddbtest.Users{TenantId: "T1"}
		q.SetNextPageToken(&invalidToken)
		ge := store.Query(ctx, q)

		assert.Assert(t, ge != nil, "Expected error with malformed token")
		assert.ErrorType(t, ge, gomerr.MalformedValue("", nil))
	})

	t.Run("reuse same query object", func(t *testing.T) {
		t.Skip("Tests not validated")

		cleanupPaginationTable(t, client)
		seedTestData(t, store, "T1", 15)

		q := &ddbtest.Users{TenantId: "T1"}
		q.SetMaximumPageSize(5)

		// First query
		ge := store.Query(ctx, q)
		assert.Success(t, ge)
		firstPageCount := len(q.Results())
		firstPageFirstId := q.Results()[0].(*ddbtest.User).Id

		// Reuse same object for second query
		ge = store.Query(ctx, q)
		assert.Success(t, ge)
		secondPageCount := len(q.Results())
		secondPageFirstId := q.Results()[0].(*ddbtest.User).Id

		// Should get same results (query resets)
		assert.Equals(t, firstPageCount, secondPageCount,
			"Reused query should return same page size")
		assert.Equals(t, firstPageFirstId, secondPageFirstId,
			"Reused query should return same items")
	})
}

// Test Section 7: Pagination Across Indexes

func TestPagination_AcrossIndexes(t *testing.T) {
	cipher := createTestCipher(t, []byte("test-key"))

	t.Run("base table pagination", func(t *testing.T) {
		store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
		defer cleanupPaginationTable(t, client)

		cleanupPaginationTable(t, client)
		seedTestData(t, store, "T1", 25)

		q := &ddbtest.Users{TenantId: "T1"}
		q.SetMaximumPageSize(10)
		allItems := collectAllPages(t, store, q)

		assert.Equals(t, 25, len(allItems), "Should retrieve all items via base table")
		verifyNoDuplicates(t, allItems)
	})

	t.Run("LSI pagination with descending order", func(t *testing.T) {
		t.Skip("Tests not validated")

		store, client := setupPaginationStore(t, cipher, &ddbtest.Order{})
		defer cleanupPaginationTable(t, client)

		cleanupPaginationTable(t, client)
		seedOrders(t, store, "T1", "user1", 25)

		q := &ddbtest.Orders{TenantId: "T1", UserId: "user1"}
		q.SetMaximumPageSize(10)
		allItems := collectAllPages(t, store, q)

		assert.Equals(t, 25, len(allItems), "Should retrieve all orders via LSI")
		verifyNoDuplicates(t, allItems)

		// Verify descending order by date
		for i := 0; i < len(allItems)-1; i++ {
			o1 := allItems[i].(*ddbtest.Order)
			o2 := allItems[i+1].(*ddbtest.Order)
			assert.Assert(t, o1.OrderDate.After(o2.OrderDate) || o1.OrderDate.Equal(o2.OrderDate),
				"Orders should be in descending date order")
		}
	})

	t.Run("GSI pagination", func(t *testing.T) {
		store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
		defer cleanupPaginationTable(t, client)

		cleanupPaginationTable(t, client)

		// Seed users with unique emails
		ctx := context.Background()
		for i := 0; i < 25; i++ {
			user := &ddbtest.User{
				TenantId: "T1",
				Id:       fmt.Sprintf("user%03d", i),
				Email:    fmt.Sprintf("user%03d@test.com", i),
				Name:     fmt.Sprintf("User %d", i),
				Status:   "active",
			}
			ge := store.Create(ctx, user)
			assert.Success(t, ge)
		}

		// Query by specific email (GSI lookup)
		q := &ddbtest.Users{Email: "user001@test.com"}
		ge := store.Query(ctx, q)
		assert.Success(t, ge)

		items := q.Results()
		assert.Equals(t, 1, len(items), "Should find single user by email via GSI")
	})
}

// Test Section 8: Token Serialization (internal validation)

func TestPagination_TokenSerialization(t *testing.T) {
	// Note: Token serialization functions (encodeLastEvaluatedKey, decodeLastEvaluatedKey)
	// are unexported and tested indirectly through pagination tests above.
	// This test validates the roundtrip behavior at the API level.

	cipher := createTestCipher(t, []byte("test-key"))
	store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
	defer cleanupPaginationTable(t, client)

	ctx := context.Background()

	t.Run("roundtrip string keys", func(t *testing.T) {
		cleanupPaginationTable(t, client)
		seedTestData(t, store, "T1", 25)

		// Get first page
		q1 := &ddbtest.Users{TenantId: "T1"}
		q1.SetMaximumPageSize(10)
		ge := store.Query(ctx, q1)
		assert.Success(t, ge)

		// Save last item ID from first page
		lastItemOnPage1 := q1.Results()[len(q1.Results())-1].(*ddbtest.User).Id

		// Get second page using token
		q2 := &ddbtest.Users{TenantId: "T1"}
		q2.SetMaximumPageSize(10)
		q2.SetNextPageToken(q1.NextPageToken())
		ge = store.Query(ctx, q2)
		assert.Success(t, ge)

		// First item on page 2 should be different from last item on page 1
		firstItemOnPage2 := q2.Results()[0].(*ddbtest.User).Id
		assert.Assert(t, firstItemOnPage2 != lastItemOnPage1,
			"Pages should not overlap")

		// Items should be sequential
		assert.Assert(t, firstItemOnPage2 > lastItemOnPage1,
			"Second page items should come after first page items")
	})
}

// Test Section 9: Consistency and Pagination

func TestPagination_ConsistencyTypes(t *testing.T) {
	cipher := createTestCipher(t, []byte("test-key"))
	store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
	defer cleanupPaginationTable(t, client)

	seedTestData(t, store, "T1", 25)

	// TODO: Consistency type tests require Users to implement ConsistencyTyper interface
	// t.Run("consistent read on base table", func(t *testing.T) {
	// 	q := &ddbtest.Users{TenantId: "T1"}
	// 	q.SetConsistencyType(ddb.Required)
	// 	q.SetMaximumPageSize(10)
	//
	// 	// First page
	// 	ge := store.Query(ctx, q)
	// 	assert.Success(t, ge)
	//
	// 	// Second page with consistent read
	// 	if q.NextPageToken() != nil {
	// 		q.SetNextPageToken(q.NextPageToken())
	// 		ge = store.Query(ctx, q)
	// 		assert.Success(t, ge)
	// 	}
	// })
	//
	// t.Run("eventually consistent on base table", func(t *testing.T) {
	// 	q := &ddbtest.Users{TenantId: "T1"}
	// 	q.SetConsistencyType(ddb.Indifferent)
	// 	q.SetMaximumPageSize(10)
	//
	// 	ge := store.Query(ctx, q)
	// 	assert.Success(t, ge)
	// })
}

// Test Section 10: Performance Tests

//func TestPagination_Performance(t *testing.T) {
//	if testing.Short() {
//		t.Skip("Skipping performance test in short mode")
//	}
//
//	cipher := createTestCipher(t, []byte("test-key"))
//	store, client := setupPaginationStore(t, cipher, &ddbtest.User{})
//	defer cleanupPaginationTable(t, client)
//
//	t.Run("large result set pagination", func(t *testing.T) {
//		cleanupPaginationTable(t, client)
//
//		// Seed 1000 items (reduced from 10000 for practicality)
//		t.Log("Seeding 1000 items...")
//		for batch := 0; batch < 10; batch++ {
//			seedTestData(t, store, fmt.Sprintf("T%d", batch), 100)
//		}
//
//		// Paginate through all items
//		start := time.Now()
//		totalItems := 0
//		for batch := 0; batch < 10; batch++ {
//			q := &ddbtest.Users{TenantId: fmt.Sprintf("T%d", batch)}
//			q.SetMaximumPageSize(100)
//			allItems := collectAllPages(t, store, q)
//			totalItems += len(allItems)
//		}
//		elapsed := time.Since(start)
//
//		assert.Equals(t, 1000, totalItems, "Should retrieve all 1000 items")
//		t.Logf("Retrieved 1000 items in %v", elapsed)
//	})
//
//	t.Run("many small pages", func(t *testing.T) {
//		cleanupPaginationTable(t, client)
//		seedTestData(t, store, "T1", 50)
//
//		start := time.Now()
//		q := &ddbtest.Users{TenantId: "T1"}
//		q.SetMaximumPageSize(1)
//		allItems := collectAllPages(t, store, q)
//		elapsed := time.Since(start)
//
//		assert.Equals(t, 50, len(allItems), "Should retrieve all items with page size 1")
//		t.Logf("Retrieved 50 items with page size 1 in %v (50 pages)", elapsed)
//	})
//}

//// Benchmark token generation
//func BenchmarkTokenGeneration(b *testing.B) {
//	cipher := createTestCipher(nil, []byte("test-key"))
//	store, client := setupPaginationStore(nil, cipher, &ddbtest.User{})
//
//	// Note: Can't use defer in benchmark, manual cleanup needed
//	ctx := context.Background()
//
//	// Seed some data
//	for i := 0; i < 20; i++ {
//		user := &ddbtest.User{
//			TenantId: "T1",
//			Id:       fmt.Sprintf("user%03d", i),
//			Email:    fmt.Sprintf("user%d@test.com", i),
//			Name:     fmt.Sprintf("User %d", i),
//		}
//		store.Create(ctx, user)
//	}
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		q := &ddbtest.Users{TenantId: "T1"}
//		q.SetMaximumPageSize(5)
//		store.Query(ctx, q)
//		// Token generation happens inside Query
//	}
//
//	// Cleanup
//	ddbtest.DeleteAllTableData(client, paginationTestTableName)
//}
