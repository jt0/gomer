package dynamodb_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/data/dataerr"
	ddb "github.com/jt0/gomer/data/dynamodb"
	ddbtest "github.com/jt0/gomer/data/dynamodb/_test"
	"github.com/jt0/gomer/gomerr"
)

const errorsTestTableName = "gomer_errors_test"

// Test setup helpers

func setupErrorStore(t *testing.T, config *ddb.Configuration, persistables ...data.Persistable) (data.Store, *dynamodb.Client) {
	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)

	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	// Use provided config or create default
	if config == nil {
		config = testConfig()
	}
	config.DynamoDb = client

	store, ge := ddb.Store(errorsTestTableName, config, persistables...)
	assert.Success(t, ge)

	return store, client
}

func testConfig() *ddb.Configuration {
	return &ddb.Configuration{
		MaxResultsDefault:           100,
		MaxResultsMax:               1000,
		ConsistencyDefault:          ddb.Preferred,
		FailDeleteIfNotPresent:      false,
		ValidateKeyFieldConsistency: false,
	}
}

func cleanupErrorTable(t *testing.T, client *dynamodb.Client) {
	err := ddbtest.DeleteAllTableData(client, errorsTestTableName)
	assert.Success(t, err)
}

// ============================================================================
// SECTION 1: CONFIGURATION ERRORS
// ============================================================================
// Configuration errors are detected during Store initialization. They indicate
// developer mistakes in struct tags or configuration values.

// ----------------------------------------------------------------------------
// 1.1 Invalid db.keys Tag Format
// ----------------------------------------------------------------------------

// Entities with intentionally invalid tags for testing
type InvalidKeysEntity1 struct {
	Id string `db.keys:"0"` // Missing pk/sk prefix
}

func (e *InvalidKeysEntity1) TypeName() string             { return "InvalidKeysEntity1" }
func (e *InvalidKeysEntity1) NewQueryable() data.Queryable { return &InvalidKeysQueryable1{} }

type InvalidKeysQueryable1 struct{ data.BaseQueryable }

func (q *InvalidKeysQueryable1) TypeNames() []string         { return []string{"InvalidKeysEntity1"} }
func (q *InvalidKeysQueryable1) TypeOf(_ interface{}) string { return "InvalidKeysEntity1" }

type InvalidKeysEntity2 struct {
	Id string `db.keys:"pk.abc"` // Non-numeric part index
}

func (e *InvalidKeysEntity2) TypeName() string             { return "InvalidKeysEntity2" }
func (e *InvalidKeysEntity2) NewQueryable() data.Queryable { return &InvalidKeysQueryable2{} }

type InvalidKeysQueryable2 struct{ data.BaseQueryable }

func (q *InvalidKeysQueryable2) TypeNames() []string         { return []string{"InvalidKeysEntity2"} }
func (q *InvalidKeysQueryable2) TypeOf(_ interface{}) string { return "InvalidKeysEntity2" }

type InvalidKeysEntity3 struct {
	Id string `db.keys:"pk.0='UNCLOSED"` // Unclosed quote
}

func (e *InvalidKeysEntity3) TypeName() string             { return "InvalidKeysEntity3" }
func (e *InvalidKeysEntity3) NewQueryable() data.Queryable { return &InvalidKeysQueryable3{} }

type InvalidKeysQueryable3 struct{ data.BaseQueryable }

func (q *InvalidKeysQueryable3) TypeNames() []string         { return []string{"InvalidKeysEntity3"} }
func (q *InvalidKeysQueryable3) TypeOf(_ interface{}) string { return "InvalidKeysEntity3" }

type InvalidKeysEntity4 struct {
	Id string `db.keys:"nonexistent:pk"` // Undefined index
}

func (e *InvalidKeysEntity4) TypeName() string             { return "InvalidKeysEntity4" }
func (e *InvalidKeysEntity4) NewQueryable() data.Queryable { return &InvalidKeysQueryable4{} }

type InvalidKeysQueryable4 struct{ data.BaseQueryable }

func (q *InvalidKeysQueryable4) TypeNames() []string         { return []string{"InvalidKeysEntity4"} }
func (q *InvalidKeysQueryable4) TypeOf(_ interface{}) string { return "InvalidKeysEntity4" }

type InvalidKeysEntity5 struct {
	Id string `db.keys:"pk.0='"` // Malformed expression
}

func (e *InvalidKeysEntity5) TypeName() string             { return "InvalidKeysEntity5" }
func (e *InvalidKeysEntity5) NewQueryable() data.Queryable { return &InvalidKeysQueryable5{} }

type InvalidKeysQueryable5 struct{ data.BaseQueryable }

func (q *InvalidKeysQueryable5) TypeNames() []string         { return []string{"InvalidKeysEntity5"} }
func (q *InvalidKeysQueryable5) TypeOf(_ interface{}) string { return "InvalidKeysEntity5" }

type InvalidKeysEntity6 struct {
	Id string `db.keys:","` // Empty tag with comma
}

func (e *InvalidKeysEntity6) TypeName() string             { return "InvalidKeysEntity6" }
func (e *InvalidKeysEntity6) NewQueryable() data.Queryable { return &InvalidKeysQueryable6{} }

type InvalidKeysQueryable6 struct{ data.BaseQueryable }

func (q *InvalidKeysQueryable6) TypeNames() []string         { return []string{"InvalidKeysEntity6"} }
func (q *InvalidKeysQueryable6) TypeOf(_ interface{}) string { return "InvalidKeysEntity6" }

func TestErrors_Configuration_InvalidKeysTag(t *testing.T) {
	t.Skip("Tests not validated")

	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)
	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	tests := []struct {
		name          string
		entity        data.Persistable
		expectedErr   string
		expectedField string
	}{
		{
			name:          "missing key type",
			entity:        &InvalidKeysEntity1{},
			expectedErr:   "invalid `db.keys` value",
			expectedField: "Id",
		},
		{
			name:          "invalid part index",
			entity:        &InvalidKeysEntity2{},
			expectedErr:   "invalid `db.keys` value",
			expectedField: "Id",
		},
		{
			name:          "unclosed static quote",
			entity:        &InvalidKeysEntity3{},
			expectedErr:   "invalid `db.keys` value",
			expectedField: "Id",
		},
		{
			name:          "undefined index",
			entity:        &InvalidKeysEntity4{},
			expectedErr:   "undefined index",
			expectedField: "Id",
		},
		{
			name:          "malformed expression",
			entity:        &InvalidKeysEntity5{},
			expectedErr:   "invalid `db.keys` value",
			expectedField: "Id",
		},
		{
			name:          "empty tag with comma",
			entity:        &InvalidKeysEntity6{},
			expectedErr:   "invalid `db.keys` value",
			expectedField: "Id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ge := ddb.Store(errorsTestTableName, &ddb.Configuration{
				DynamoDb:                    client,
				MaxResultsDefault:           100,
				MaxResultsMax:               1000,
				ConsistencyDefault:          ddb.Preferred,
				FailDeleteIfNotPresent:      false,
				ValidateKeyFieldConsistency: false,
			}, tt.entity)

			// Verify error occurred
			if ge == nil {
				t.Fatal("Expected configuration error but got nil")
			}

			// Check error message contains expected strings
			errMsg := ge.Error()
			if !contains(errMsg, tt.expectedErr) {
				t.Errorf("Error message should contain %q, got: %s", tt.expectedErr, errMsg)
			}
			if !contains(errMsg, tt.expectedField) {
				t.Errorf("Error message should contain field name %q, got: %s", tt.expectedField, errMsg)
			}

			// Verify it's a Configuration error
			var configErr *gomerr.ConfigurationError
			if !errors.As(ge, &configErr) {
				t.Errorf("Expected ConfigurationError in chain, got: %T", ge)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// 1.2 Undefined Index Name
// ----------------------------------------------------------------------------

type UndefinedIndexEntity struct {
	Id    string `db.keys:"pk"`
	Email string `db.keys:"gsi_5:pk"` // gsi_5 doesn't exist in table
}

func (e *UndefinedIndexEntity) TypeName() string             { return "UndefinedIndexEntity" }
func (e *UndefinedIndexEntity) NewQueryable() data.Queryable { return &UndefinedIndexQueryable{} }

type UndefinedIndexQueryable struct{ data.BaseQueryable }

func (q *UndefinedIndexQueryable) TypeNames() []string         { return []string{"UndefinedIndexEntity"} }
func (q *UndefinedIndexQueryable) TypeOf(_ interface{}) string { return "UndefinedIndexEntity" }

func TestErrors_Configuration_UndefinedIndex(t *testing.T) {
	t.Skip("Tests not validated")

	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)
	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	entity := &UndefinedIndexEntity{Id: "test", Email: "test@example.com"}

	_, ge := ddb.Store(errorsTestTableName, &ddb.Configuration{
		DynamoDb:                    client,
		MaxResultsDefault:           100,
		MaxResultsMax:               1000,
		ConsistencyDefault:          ddb.Preferred,
		FailDeleteIfNotPresent:      false,
		ValidateKeyFieldConsistency: false,
	}, entity)

	if ge == nil {
		t.Fatal("Expected configuration error but got nil")
	}

	errMsg := ge.Error()
	if !contains(errMsg, "undefined index") {
		t.Errorf("Error message should contain 'undefined index', got: %s", errMsg)
	}
	if !contains(errMsg, "gsi_5") {
		t.Errorf("Error message should contain 'gsi_5', got: %s", errMsg)
	}
	if !contains(errMsg, "Email") {
		t.Errorf("Error message should contain 'Email', got: %s", errMsg)
	}

	var configErr *gomerr.ConfigurationError
	if !errors.As(ge, &configErr) {
		t.Errorf("Expected ConfigurationError in chain, got: %T", ge)
	}
}

// ----------------------------------------------------------------------------
// 1.3 Invalid db.constraints Tag
// ----------------------------------------------------------------------------

type InvalidConstraintsEntity1 struct {
	Id    string `db.keys:"pk"`
	Email string `db.constraints:"foreign(OtherId)"` // Unknown constraint type
}

func (e *InvalidConstraintsEntity1) TypeName() string { return "InvalidConstraintsEntity1" }
func (e *InvalidConstraintsEntity1) NewQueryable() data.Queryable {
	return &InvalidConstraintsQueryable1{}
}

type InvalidConstraintsQueryable1 struct{ data.BaseQueryable }

func (q *InvalidConstraintsQueryable1) TypeNames() []string {
	return []string{"InvalidConstraintsEntity1"}
}
func (q *InvalidConstraintsQueryable1) TypeOf(_ interface{}) string {
	return "InvalidConstraintsEntity1"
}

type InvalidConstraintsEntity2 struct {
	Id    string `db.keys:"pk"`
	Email string `db.constraints:"unique("` // Malformed
}

func (e *InvalidConstraintsEntity2) TypeName() string { return "InvalidConstraintsEntity2" }
func (e *InvalidConstraintsEntity2) NewQueryable() data.Queryable {
	return &InvalidConstraintsQueryable2{}
}

type InvalidConstraintsQueryable2 struct{ data.BaseQueryable }

func (q *InvalidConstraintsQueryable2) TypeNames() []string {
	return []string{"InvalidConstraintsEntity2"}
}
func (q *InvalidConstraintsQueryable2) TypeOf(_ interface{}) string {
	return "InvalidConstraintsEntity2"
}

type InvalidConstraintsEntity3 struct {
	Id    string `db.keys:"pk"`
	Email string `db.constraints:"unique[]"` // Wrong bracket type
}

func (e *InvalidConstraintsEntity3) TypeName() string { return "InvalidConstraintsEntity3" }
func (e *InvalidConstraintsEntity3) NewQueryable() data.Queryable {
	return &InvalidConstraintsQueryable3{}
}

type InvalidConstraintsQueryable3 struct{ data.BaseQueryable }

func (q *InvalidConstraintsQueryable3) TypeNames() []string {
	return []string{"InvalidConstraintsEntity3"}
}
func (q *InvalidConstraintsQueryable3) TypeOf(_ interface{}) string {
	return "InvalidConstraintsEntity3"
}

type ValidEmptyConstraintEntity struct {
	Id    string `db.keys:"pk"`
	Email string `db.constraints:""` // Empty tag is allowed
}

func (e *ValidEmptyConstraintEntity) TypeName() string { return "ValidEmptyConstraintEntity" }
func (e *ValidEmptyConstraintEntity) NewQueryable() data.Queryable {
	return &ValidEmptyConstraintQueryable{}
}

type ValidEmptyConstraintQueryable struct{ data.BaseQueryable }

func (q *ValidEmptyConstraintQueryable) TypeNames() []string {
	return []string{"ValidEmptyConstraintEntity"}
}
func (q *ValidEmptyConstraintQueryable) TypeOf(_ interface{}) string {
	return "ValidEmptyConstraintEntity"
}

type InvalidConstraintsEntity4 struct {
	Id    string `db.keys:"pk"`
	Email string `db.constraints:"invalid"` // Random text
}

func (e *InvalidConstraintsEntity4) TypeName() string { return "InvalidConstraintsEntity4" }
func (e *InvalidConstraintsEntity4) NewQueryable() data.Queryable {
	return &InvalidConstraintsQueryable4{}
}

type InvalidConstraintsQueryable4 struct{ data.BaseQueryable }

func (q *InvalidConstraintsQueryable4) TypeNames() []string {
	return []string{"InvalidConstraintsEntity4"}
}
func (q *InvalidConstraintsQueryable4) TypeOf(_ interface{}) string {
	return "InvalidConstraintsEntity4"
}

func TestErrors_Configuration_InvalidConstraintsTag(t *testing.T) {
	t.Skip("Tests not validated")

	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)
	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	tests := []struct {
		name        string
		entity      data.Persistable
		shouldError bool
		errorMsg    string
	}{
		{
			name:        "unknown constraint type",
			entity:      &InvalidConstraintsEntity1{},
			shouldError: true,
			errorMsg:    "invalid `db.constraints` value",
		},
		{
			name:        "malformed syntax",
			entity:      &InvalidConstraintsEntity2{},
			shouldError: true,
			errorMsg:    "invalid `db.constraints` value",
		},
		{
			name:        "wrong bracket type",
			entity:      &InvalidConstraintsEntity3{},
			shouldError: true,
			errorMsg:    "invalid `db.constraints` value",
		},
		{
			name:        "empty constraint tag",
			entity:      &ValidEmptyConstraintEntity{},
			shouldError: false,
		},
		{
			name:        "random text",
			entity:      &InvalidConstraintsEntity4{},
			shouldError: true,
			errorMsg:    "invalid `db.constraints` value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ge := ddb.Store(errorsTestTableName, &ddb.Configuration{
				DynamoDb:                    client,
				MaxResultsDefault:           100,
				MaxResultsMax:               1000,
				ConsistencyDefault:          ddb.Preferred,
				FailDeleteIfNotPresent:      false,
				ValidateKeyFieldConsistency: false,
			}, tt.entity)

			if tt.shouldError {
				if ge == nil {
					t.Fatal("Expected configuration error but got nil")
				}
				if !contains(ge.Error(), tt.errorMsg) {
					t.Errorf("Error message should contain %q, got: %s", tt.errorMsg, ge.Error())
				}
				var configErr *gomerr.ConfigurationError
				if !errors.As(ge, &configErr) {
					t.Errorf("Expected ConfigurationError in chain, got: %T", ge)
				}
			} else {
				assert.Success(t, ge)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// 1.4 Invalid Separator Configuration
// ----------------------------------------------------------------------------

func TestErrors_Configuration_InvalidSeparator(t *testing.T) {
	t.Skip("Tests not validated")

	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)
	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	tests := []struct {
		name        string
		separator   byte
		shouldError bool
	}{
		{"below range", 31, true},
		{"at lower boundary", 32, true},
		{"valid lower bound", 33, false},
		{"valid upper bound", 126, false},
		{"above range", 127, true},
		{"non-printable null", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &ddb.Configuration{
				DynamoDb:                    client,
				MaxResultsDefault:           100,
				MaxResultsMax:               1000,
				ConsistencyDefault:          ddb.Preferred,
				FailDeleteIfNotPresent:      false,
				ValidateKeyFieldConsistency: false,
				ValueSeparatorChar:          tt.separator,
			}

			_, ge := ddb.Store(errorsTestTableName, config, &ddbtest.CompositeKeyEntity{})

			if tt.shouldError {
				if ge == nil {
					t.Fatalf("Expected configuration error for separator %d but got nil", tt.separator)
				}
				errMsg := ge.Error()
				if !contains(errMsg, "ValueSeparatorChar") {
					t.Errorf("Error message should contain 'ValueSeparatorChar', got: %s", errMsg)
				}
				var configErr *gomerr.ConfigurationError
				if !errors.As(ge, &configErr) {
					t.Errorf("Expected ConfigurationError in chain, got: %T", ge)
				}
			} else {
				assert.Success(t, ge)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// 1.5 Invalid QueryWildcardChar Configuration
// ----------------------------------------------------------------------------

func TestErrors_Configuration_InvalidWildcard(t *testing.T) {
	t.Skip("Tests not validated")

	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)
	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	tests := []struct {
		name        string
		wildcard    byte
		shouldError bool
	}{
		{"alphanumeric", 'a', true},
		{"digit", '5', true},
		{"valid asterisk", '*', false},
		{"valid question", '?', false},
		{"valid percent", '%', false},
		{"space", ' ', true},
		{"zero disabled", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &ddb.Configuration{
				DynamoDb:                    client,
				MaxResultsDefault:           100,
				MaxResultsMax:               1000,
				ConsistencyDefault:          ddb.Preferred,
				FailDeleteIfNotPresent:      false,
				ValidateKeyFieldConsistency: false,
				QueryWildcardChar:           tt.wildcard,
			}

			_, ge := ddb.Store(errorsTestTableName, config, &ddbtest.CompositeKeyEntity{})

			if tt.shouldError {
				if ge == nil {
					t.Fatalf("Expected configuration error for wildcard %c but got nil", tt.wildcard)
				}
				errMsg := ge.Error()
				if !contains(errMsg, "QueryWildcardChar") {
					t.Errorf("Error message should contain 'QueryWildcardChar', got: %s", errMsg)
				}
				var configErr *gomerr.ConfigurationError
				if !errors.As(ge, &configErr) {
					t.Errorf("Expected ConfigurationError in chain, got: %T", ge)
				}
			} else {
				assert.Success(t, ge)
			}
		})
	}
}

// ============================================================================
// SECTION 2: RUNTIME ERRORS
// ============================================================================
// Runtime errors occur during CRUD operations due to invalid data or
// operational constraints.

// ----------------------------------------------------------------------------
// 2.1 Missing Required Key Fields
// ----------------------------------------------------------------------------

func TestErrors_Runtime_MissingKeyFields(t *testing.T) {
	t.Skip("Tests not validated")

	store, client := setupErrorStore(t, nil, &ddbtest.CompositeKeyEntity{}, &ddbtest.CompositeKeyEntity{}, &ddbtest.MultiPartKeyEntity{}, &ddbtest.NumericKeyEntity{}, &ddbtest.PointerKeyEntity{})
	defer cleanupErrorTable(t, client)

	ctx := context.Background()

	tests := []struct {
		name          string
		entity        data.Persistable
		operation     string
		operationFunc func(data.Persistable) gomerr.Gomerr
		expectedKey   string
		expectedField string
	}{
		{
			name:          "empty PK on Create",
			entity:        &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sort1", Data: "Test"},
			operation:     "Create",
			operationFunc: func(p data.Persistable) gomerr.Gomerr { return store.Create(ctx, p) },
			expectedKey:   "PK",
			expectedField: "PartitionKey",
		},
		{
			name:          "empty PK on Read",
			entity:        &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sort1"},
			operation:     "Read",
			operationFunc: func(p data.Persistable) gomerr.Gomerr { return store.Read(ctx, p) },
			expectedKey:   "PK",
			expectedField: "PartitionKey",
		},
		{
			name:          "empty PK on Update",
			entity:        &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sort1", Data: "Test"},
			operation:     "Update",
			operationFunc: func(p data.Persistable) gomerr.Gomerr { return store.Update(ctx, p, nil) },
			expectedKey:   "PK",
			expectedField: "PartitionKey",
		},
		{
			name:          "empty PK on Delete",
			entity:        &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sort1"},
			operation:     "Delete",
			operationFunc: func(p data.Persistable) gomerr.Gomerr { return store.Delete(ctx, p) },
			expectedKey:   "PK",
			expectedField: "Id",
		},
		{
			name:          "empty SK on Create",
			entity:        &ddbtest.CompositeKeyEntity{PartitionKey: "tenant1", SortKey: ""},
			operation:     "Create",
			operationFunc: func(p data.Persistable) gomerr.Gomerr { return store.Create(ctx, p) },
			expectedKey:   "SK",
			expectedField: "SortKey",
		},
		{
			name:          "empty SK on Read",
			entity:        &ddbtest.CompositeKeyEntity{PartitionKey: "tenant1", SortKey: ""},
			operation:     "Read",
			operationFunc: func(p data.Persistable) gomerr.Gomerr { return store.Read(ctx, p) },
			expectedKey:   "SK",
			expectedField: "SortKey",
		},
		{
			name:          "zero int PK",
			entity:        &ddbtest.NumericKeyEntity{Id: 0, Version: 1},
			operation:     "Create",
			operationFunc: func(p data.Persistable) gomerr.Gomerr { return store.Create(ctx, p) },
			expectedKey:   "PK",
			expectedField: "Id",
		},
		{
			name:          "nil pointer PK",
			entity:        &ddbtest.PointerKeyEntity{Id: nil},
			operation:     "Create",
			operationFunc: func(p data.Persistable) gomerr.Gomerr { return store.Create(ctx, p) },
			expectedKey:   "PK",
			expectedField: "Id",
		},
		{
			name:          "missing first part of composite PK",
			entity:        &ddbtest.MultiPartKeyEntity{TenantId: "", EntityType: "TYPE1", Id: "id1"},
			operation:     "Create",
			operationFunc: func(p data.Persistable) gomerr.Gomerr { return store.Create(ctx, p) },
			expectedKey:   "PK",
			expectedField: "TenantId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ge := tt.operationFunc(tt.entity)

			// Verify error occurred
			if ge == nil {
				t.Fatal("Expected KeyValueNotFound error but got nil")
			}

			// Check if error is wrapped in dataerr.Store
			var storeErr *dataerr.StoreError
			if errors.As(ge, &storeErr) {
				if storeErr.Operation != tt.operation {
					t.Errorf("Expected operation %q, got %q", tt.operation, storeErr.Operation)
				}
			}

			// Unwrap to get KeyValueNotFoundError
			var kvnErr *dataerr.KeyValueNotFoundError
			if !errors.As(ge, &kvnErr) {
				t.Fatalf("Expected KeyValueNotFoundError in chain, got: %v", ge)
			}

			// Verify error contains key name
			if kvnErr.KeyName != tt.expectedKey {
				t.Errorf("Expected key name %q, got %q", tt.expectedKey, kvnErr.KeyName)
			}

			// Verify error contains field information
			foundField := false
			for _, f := range kvnErr.KeyFields {
				if f == tt.expectedField {
					foundField = true
					break
				}
			}
			if !foundField {
				t.Errorf("Expected field %q in KeyFields %v", tt.expectedField, kvnErr.KeyFields)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// 2.2 Query Without Partition Key
// ----------------------------------------------------------------------------

func TestErrors_Runtime_QueryWithoutPartitionKey(t *testing.T) {
	t.Skip("Tests not validated")

	store, client := setupErrorStore(t, nil, &ddbtest.User{}, &ddbtest.MultiPartKeyEntity{})
	defer cleanupErrorTable(t, client)

	ctx := context.Background()

	tests := []struct {
		name        string
		queryable   data.Queryable
		expectError bool
	}{
		{
			name:        "no fields set",
			queryable:   &ddbtest.Users{}, // All zero values
			expectError: true,
		},
		{
			name:        "partial composite PK",
			queryable:   &ddbtest.MultiPartKeyEntities{TenantId: "T1"}, // Missing EntityType
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ge := store.Query(ctx, tt.queryable)

			if tt.expectError {
				// Verify error occurred
				if ge == nil {
					t.Fatal("Expected NoIndexMatch error but got nil")
				}

				// Check if error is wrapped in dataerr.Store
				var storeErr *dataerr.StoreError
				if errors.As(ge, &storeErr) {
					if storeErr.Operation != "Query" {
						t.Errorf("Expected operation 'Query', got %q", storeErr.Operation)
					}
				}

				// Unwrap to get NoIndexMatchError
				var nimErr *dataerr.NoIndexMatchError
				if !errors.As(ge, &nimErr) {
					t.Fatalf("Expected NoIndexMatchError in chain, got: %v", ge)
				}

				// Verify error contains available indexes
				if len(nimErr.AvailableIndexes) == 0 {
					t.Error("Expected AvailableIndexes to be populated")
				}

				// Error message should be helpful
				errStr := ge.Error()
				if !contains(errStr, "no index match") && !contains(errStr, "NoIndexMatch") {
					t.Errorf("Error message should mention index matching: %s", errStr)
				}
			} else {
				assert.Success(t, ge)
			}
		})
	}
}

// ============================================================================
// SECTION 3: DYNAMODB-SPECIFIC ERRORS
// ============================================================================
// DynamoDB-specific errors are returned by the AWS SDK and wrapped with
// appropriate context.

// ----------------------------------------------------------------------------
// 3.1 ConditionalCheckFailedException
// ----------------------------------------------------------------------------

func TestErrors_DynamoDB_ConditionalCheckFailed(t *testing.T) {
	t.Skip("Tests not validated")

	_, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)
	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	t.Run("duplicate ID on Create", func(t *testing.T) {
		store, client := setupErrorStore(t, nil, &ddbtest.CompositeKeyEntity{})
		defer cleanupErrorTable(t, client)

		ctx := context.Background()

		entity1 := &ddbtest.CompositeKeyEntity{PartitionKey: "duplicate", SortKey: "sk1", Data: "First"}
		assert.Success(t, store.Create(ctx, entity1))

		// Attempt to create with same ID
		entity2 := &ddbtest.CompositeKeyEntity{PartitionKey: "duplicate", SortKey: "sk1", Data: "Second"}
		ge := store.Create(ctx, entity2)

		if ge == nil {
			t.Fatal("Expected error for duplicate ID but got nil")
		}

		// Should be Internal error for unique constraint failure
		var internalErr *gomerr.InternalError
		if !errors.As(ge, &internalErr) {
			t.Errorf("Expected InternalError in chain, got: %T", ge)
		}

		errMsg := ge.Error()
		if !contains(errMsg, "Unique") && !contains(errMsg, "unique") {
			t.Errorf("Error message should mention unique constraint: %s", errMsg)
		}
	})

	t.Run("delete non-existent with strict mode", func(t *testing.T) {
		configStrict := &ddb.Configuration{
			MaxResultsDefault:           100,
			MaxResultsMax:               1000,
			ConsistencyDefault:          ddb.Preferred,
			FailDeleteIfNotPresent:      true,
			ValidateKeyFieldConsistency: false,
		}
		store, client := setupErrorStore(t, configStrict, &ddbtest.CompositeKeyEntity{})
		defer cleanupErrorTable(t, client)

		ctx := context.Background()

		entity := &ddbtest.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "sk1"}
		ge := store.Delete(ctx, entity)

		if ge == nil {
			t.Fatal("Expected PersistableNotFound error but got nil")
		}

		var pnfErr *dataerr.PersistableNotFoundError
		if !errors.As(ge, &pnfErr) {
			t.Errorf("Expected PersistableNotFoundError in chain, got: %T", ge)
		}
	})
}

// ----------------------------------------------------------------------------
// 3.2 PersistableNotFound
// ----------------------------------------------------------------------------

func TestErrors_DynamoDB_PersistableNotFound(t *testing.T) {
	t.Skip("Tests not validated")

	store, client := setupErrorStore(t, nil, &ddbtest.CompositeKeyEntity{}, &ddbtest.CompositeKeyEntity{})
	defer cleanupErrorTable(t, client)

	ctx := context.Background()

	t.Run("read non-existent simple entity", func(t *testing.T) {
		entity := &ddbtest.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "sk1"}
		ge := store.Read(ctx, entity)

		if ge == nil {
			t.Fatal("Expected PersistableNotFound error but got nil")
		}

		var pnfErr *dataerr.PersistableNotFoundError
		if !errors.As(ge, &pnfErr) {
			t.Fatalf("Expected PersistableNotFoundError in chain, got: %T", ge)
		}

		// Verify error details
		if pnfErr.TypeName != "CompositeKeyEntity" {
			t.Errorf("Expected TypeName 'SimpleEntity', got %q", pnfErr.TypeName)
		}
		if pnfErr.Key == nil {
			t.Error("Expected Key to be populated")
		}

		// Error should be wrapped in dataerr.Store
		if !contains(ge.Error(), "Read") {
			t.Errorf("Error message should contain 'Read': %s", ge.Error())
		}
	})

	t.Run("read non-existent composite entity", func(t *testing.T) {
		entity := &ddbtest.CompositeKeyEntity{
			PartitionKey: "tenant1",
			SortKey:      "nonexistent",
		}
		ge := store.Read(ctx, entity)

		if ge == nil {
			t.Fatal("Expected PersistableNotFound error but got nil")
		}

		var pnfErr *dataerr.PersistableNotFoundError
		if !errors.As(ge, &pnfErr) {
			t.Fatalf("Expected PersistableNotFoundError in chain, got: %T", ge)
		}

		if pnfErr.TypeName != "CompositeKeyEntity" {
			t.Errorf("Expected TypeName 'CompositeKeyEntity', got %q", pnfErr.TypeName)
		}
	})

	t.Run("delete non-existent with strict mode", func(t *testing.T) {
		configStrict := &ddb.Configuration{
			MaxResultsDefault:           100,
			MaxResultsMax:               1000,
			ConsistencyDefault:          ddb.Preferred,
			FailDeleteIfNotPresent:      true,
			ValidateKeyFieldConsistency: false,
		}
		storeStrict, client := setupErrorStore(t, configStrict, &ddbtest.CompositeKeyEntity{})
		defer cleanupErrorTable(t, client)

		entity := &ddbtest.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "sk1"}
		ge := storeStrict.Delete(ctx, entity)

		if ge == nil {
			t.Fatal("Expected PersistableNotFound error but got nil")
		}

		var pnfErr *dataerr.PersistableNotFoundError
		if !errors.As(ge, &pnfErr) {
			t.Fatalf("Expected PersistableNotFoundError in chain, got: %T", ge)
		}
	})

	t.Run("delete non-existent with permissive mode", func(t *testing.T) {
		configPermissive := &ddb.Configuration{
			MaxResultsDefault:           100,
			MaxResultsMax:               1000,
			ConsistencyDefault:          ddb.Preferred,
			FailDeleteIfNotPresent:      false,
			ValidateKeyFieldConsistency: false,
		}
		storePermissive, client := setupErrorStore(t, configPermissive, &ddbtest.CompositeKeyEntity{})
		defer cleanupErrorTable(t, client)

		entity := &ddbtest.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "sk1"}
		ge := storePermissive.Delete(ctx, entity)

		// No error in permissive mode
		assert.Success(t, ge)
	})
}

// ============================================================================
// SECTION 4: ERROR MESSAGE QUALITY AND CONTEXT
// ============================================================================
// Tests to verify error messages are clear, actionable, and contain proper
// context.

// ----------------------------------------------------------------------------
// 4.1 Error Message Contains Context
// ----------------------------------------------------------------------------

func TestErrors_Quality_MessageContext(t *testing.T) {
	t.Skip("Tests not validated")

	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)
	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	t.Run("configuration error contains field name", func(t *testing.T) {
		_, ge := ddb.Store(errorsTestTableName, &ddb.Configuration{
			DynamoDb:                    client,
			MaxResultsDefault:           100,
			MaxResultsMax:               1000,
			ConsistencyDefault:          ddb.Preferred,
			FailDeleteIfNotPresent:      false,
			ValidateKeyFieldConsistency: false,
		}, &InvalidKeysEntity1{})

		if ge == nil {
			t.Fatal("Expected configuration error")
		}

		errMsg := ge.Error()
		if !contains(errMsg, "Id") {
			t.Errorf("Error should contain field name 'Id': %s", errMsg)
		}
	})

	t.Run("runtime error contains operation name", func(t *testing.T) {
		store, client := setupErrorStore(t, nil, &ddbtest.CompositeKeyEntity{})
		defer cleanupErrorTable(t, client)

		entity := &ddbtest.CompositeKeyEntity{PartitionKey: "", SortKey: "sk1"}
		ge := store.Create(context.Background(), entity)

		if ge == nil {
			t.Fatal("Expected runtime error")
		}

		var storeErr *dataerr.StoreError
		if !errors.As(ge, &storeErr) {
			t.Fatal("Expected StoreError wrapper")
		}

		if storeErr.Operation != "Create" {
			t.Errorf("Expected operation 'Create', got %q", storeErr.Operation)
		}
	})

	t.Run("not found error contains entity type", func(t *testing.T) {
		store, client := setupErrorStore(t, nil, &ddbtest.CompositeKeyEntity{})
		defer cleanupErrorTable(t, client)

		entity := &ddbtest.CompositeKeyEntity{PartitionKey: "nonexistent", SortKey: "sk1"}
		ge := store.Read(context.Background(), entity)

		if ge == nil {
			t.Fatal("Expected not found error")
		}

		errMsg := ge.Error()
		if !contains(errMsg, "CompositeKeyEntity") {
			t.Errorf("Error should contain entity type 'SimpleEntity': %s", errMsg)
		}
	})
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && hasSubstring(s, substr)))
}

func hasSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
