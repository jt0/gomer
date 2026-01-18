package dynamodb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/jt0/gomer/_test/assert"
	"github.com/jt0/gomer/constraint"
	"github.com/jt0/gomer/data"
	ddb "github.com/jt0/gomer/data/dynamodb"
	ddbtest "github.com/jt0/gomer/data/dynamodb/_test"
)

const testTableName = "gomer_constraint_test"

// Test fixtures

type User struct {
	Id       string `db.keys:"pk"`
	Email    string `db.constraints:"unique"`
	TenantId string
	Name     string
}

func (u *User) TypeName() string             { return "User" }
func (u *User) NewQueryable() data.Queryable { return &UserQuery{} }

type UserQuery struct {
	data.BaseQueryable
	Id       string `db.keys:"pk"`
	Email    string
	TenantId string
}

func (q *UserQuery) TypeNames() []string         { return []string{"User"} }
func (q *UserQuery) TypeOf(_ interface{}) string { return "User" }

type Account struct {
	Id       string `db.keys:"pk"`
	Email    string `db.constraints:"unique(TenantId)"`
	TenantId string
	Plan     string
}

func (a *Account) TypeName() string             { return "Account" }
func (a *Account) NewQueryable() data.Queryable { return &AccountQuery{} }

type AccountQuery struct {
	data.BaseQueryable
	Id       string `db.keys:"pk"`
	Email    string
	TenantId string
}

func (q *AccountQuery) TypeNames() []string         { return []string{"Account"} }
func (q *AccountQuery) TypeOf(_ interface{}) string { return "Account" }

// Test setup

func setupStore(t *testing.T, persistables ...data.Persistable) (data.Store, *dynamodb.Client) {
	client, isLocal, err := ddbtest.NewClient()
	assert.Success(t, err)

	if !isLocal {
		t.Skip("Skipping test: DDB_LOCAL not set")
	}

	// Create table definition
	tableDef := &ddbtest.TableDefinition{}
	tableDef.WithTableName(testTableName).
		WithAttributeDefinition("PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("SK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("Email", types.ScalarAttributeTypeS).
		WithAttributeDefinition("TenantId", types.ScalarAttributeTypeS).
		WithKeySchema("PK", types.KeyTypeHash).
		WithKeySchema("SK", types.KeyTypeRange).
		WithGsi("gsi_1", []types.KeySchemaElement{
			{AttributeName: ddbtest.Ptr("Email"), KeyType: types.KeyTypeHash},
			{AttributeName: ddbtest.Ptr("TenantId"), KeyType: types.KeyTypeRange},
		}, types.Projection{ProjectionType: types.ProjectionTypeAll})

	tableDef.Create(client)

	store, ge := ddb.Store(testTableName, &ddb.Configuration{
		DynamoDb:           client,
		MaxResultsDefault:  100,
		MaxResultsMax:      1000,
		ConsistencyDefault: ddb.Preferred,
	}, persistables...)
	assert.Success(t, ge)

	return store, client
}

func cleanupTable(t *testing.T, client *dynamodb.Client) {
	err := ddbtest.DeleteAllTableData(client, testTableName)
	assert.Success(t, err)
}

// Tests

func TestConstraintTool_Create(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(store data.Store) // Optional setup before the test
		persistable data.Persistable
		expectError bool
	}{
		{
			name:        "unique email - first create succeeds",
			persistable: &User{Id: "user1", Email: "test@example.com", TenantId: "tenant1", Name: "Test User"},
			expectError: false,
		},
		{
			name: "unique email - duplicate fails",
			setup: func(store data.Store) {
				user1 := &User{Id: "user1", Email: "duplicate@example.com", TenantId: "tenant1", Name: "First User"}
				ge := store.Create(context.Background(), user1)
				assert.Success(t, ge)
			},
			persistable: &User{Id: "user2", Email: "duplicate@example.com", TenantId: "tenant2", Name: "Second User"},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupStore(t, &User{})
			defer cleanupTable(t, client)

			ctx := context.Background()

			if tt.setup != nil {
				tt.setup(store)
			}

			ge := store.Create(ctx, tt.persistable)

			if tt.expectError {
				assert.ErrorType(t, ge, constraint.NotSatisfied(nil))
			} else {
				assert.Success(t, ge)
			}
		})
	}
}

func TestConstraintTool_Update(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(store data.Store)
		update      func(store data.Store)
		expectError bool
	}{
		{
			name: "update to unique email succeeds",
			setup: func(store data.Store) {
				ctx := context.Background()
				user1 := &User{Id: "user1", Email: "user1@example.com", TenantId: "tenant1", Name: "User One"}
				assert.Success(t, store.Create(ctx, user1))
				user2 := &User{Id: "user2", Email: "user2@example.com", TenantId: "tenant1", Name: "User Two"}
				assert.Success(t, store.Create(ctx, user2))
			},
			update: func(store data.Store) {
				ctx := context.Background()
				user2 := &User{Id: "user2", Email: "newemail@example.com", TenantId: "tenant1", Name: "User Two"}
				assert.Success(t, store.Update(ctx, user2, nil))
			},
			expectError: false,
		},
		{
			name: "update to duplicate email fails",
			setup: func(store data.Store) {
				ctx := context.Background()
				user1 := &User{Id: "user1", Email: "user1@example.com", TenantId: "tenant1", Name: "User One"}
				assert.Success(t, store.Create(ctx, user1))
				user2 := &User{Id: "user2", Email: "user2@example.com", TenantId: "tenant1", Name: "User Two"}
				assert.Success(t, store.Create(ctx, user2))
			},
			update: func(store data.Store) {
				ctx := context.Background()
				user2 := &User{Id: "user2", Email: "user1@example.com", TenantId: "tenant1", Name: "User Two"}
				store.Update(ctx, user2, nil)
			},
			expectError: true,
		},
		{
			name: "update non-constrained field succeeds without validation",
			setup: func(store data.Store) {
				ctx := context.Background()
				user := &User{Id: "user1", Email: "user@example.com", TenantId: "tenant1", Name: "Original Name"}
				assert.Success(t, store.Create(ctx, user))
			},
			update: func(store data.Store) {
				ctx := context.Background()
				user := &User{Id: "user1", Email: "user@example.com", TenantId: "tenant1", Name: "Updated Name"}
				assert.Success(t, store.Update(ctx, user, &User{Name: "Updated Name"}))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupStore(t, &User{})
			defer cleanupTable(t, client)

			tt.setup(store)

			if tt.expectError {
				// Capture the error from update in a way we can test
				ctx := context.Background()
				user2 := &User{Id: "user2", Email: "user1@example.com", TenantId: "tenant1", Name: "User Two"}
				ge := store.Update(ctx, user2, nil)
				assert.ErrorType(t, ge, constraint.NotSatisfied(nil))
			} else {
				tt.update(store)
			}
		})
	}
}

func TestConstraintTool_MultiFieldUnique(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(store data.Store)
		operation   func(store data.Store) error
		expectError bool
	}{
		{
			name: "create with same email different tenant succeeds",
			setup: func(store data.Store) {
				ctx := context.Background()
				account1 := &Account{Id: "account1", Email: "user@example.com", TenantId: "tenant1", Plan: "premium"}
				assert.Success(t, store.Create(ctx, account1))
			},
			operation: func(store data.Store) error {
				ctx := context.Background()
				account2 := &Account{Id: "account2", Email: "user@example.com", TenantId: "tenant2", Plan: "basic"}
				return store.Create(ctx, account2)
			},
			expectError: false,
		},
		{
			name: "create with same email same tenant fails",
			setup: func(store data.Store) {
				ctx := context.Background()
				account1 := &Account{Id: "account1", Email: "user@example.com", TenantId: "tenant1", Plan: "premium"}
				assert.Success(t, store.Create(ctx, account1))
			},
			operation: func(store data.Store) error {
				ctx := context.Background()
				account2 := &Account{Id: "account2", Email: "user@example.com", TenantId: "tenant1", Plan: "basic"}
				return store.Create(ctx, account2)
			},
			expectError: true,
		},
		{
			name: "update to different email same tenant succeeds",
			setup: func(store data.Store) {
				ctx := context.Background()
				account1 := &Account{Id: "account1", Email: "user1@example.com", TenantId: "tenant1", Plan: "premium"}
				assert.Success(t, store.Create(ctx, account1))
				account2 := &Account{Id: "account2", Email: "user2@example.com", TenantId: "tenant1", Plan: "basic"}
				assert.Success(t, store.Create(ctx, account2))
			},
			operation: func(store data.Store) error {
				ctx := context.Background()
				account2 := &Account{Id: "account2", Email: "different@example.com", TenantId: "tenant1", Plan: "basic"}
				return store.Update(ctx, account2, nil)
			},
			expectError: false,
		},
		{
			name: "update to duplicate email same tenant fails",
			setup: func(store data.Store) {
				ctx := context.Background()
				account1 := &Account{Id: "account1", Email: "user1@example.com", TenantId: "tenant1", Plan: "premium"}
				assert.Success(t, store.Create(ctx, account1))
				account2 := &Account{Id: "account2", Email: "user2@example.com", TenantId: "tenant1", Plan: "basic"}
				assert.Success(t, store.Create(ctx, account2))
			},
			operation: func(store data.Store) error {
				ctx := context.Background()
				account2 := &Account{Id: "account2", Email: "user1@example.com", TenantId: "tenant1", Plan: "basic"}
				return store.Update(ctx, account2, nil)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupStore(t, &Account{})
			defer cleanupTable(t, client)

			if tt.setup != nil {
				tt.setup(store)
			}

			ge := tt.operation(store)

			if tt.expectError {
				assert.ErrorType(t, ge, constraint.NotSatisfied(nil))
			} else {
				assert.Success(t, ge)
			}
		})
	}
}
