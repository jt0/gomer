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
	TenantId string `db.keys:"pk"`
	Id       string `db.keys:"sk"`
	Email    string `db.keys:"gsi_1:pk" db.constraints:"unique"` // atypical, but set this way for testing
	Name     string
}

func (u *User) TypeName() string             { return "User" }
func (u *User) NewQueryable() data.Queryable { return &Users{} }

type Users struct {
	data.BaseQueryable
	TenantId string
	Email    string
}

func (q *Users) TypeName() string  { return "User" }
func (q *Users) ItemTemplate() any { return q }

type Account struct {
	TenantId string `db.keys:"pk"`
	Id       string `db.keys:"sk"`
	Email    string `db.keys:"lsi_1:sk" db.constraints:"unique(TenantId)"` // more typical
	Plan     string
}

func (a *Account) TypeName() string             { return "Account" }
func (a *Account) NewQueryable() data.Queryable { return &Accounts{} }

type Accounts struct {
	data.BaseQueryable
	TenantId string
	Email    string
}

func (q *Accounts) TypeName() string  { return "Account" }
func (q *Accounts) ItemTemplate() any { return q }

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
		WithAttributeDefinition("GSI1PK", types.ScalarAttributeTypeS).
		WithAttributeDefinition("LSI1SK", types.ScalarAttributeTypeS).
		WithKeySchema("PK", types.KeyTypeHash).
		WithKeySchema("SK", types.KeyTypeRange).
		WithLsi("lsi_1", []types.KeySchemaElement{
			{AttributeName: ddbtest.Ptr("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: ddbtest.Ptr("LSI1SK"), KeyType: types.KeyTypeRange},
		}, types.Projection{ProjectionType: types.ProjectionTypeAll}).
		WithGsi("gsi_1", []types.KeySchemaElement{
			{AttributeName: ddbtest.Ptr("GSI1PK"), KeyType: types.KeyTypeHash},
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
				user1 := &User{Id: "user2", Email: "duplicate@example.com", TenantId: "tenant1", Name: "Second User"}
				ge := store.Create(context.Background(), user1)
				assert.Success(t, ge)
			},
			persistable: &User{Id: "user3", Email: "duplicate@example.com", TenantId: "tenant1", Name: "Third User"},
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
		toUpdate    *User
		update      *User
		expectError bool
	}{
		{
			name: "update to unique email succeeds",
			setup: func(store data.Store) {
				user1 := &User{Id: "user1", Email: "user1@example.com", TenantId: "tenant1", Name: "User One"}
				assert.Success(t, store.Create(context.Background(), user1))
			},
			toUpdate:    &User{Id: "user2", Email: "user2@example.com", TenantId: "tenant1", Name: "User Two"},
			update:      &User{Email: "newemail@example.com"},
			expectError: false,
		},
		{
			name: "update to duplicate email fails",
			setup: func(store data.Store) {
				user1 := &User{Id: "user1", Email: "user1@example.com", TenantId: "tenant1", Name: "User One"}
				assert.Success(t, store.Create(context.Background(), user1))
			},
			toUpdate:    &User{Id: "user2", Email: "user2@example.com", TenantId: "tenant1", Name: "User Two"},
			update:      &User{Email: "user1@example.com"},
			expectError: true,
		},
		{
			name:        "update non-constrained field succeeds without validation",
			setup:       func(store data.Store) {},
			toUpdate:    &User{Id: "user1", Email: "user@example.com", TenantId: "tenant1", Name: "Original Name"},
			update:      &User{Name: "Updated Name"},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupStore(t, &User{})
			defer cleanupTable(t, client)
			tt.setup(store)

			assert.Success(t, store.Create(context.Background(), tt.toUpdate))
			ge := store.Update(context.Background(), tt.toUpdate, tt.update)

			if tt.expectError {
				assert.ErrorType(t, ge, constraint.NotSatisfied(nil))
			} else {
				assert.Success(t, ge)
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
				return store.Update(ctx, account2, &User{Email: "different@example.com"})
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
				account2 := &Account{Id: "account2", Email: "user2@example.com", TenantId: "tenant1", Plan: "basic"}
				return store.Update(ctx, account2, &User{Email: "user1@example.com"})
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, client := setupStore(t, &Account{})
			defer cleanupTable(t, client)
			tt.setup(store)

			ge := tt.operation(store)

			if tt.expectError {
				assert.ErrorType(t, ge, constraint.NotSatisfied(nil))
			} else {
				assert.Success(t, ge)
			}
		})
	}
}
