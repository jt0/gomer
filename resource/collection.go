package resource

import (
	"context"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/data/dynamodb"
	"github.com/jt0/gomer/gomerr"
)

func NewCollection[I Instance[I]](proto I) *Collection[I] {
	return &Collection[I]{proto: proto}
}

// Collection holds a set of instances resulting from a query.
// Unlike Instance, Collection is parameterized only by its item type,
// delegating resource identity to the instance type.
type Collection[I Instance[I]] struct {
	Resource[*Collection[I]]
	proto      I       // Instance used as proto for query
	Items      []I     `out:"+,includeempty"`
	NextToken  *string `in:"query.next_token" out:"+"`
	MaxResults int     `in:"query.max_results" validate:"intbetween(1,100)"` // TODO:  "intbetween(1,$.MaximumPageSize())

	// TODO: move to `data` package
	consistencyType dynamodb.ConsistencyType
}

func (c *Collection[I]) Metadata() *Metadata {
	return c.proto.Metadata()
}

func (c *Collection[I]) Subject() auth.Subject {
	return c.proto.Subject()
}

func (c *Collection[I]) initialize(md *Metadata, sub auth.Subject) {
	// TODO:?
	c.proto.initialize(md, sub)
}

func (c *Collection[I]) DoAction(ctx context.Context, action Action[*Collection[I]]) (*Collection[I], gomerr.Gomerr) {
	if ge := action.Pre(ctx, c); ge != nil {
		return nil, ge
	}

	if ge := action.Do(ctx, c); ge != nil {
		return nil, action.OnDoFailure(ctx, c, ge)
	}

	return action.OnDoSuccess(ctx, c)
}

func (c *Collection[I]) Query(ctx context.Context) gomerr.Gomerr {
	return c.proto.Metadata().dataStore.Query(ctx, c)
}

func (c *Collection[I]) TypeName() string {
	return c.proto.TypeName()
}

func (c *Collection[I]) ItemTemplate() any {
	return c.proto
}

// Results implements data.Queryable with []any.
func (c *Collection[I]) Results() []any {
	result := make([]any, len(c.Items))
	for i, item := range c.Items {
		result[i] = item
	}
	return result
}

// SetResults implements data.Queryable with []any.
func (c *Collection[I]) SetResults(items []any) {
	c.Items = make([]I, len(items))
	for i, item := range items {
		typedItem := item.(I)
		typedItem.initialize(c.proto.Metadata(), c.proto.Subject())
		c.Items[i] = typedItem
	}
}

func (c *Collection[I]) NextPageToken() *string {
	return c.NextToken
}

func (c *Collection[I]) SetNextPageToken(token *string) {
	c.NextToken = token
}

func (c *Collection[I]) MaximumPageSize() int {
	//	if b.MaxResults == nil || *b.MaxResults > DefaultMaxMaxResults {
	//		return DefaultMaxResults
	//	}
	if c.MaxResults == 0 {
		return data.MaxResultsDefault
	}
	return c.MaxResults
}

func (c *Collection[I]) ConsistencyType() dynamodb.ConsistencyType {
	return c.consistencyType
}

func (c *Collection[I]) SetConsistencyType(consistencyType dynamodb.ConsistencyType) {
	c.consistencyType = consistencyType
}

// List lifecycle hooks - override these in concrete types as needed.

func (*Collection[I]) PreList(_ context.Context) gomerr.Gomerr {
	return nil
}

func (*Collection[I]) PostList(_ context.Context) gomerr.Gomerr {
	return nil
}
