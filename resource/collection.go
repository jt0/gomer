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
	NextToken  *string `out:"+"`
	MaxResults int

	// TODO: move to `data` package
	consistencyType dynamodb.ConsistencyType
}

func (c *Collection[I]) registeredType() *registeredType {
	return c.proto.registeredType()
}

func (c *Collection[I]) Subject() auth.Subject {
	return c.proto.Subject()
}

func (c *Collection[I]) initialize(rt *registeredType, sub auth.Subject) {
	c.proto.initialize(rt, sub)
}

func (c *Collection[I]) DoAction(ctx context.Context, action Action[*Collection[I]]) (*Collection[I], gomerr.Gomerr) {
	if ge := action.Pre(ctx, c); ge != nil {
		return nil, ge
	}

	ge := action.Do(ctx, c)
	for i := 0; i < 2 && ge != nil; i++ { // up to 2 retries
		if ge = action.Retry(ctx, c, ge); ge != nil {
			break
		}
		ge = action.Do(ctx, c)
	}
	if ge != nil {
		return nil, action.OnDoFailure(ctx, c, ge)
	}

	return action.OnDoSuccess(ctx, c)
}

func (c *Collection[I]) Query(ctx context.Context) gomerr.Gomerr {
	return c.proto.registeredType().store.Query(ctx, c)
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
		typedItem.initialize(c.proto.registeredType(), c.proto.Subject())
		c.Items[i] = typedItem
	}
}

func (c *Collection[I]) NextPageToken() *string {
	if c.NextToken == nil {
		if i, ok := any(c.proto).(interface{ NextPageToken() *string }); ok {
			return i.NextPageToken()
		}
	}
	return c.NextToken
}

func (c *Collection[I]) SetNextPageToken(token *string) {
	c.NextToken = token
}

func (c *Collection[I]) MaximumPageSize() int {
	mps := c.MaxResults
	if mps == 0 {
		if i, ok := any(c.proto).(interface{ MaximumPageSize() int }); ok {
			mps = i.MaximumPageSize()
		}
	}
	if mps > 0 && mps <= data.MaxResultsDefault { // TODO: MaxMaxResults
		return mps
	}
	return data.MaxResultsDefault
}

func (c *Collection[I]) SetMaximumPageSize(pageSize int) {
	c.MaxResults = pageSize
}

func (c *Collection[I]) ConsistencyType() dynamodb.ConsistencyType {
	return c.consistencyType
}

func (c *Collection[I]) SetConsistencyType(consistencyType dynamodb.ConsistencyType) {
	c.consistencyType = consistencyType
}

// List hooks - override these in concrete types as needed.

func (*Collection[I]) PreList(_ context.Context) gomerr.Gomerr {
	return nil
}

func (*Collection[I]) RetryList(_ context.Context, ge gomerr.Gomerr) gomerr.Gomerr {
	return ge
}

func (*Collection[I]) PostList(_ context.Context) gomerr.Gomerr {
	return nil
}
