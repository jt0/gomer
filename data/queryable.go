package data

type Queryable interface {
	TypeName() string
	ItemTemplate() any
	Results() []any
	SetResults([]any)
	NextPageToken() *string
	SetNextPageToken(*string)
	MaximumPageSize() int
}

type QueryTypes uint16

const (
	EQ QueryTypes = iota + 1
	// NEQ
	// GTE
	// GT
	// LTE
	// LT
	// BETWEEN
	// CONTAINS
)

var MaxResultsDefault = 100

type BaseQueryable struct {
	results    []any
	nextToken  *string
	maxResults *int
}

func (b *BaseQueryable) Results() []any {
	return b.results
}

func (b *BaseQueryable) SetResults(items []any) {
	b.results = items
}

func (b *BaseQueryable) NextPageToken() *string {
	return b.nextToken
}

func (b *BaseQueryable) SetNextPageToken(nextToken *string) {
	b.nextToken = nextToken
}

func (b *BaseQueryable) MaximumPageSize() int {
	if b.maxResults == nil {
		return MaxResultsDefault
	}
	return *b.maxResults
}

func (b *BaseQueryable) SetMaximumPageSize(size int) {
	b.maxResults = &size
}
