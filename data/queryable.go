package data

type Listable interface {
	TypeNames() []string
	TypeOf(interface{}) string
	Items() []interface{}
	SetItems([]interface{})
	NextPageToken() *string
	SetNextPageToken(*string)
	MaximumPageSize() int
	Ascending() *bool
}

type QueryTyper interface {
	QueryType() QueryType
}

type QueryType uint16

const (
	EQ QueryType = iota + 1
	//NEQ
	GTE
	GT
	LTE
	LT
	//BETWEEN
	//CONTAINS
)

var MaxResultsDefault = 100

type BaseQueryable struct {
	items      []interface{}
	nextToken  *string
	maxResults *int
}

func (b *BaseQueryable) Items() []interface{} {
	return b.items
}

func (b *BaseQueryable) SetItems(items []interface{}) {
	b.items = items
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

func (b *BaseQueryable) Ascending() *bool {
	return nil
}
