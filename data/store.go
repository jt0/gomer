package data

import (
	"context"

	"github.com/jt0/gomer/gomerr"
)

type Store interface {
	AddPersistables(...Persistable) gomerr.Gomerr
	Create(ctx context.Context, p Persistable) gomerr.Gomerr
	Read(ctx context.Context, p Persistable) gomerr.Gomerr
	Update(ctx context.Context, p Persistable, update Persistable) gomerr.Gomerr
	Delete(ctx context.Context, p Persistable) gomerr.Gomerr
	Query(ctx context.Context, q Queryable) gomerr.Gomerr
}

type Persistable interface {
	TypeName() string
	NewQueryable() Queryable
}

// ConsistencyType values declare how a data store's underlying data should be read when
// read-after-write . While some data stores may provide this option, others may always
// read consistently. all data stores will necessarily implement this, but

// ConsistencyType says whether a read must reflect every write that completed before it.
// A store that can serve a read from sources with different read-after-write guarantees,
// such as those with asynchronously-populated indexes, uses this value to select how
// best to retrieve the data.
type ConsistencyType int

const (
	// ConsistencyIndifferent allows reads to return data that does not yet include every
	// written value. For data stores that support it, these often cost less than their
	// consistent counterparts in either/both latency or monetary units.
	ConsistencyIndifferent ConsistencyType = iota

	// ConsistencyRequired limits reads to only those options that can fulfill the read-
	// after-write guarantee.
	ConsistencyRequired

	// ConsistencyPreferred reads consistently when a request has both consistent and
	// inconsistent options available.
	ConsistencyPreferred
)
