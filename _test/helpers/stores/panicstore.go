package stores

import (
	"context"

	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

var PanicStore panicStore

type panicStore struct{}

func (panicStore) Create(context.Context, data.Persistable) gomerr.Gomerr {
	panic("As promised")
}

func (panicStore) Read(context.Context, data.Persistable) gomerr.Gomerr {
	panic("As promised")
}

func (panicStore) Update(context.Context, data.Persistable, data.Persistable) gomerr.Gomerr {
	panic("As promised")
}

func (panicStore) Delete(context.Context, data.Persistable) gomerr.Gomerr {
	panic("As promised")
}

func (panicStore) Query(context.Context, data.Queryable) gomerr.Gomerr {
	panic("As promised")
}
