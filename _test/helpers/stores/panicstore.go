package stores

import (
	"github.com/jt0/gomer/data"
	"github.com/jt0/gomer/gomerr"
)

var PanicStore panicStore

type panicStore struct{}

func (panicStore) Create(data.Persistable) gomerr.Gomerr {
	panic("As promised")
}

func (panicStore) Read(data.Persistable) gomerr.Gomerr {
	panic("As promised")
}

func (panicStore) Update(data.Persistable, data.Persistable) gomerr.Gomerr {
	panic("As promised")
}

func (panicStore) Delete(data.Persistable) gomerr.Gomerr {
	panic("As promised")
}

func (panicStore) List(data.Listable) gomerr.Gomerr {
	panic("As promised")
}
