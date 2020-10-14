package limit

import (
	"github.com/jt0/gomer/gomerr"
)

type Limited interface {
	Limiter() (Limiter, gomerr.Gomerr)
	DefaultLimit() Amount
	LimitAmount() Amount
}

type Limiter interface {
	Dirtyable
	Current(limited Limited) Amount
	SetCurrent(limited Limited, current Amount)
	Override(limited Limited) Amount
	SetOverride(limited Limited, override Amount)
	Maximum(limited Limited) Amount
}

type Dirtyable interface {
	IsDirty() bool
	ClearDirty()
}
