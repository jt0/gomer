package util

type Dirtyable interface {
	IsDirty() bool
	ClearDirty()
}
